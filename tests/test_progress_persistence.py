"""Tests for progress persistence (save/load job progress)."""

import json
from pathlib import Path

import pytest

from jsonl_resumable.models import JobProgress
from jsonl_resumable.progress import (
    FORMAT_VERSION,
    delete_job_progress,
    load_progress,
    save_progress,
    update_job_progress,
)


@pytest.fixture
def sample_job() -> JobProgress:
    """Create sample JobProgress for testing."""
    return JobProgress(
        job_id="test_job",
        position=500,
        file_size=10000,
        file_mtime=1234567890.123,
        status="in_progress",
        created_at="2024-01-01T00:00:00+00:00",
        last_checkpoint_at="2024-01-01T01:00:00+00:00",
    )


@pytest.fixture
def sample_jobs(sample_job: JobProgress) -> dict[str, JobProgress]:
    """Create multiple sample jobs for testing."""
    return {
        "test_job": sample_job,
        "another_job": JobProgress(
            job_id="another_job",
            position=100,
            file_size=5000,
            file_mtime=1234567890.456,
            status="completed",
            created_at="2024-01-01T00:00:00+00:00",
            last_checkpoint_at="2024-01-01T02:00:00+00:00",
            completed_at="2024-01-01T02:00:00+00:00",
        ),
    }


class TestSaveProgress:
    """Tests for save_progress function."""

    def test_saves_valid_json(self, tmp_path: Path, sample_jobs: dict[str, JobProgress]):
        """Saved progress is valid JSON."""
        progress_path = tmp_path / "test.progress"
        save_progress(progress_path, sample_jobs)

        with open(progress_path) as f:
            data = json.load(f)

        assert "format_version" in data
        assert "jobs" in data

    def test_includes_format_version(
        self, tmp_path: Path, sample_jobs: dict[str, JobProgress]
    ):
        """Saved progress includes format version."""
        progress_path = tmp_path / "test.progress"
        save_progress(progress_path, sample_jobs)

        with open(progress_path) as f:
            data = json.load(f)

        assert data["format_version"] == FORMAT_VERSION

    def test_stores_job_fields(self, tmp_path: Path, sample_job: JobProgress):
        """All job fields are stored."""
        progress_path = tmp_path / "test.progress"
        save_progress(progress_path, {"test_job": sample_job})

        with open(progress_path) as f:
            data = json.load(f)

        job = data["jobs"]["test_job"]
        assert job["position"] == sample_job.position
        assert job["file_size"] == sample_job.file_size
        assert job["file_mtime"] == sample_job.file_mtime
        assert job["status"] == sample_job.status
        assert job["created_at"] == sample_job.created_at
        assert job["last_checkpoint_at"] == sample_job.last_checkpoint_at
        assert job["completed_at"] is None

    def test_stores_completed_at(self, tmp_path: Path):
        """completed_at is stored when present."""
        job = JobProgress(
            job_id="done_job",
            position=100,
            file_size=1000,
            file_mtime=123.0,
            status="completed",
            created_at="2024-01-01T00:00:00Z",
            last_checkpoint_at="2024-01-01T01:00:00Z",
            completed_at="2024-01-01T01:00:00Z",
        )
        progress_path = tmp_path / "test.progress"
        save_progress(progress_path, {"done_job": job})

        with open(progress_path) as f:
            data = json.load(f)

        assert data["jobs"]["done_job"]["completed_at"] == "2024-01-01T01:00:00Z"

    def test_stores_multiple_jobs(
        self, tmp_path: Path, sample_jobs: dict[str, JobProgress]
    ):
        """Multiple jobs are stored."""
        progress_path = tmp_path / "test.progress"
        save_progress(progress_path, sample_jobs)

        with open(progress_path) as f:
            data = json.load(f)

        assert len(data["jobs"]) == 2
        assert "test_job" in data["jobs"]
        assert "another_job" in data["jobs"]

    def test_uses_compact_json(
        self, tmp_path: Path, sample_jobs: dict[str, JobProgress]
    ):
        """JSON is written without extra whitespace."""
        progress_path = tmp_path / "test.progress"
        save_progress(progress_path, sample_jobs)

        content = progress_path.read_text()

        # No pretty-printing
        assert ": " not in content
        assert ", " not in content

    def test_overwrites_existing(self, tmp_path: Path, sample_job: JobProgress):
        """Can overwrite existing progress file."""
        progress_path = tmp_path / "test.progress"
        progress_path.write_text("old content")

        save_progress(progress_path, {"test_job": sample_job})

        with open(progress_path) as f:
            data = json.load(f)

        assert data["format_version"] == FORMAT_VERSION

    def test_empty_jobs(self, tmp_path: Path):
        """Handles empty jobs dict."""
        progress_path = tmp_path / "test.progress"
        save_progress(progress_path, {})

        with open(progress_path) as f:
            data = json.load(f)

        assert data["jobs"] == {}


class TestLoadProgress:
    """Tests for load_progress function."""

    def test_loads_saved_progress(
        self, tmp_path: Path, sample_jobs: dict[str, JobProgress]
    ):
        """Can load saved progress."""
        progress_path = tmp_path / "test.progress"
        save_progress(progress_path, sample_jobs)

        result = load_progress(progress_path)

        assert result is not None
        assert len(result) == 2
        assert "test_job" in result
        assert "another_job" in result

    def test_restores_job_fields(self, tmp_path: Path, sample_job: JobProgress):
        """Job fields are fully restored."""
        progress_path = tmp_path / "test.progress"
        save_progress(progress_path, {"test_job": sample_job})

        result = load_progress(progress_path)
        assert result is not None
        loaded = result["test_job"]

        assert loaded.job_id == sample_job.job_id
        assert loaded.position == sample_job.position
        assert loaded.file_size == sample_job.file_size
        assert loaded.file_mtime == sample_job.file_mtime
        assert loaded.status == sample_job.status
        assert loaded.created_at == sample_job.created_at
        assert loaded.last_checkpoint_at == sample_job.last_checkpoint_at
        assert loaded.completed_at == sample_job.completed_at

    def test_missing_file_returns_none(self, tmp_path: Path):
        """Returns None for missing file."""
        result = load_progress(tmp_path / "missing.progress")
        assert result is None

    def test_invalid_json_returns_none(self, tmp_path: Path):
        """Returns None for invalid JSON."""
        progress_path = tmp_path / "invalid.progress"
        progress_path.write_text("not valid json {{{")

        result = load_progress(progress_path)
        assert result is None

    def test_wrong_version_returns_none(self, tmp_path: Path):
        """Returns None for wrong format version."""
        progress_path = tmp_path / "old.progress"
        data = {"format_version": "0.1", "jobs": {}}
        progress_path.write_text(json.dumps(data))

        result = load_progress(progress_path)
        assert result is None

    def test_missing_version_returns_none(self, tmp_path: Path):
        """Returns None when format_version is missing."""
        progress_path = tmp_path / "no_version.progress"
        data = {"jobs": {}}
        progress_path.write_text(json.dumps(data))

        result = load_progress(progress_path)
        assert result is None

    def test_missing_jobs_returns_empty(self, tmp_path: Path):
        """Returns empty dict when jobs section is missing."""
        progress_path = tmp_path / "no_jobs.progress"
        data = {"format_version": FORMAT_VERSION}
        progress_path.write_text(json.dumps(data))

        result = load_progress(progress_path)
        assert result is not None
        assert result == {}

    def test_malformed_job_returns_none(self, tmp_path: Path):
        """Returns None when job has missing required fields."""
        progress_path = tmp_path / "bad_job.progress"
        data = {
            "format_version": FORMAT_VERSION,
            "jobs": {"bad_job": {"position": 0}},  # Missing other fields
        }
        progress_path.write_text(json.dumps(data))

        result = load_progress(progress_path)
        assert result is None

    def test_empty_file_returns_none(self, tmp_path: Path):
        """Returns None for empty file."""
        progress_path = tmp_path / "empty.progress"
        progress_path.write_text("")

        result = load_progress(progress_path)
        assert result is None


class TestUpdateJobProgress:
    """Tests for update_job_progress function."""

    def test_creates_new_file(self, tmp_path: Path, sample_job: JobProgress):
        """Creates progress file if it doesn't exist."""
        progress_path = tmp_path / "new.progress"

        update_job_progress(progress_path, sample_job)

        assert progress_path.exists()
        result = load_progress(progress_path)
        assert result is not None
        assert "test_job" in result

    def test_adds_new_job(self, tmp_path: Path, sample_job: JobProgress):
        """Adds new job to existing file."""
        progress_path = tmp_path / "test.progress"
        save_progress(progress_path, {"existing_job": sample_job})

        new_job = JobProgress(
            job_id="new_job",
            position=0,
            file_size=1000,
            file_mtime=123.0,
            status="in_progress",
            created_at="2024-01-02T00:00:00Z",
            last_checkpoint_at="2024-01-02T00:00:00Z",
        )
        update_job_progress(progress_path, new_job)

        result = load_progress(progress_path)
        assert result is not None
        assert len(result) == 2
        assert "existing_job" in result
        assert "new_job" in result

    def test_updates_existing_job(self, tmp_path: Path, sample_job: JobProgress):
        """Updates existing job in file."""
        progress_path = tmp_path / "test.progress"
        save_progress(progress_path, {"test_job": sample_job})

        # Update the job
        sample_job.position = 1000
        sample_job.last_checkpoint_at = "2024-01-01T02:00:00Z"
        update_job_progress(progress_path, sample_job)

        result = load_progress(progress_path)
        assert result is not None
        assert result["test_job"].position == 1000
        assert result["test_job"].last_checkpoint_at == "2024-01-01T02:00:00Z"

    def test_preserves_other_jobs(
        self, tmp_path: Path, sample_jobs: dict[str, JobProgress]
    ):
        """Updating one job doesn't affect others."""
        progress_path = tmp_path / "test.progress"
        save_progress(progress_path, sample_jobs)

        # Update just one job
        job = sample_jobs["test_job"]
        job.position = 999
        update_job_progress(progress_path, job)

        result = load_progress(progress_path)
        assert result is not None

        # Other job unchanged
        assert result["another_job"].position == 100


class TestDeleteJobProgress:
    """Tests for delete_job_progress function."""

    def test_deletes_existing_job(
        self, tmp_path: Path, sample_jobs: dict[str, JobProgress]
    ):
        """Deletes existing job from file."""
        progress_path = tmp_path / "test.progress"
        save_progress(progress_path, sample_jobs)

        result = delete_job_progress(progress_path, "test_job")

        assert result is True
        loaded = load_progress(progress_path)
        assert loaded is not None
        assert "test_job" not in loaded
        assert "another_job" in loaded

    def test_returns_false_for_missing_job(
        self, tmp_path: Path, sample_job: JobProgress
    ):
        """Returns False when job doesn't exist."""
        progress_path = tmp_path / "test.progress"
        save_progress(progress_path, {"test_job": sample_job})

        result = delete_job_progress(progress_path, "nonexistent")

        assert result is False

    def test_returns_false_for_missing_file(self, tmp_path: Path):
        """Returns False when file doesn't exist."""
        result = delete_job_progress(tmp_path / "missing.progress", "test_job")
        assert result is False

    def test_preserves_other_jobs(
        self, tmp_path: Path, sample_jobs: dict[str, JobProgress]
    ):
        """Deleting one job doesn't affect others."""
        progress_path = tmp_path / "test.progress"
        save_progress(progress_path, sample_jobs)

        delete_job_progress(progress_path, "test_job")

        loaded = load_progress(progress_path)
        assert loaded is not None
        assert "another_job" in loaded
        assert loaded["another_job"].position == 100


class TestRoundTrip:
    """Tests for save/load round-trip consistency."""

    def test_full_roundtrip(
        self, tmp_path: Path, sample_jobs: dict[str, JobProgress]
    ):
        """Data survives complete save/load cycle."""
        progress_path = tmp_path / "test.progress"

        save_progress(progress_path, sample_jobs)
        result = load_progress(progress_path)

        assert result is not None
        for job_id, original in sample_jobs.items():
            loaded = result[job_id]
            assert loaded.job_id == original.job_id
            assert loaded.position == original.position
            assert loaded.file_size == original.file_size
            assert loaded.file_mtime == original.file_mtime
            assert loaded.status == original.status
            assert loaded.created_at == original.created_at
            assert loaded.last_checkpoint_at == original.last_checkpoint_at
            assert loaded.completed_at == original.completed_at

    def test_roundtrip_preserves_float_precision(self, tmp_path: Path):
        """mtime float precision is preserved."""
        precise_mtime = 1234567890.123456
        job = JobProgress(
            job_id="precise",
            position=0,
            file_size=1000,
            file_mtime=precise_mtime,
            status="in_progress",
            created_at="2024-01-01T00:00:00Z",
            last_checkpoint_at="2024-01-01T00:00:00Z",
        )

        progress_path = tmp_path / "test.progress"
        save_progress(progress_path, {"precise": job})
        result = load_progress(progress_path)

        assert result is not None
        assert result["precise"].file_mtime == precise_mtime

"""Tests for BatchProcessor."""

import json
import time
from pathlib import Path

import pytest

from jsonl_resumable import (
    InvalidCheckpointError,
    JsonlIndex,
    StaleCheckpointError,
)
from jsonl_resumable.progress import load_progress


@pytest.fixture
def sample_jsonl(tmp_path: Path) -> Path:
    """Create a sample JSONL file with 100 lines."""
    file_path = tmp_path / "sample.jsonl"
    with open(file_path, "w") as f:
        for i in range(100):
            f.write(json.dumps({"line": i, "data": f"content_{i}"}) + "\n")
    return file_path


@pytest.fixture
def small_jsonl(tmp_path: Path) -> Path:
    """Create a small JSONL file with 10 lines."""
    file_path = tmp_path / "small.jsonl"
    with open(file_path, "w") as f:
        for i in range(10):
            f.write(json.dumps({"n": i}) + "\n")
    return file_path


class TestBasicFunctionality:
    """Tests for basic batch processor functionality."""

    def test_creates_new_job(self, sample_jsonl: Path):
        """New job starts at position 0."""
        index = JsonlIndex(sample_jsonl)

        with index.batch_processor("test_job") as batch:
            assert batch.position == 0
            assert batch.job_id == "test_job"

    def test_resumes_from_checkpoint(self, small_jsonl: Path):
        """Resumes from saved position after checkpoint."""
        index = JsonlIndex(small_jsonl)

        # Process some items and checkpoint
        with index.batch_processor("resume_test") as batch:
            for i, record in batch:
                if i == 5:
                    batch.checkpoint()
                    break

        # Resume should start at position 6 (after the checkpointed item)
        with index.batch_processor("resume_test") as batch:
            assert batch.position == 6

    def test_checkpoint_persists(self, small_jsonl: Path):
        """checkpoint() saves to disk."""
        index = JsonlIndex(small_jsonl)
        progress_path = small_jsonl.with_suffix(".progress")

        with index.batch_processor("persist_test") as batch:
            for i, _ in batch:
                if i == 3:
                    batch.checkpoint()
                    break

        # Check the file directly
        jobs = load_progress(progress_path)
        assert jobs is not None
        assert "persist_test" in jobs
        assert jobs["persist_test"].position == 4  # After processing item 3

    def test_yields_line_number_and_content(self, sample_jsonl: Path):
        """Iteration yields (line_number, content) tuples."""
        index = JsonlIndex(sample_jsonl)

        with index.batch_processor("format_test") as batch:
            items = []
            for line_num, record in batch:
                items.append((line_num, record))
                if line_num >= 2:
                    break

        assert len(items) == 3
        assert items[0][0] == 0
        assert items[0][1]["line"] == 0
        assert items[1][0] == 1
        assert items[1][1]["line"] == 1
        assert items[2][0] == 2
        assert items[2][1]["line"] == 2

    def test_as_json_true(self, sample_jsonl: Path):
        """as_json=True parses lines as JSON."""
        index = JsonlIndex(sample_jsonl)

        with index.batch_processor("json_test", as_json=True) as batch:
            for _, record in batch:
                assert isinstance(record, dict)
                assert "line" in record
                break

    def test_as_json_false(self, sample_jsonl: Path):
        """as_json=False returns raw strings."""
        index = JsonlIndex(sample_jsonl)

        with index.batch_processor("raw_test", as_json=False) as batch:
            for _, record in batch:
                assert isinstance(record, str)
                assert '"line"' in record
                break

    def test_progress_property(self, small_jsonl: Path):
        """Progress percentage is calculated correctly."""
        index = JsonlIndex(small_jsonl)

        with index.batch_processor("progress_test") as batch:
            assert batch.progress == 0.0
            assert batch.total_lines == 10

            for i, _ in batch:
                if i == 4:
                    # Processed 5 items (0-4), position is now 5
                    assert batch.position == 5
                    assert batch.progress == 50.0
                    break


class TestCompletionSemantics:
    """Tests for job completion behavior."""

    def test_marks_complete_on_exhaustion(self, small_jsonl: Path):
        """Status becomes 'completed' when all items processed."""
        index = JsonlIndex(small_jsonl)

        with index.batch_processor("exhaust_test") as batch:
            for _ in batch:
                pass  # Process all items

        # Check completion status
        jobs = load_progress(small_jsonl.with_suffix(".progress"))
        assert jobs is not None
        assert jobs["exhaust_test"].status == "completed"
        assert jobs["exhaust_test"].completed_at is not None

    def test_preserves_checkpoint_on_exception(self, small_jsonl: Path):
        """Exception doesn't corrupt checkpoint."""
        index = JsonlIndex(small_jsonl)

        # Process and checkpoint, then raise exception
        with pytest.raises(ValueError):
            with index.batch_processor("exception_test") as batch:
                for i, _ in batch:
                    if i == 5:
                        batch.checkpoint()
                    if i == 7:
                        raise ValueError("Intentional error")

        # Checkpoint should be preserved at position 6
        jobs = load_progress(small_jsonl.with_suffix(".progress"))
        assert jobs is not None
        assert jobs["exception_test"].position == 6
        assert jobs["exception_test"].status == "in_progress"

    def test_no_checkpoint_stays_at_zero(self, small_jsonl: Path):
        """Without checkpoint, resume starts at 0."""
        index = JsonlIndex(small_jsonl)

        # Process some items but don't checkpoint
        with index.batch_processor("no_checkpoint_test") as batch:
            for i, _ in batch:
                if i == 5:
                    break  # Exit without checkpoint

        # Should resume from 0 since initial job was created but not checkpointed
        # The position should still be 0 from initial creation
        with index.batch_processor("no_checkpoint_test") as batch:
            assert batch.position == 0

    def test_early_exit_without_completion(self, small_jsonl: Path):
        """Breaking out of loop doesn't mark as complete."""
        index = JsonlIndex(small_jsonl)

        with index.batch_processor("early_exit_test") as batch:
            for i, _ in batch:
                if i == 5:
                    batch.checkpoint()
                    break

        # Should not be marked complete
        jobs = load_progress(small_jsonl.with_suffix(".progress"))
        assert jobs is not None
        assert jobs["early_exit_test"].status == "in_progress"


class TestEdgeCases:
    """Tests for edge cases."""

    def test_stale_checkpoint_raises(self, small_jsonl: Path):
        """File modified since checkpoint raises StaleCheckpointError."""
        index = JsonlIndex(small_jsonl)

        # Create a checkpoint
        with index.batch_processor("stale_test") as batch:
            for i, _ in batch:
                if i == 5:
                    batch.checkpoint()
                    break

        # Modify the file
        time.sleep(0.01)  # Ensure mtime changes
        with open(small_jsonl, "a") as f:
            f.write(json.dumps({"n": 999}) + "\n")

        # Create new index (picks up new file state)
        index2 = JsonlIndex(small_jsonl)

        # Should raise StaleCheckpointError
        with pytest.raises(StaleCheckpointError):
            with index2.batch_processor("stale_test") as batch:
                pass

    def test_invalid_checkpoint_raises(self, tmp_path: Path):
        """Position > total_lines raises InvalidCheckpointError."""
        # Create a file with 10 lines
        file_path = tmp_path / "test.jsonl"
        with open(file_path, "w") as f:
            for i in range(10):
                f.write(json.dumps({"n": i}) + "\n")

        index = JsonlIndex(file_path)

        # Create checkpoint at position 8
        with index.batch_processor("invalid_test") as batch:
            for i, _ in batch:
                if i == 7:
                    batch.checkpoint()
                    break

        # Truncate file to 5 lines (but keep same mtime to avoid StaleCheckpointError)
        with open(file_path, "w") as f:
            for i in range(5):
                f.write(json.dumps({"n": i}) + "\n")

        # Manually restore mtime/size to match checkpoint (hack for test)
        # This simulates a corrupted checkpoint scenario
        from jsonl_resumable.progress import load_progress, save_progress

        jobs = load_progress(file_path.with_suffix(".progress"))
        jobs["invalid_test"].file_size = file_path.stat().st_size
        jobs["invalid_test"].file_mtime = file_path.stat().st_mtime
        save_progress(file_path.with_suffix(".progress"), jobs)

        index2 = JsonlIndex(file_path)

        with pytest.raises(InvalidCheckpointError):
            with index2.batch_processor("invalid_test") as batch:
                pass

    def test_empty_file(self, tmp_path: Path):
        """Empty file yields nothing."""
        file_path = tmp_path / "empty.jsonl"
        file_path.touch()

        index = JsonlIndex(file_path)

        with index.batch_processor("empty_test") as batch:
            items = list(batch)

        assert items == []
        assert batch.progress == 100.0

    def test_already_completed_job(self, small_jsonl: Path):
        """Already completed job yields nothing."""
        index = JsonlIndex(small_jsonl)

        # Complete the job
        with index.batch_processor("completed_test") as batch:
            for _ in batch:
                pass

        # Running again yields nothing
        with index.batch_processor("completed_test") as batch:
            items = list(batch)

        assert items == []

    def test_must_use_context_manager(self, sample_jsonl: Path):
        """Iterating without context manager raises RuntimeError."""
        index = JsonlIndex(sample_jsonl)
        batch = index.batch_processor("no_context_test")

        with pytest.raises(RuntimeError, match="context manager"):
            for _ in batch:
                pass

    def test_checkpoint_outside_context_raises(self, sample_jsonl: Path):
        """checkpoint() outside context manager raises RuntimeError."""
        index = JsonlIndex(sample_jsonl)
        batch = index.batch_processor("outside_test")

        with pytest.raises(RuntimeError):
            batch.checkpoint()


class TestJobManagement:
    """Tests for job management methods."""

    def test_list_jobs(self, sample_jsonl: Path):
        """list_jobs() returns all jobs with correct info."""
        index = JsonlIndex(sample_jsonl)

        # Create two jobs
        with index.batch_processor("job1") as batch:
            for i, _ in batch:
                if i == 10:
                    batch.checkpoint()
                    break

        with index.batch_processor("job2") as batch:
            for _ in batch:
                pass  # Complete this job

        jobs = index.list_jobs()
        assert len(jobs) == 2

        job1 = next(j for j in jobs if j.job_id == "job1")
        job2 = next(j for j in jobs if j.job_id == "job2")

        assert job1.status == "in_progress"
        assert job1.position == 11
        assert job2.status == "completed"

    def test_get_job(self, sample_jsonl: Path):
        """get_job() returns correct JobInfo."""
        index = JsonlIndex(sample_jsonl)

        with index.batch_processor("get_test") as batch:
            for i, _ in batch:
                if i == 50:
                    batch.checkpoint()
                    break

        job = index.get_job("get_test")
        assert job is not None
        assert job.job_id == "get_test"
        assert job.position == 51
        assert job.total_lines == 100
        assert job.progress_pct == 51.0
        assert job.status == "in_progress"
        assert job.is_stale is False

    def test_get_job_returns_none_for_missing(self, sample_jsonl: Path):
        """get_job() returns None for nonexistent job."""
        index = JsonlIndex(sample_jsonl)
        assert index.get_job("nonexistent") is None

    def test_reset_job(self, small_jsonl: Path):
        """reset_job() removes job progress."""
        index = JsonlIndex(small_jsonl)

        # Create and checkpoint a job
        with index.batch_processor("reset_test") as batch:
            for i, _ in batch:
                if i == 5:
                    batch.checkpoint()
                    break

        # Reset it
        result = index.reset_job("reset_test")
        assert result is True

        # Job should be gone
        assert index.get_job("reset_test") is None

        # New batch should start at 0
        with index.batch_processor("reset_test") as batch:
            assert batch.position == 0

    def test_reset_job_returns_false_for_missing(self, sample_jsonl: Path):
        """reset_job() returns False for nonexistent job."""
        index = JsonlIndex(sample_jsonl)
        assert index.reset_job("nonexistent") is False

    def test_delete_completed_jobs(self, sample_jsonl: Path):
        """delete_completed_jobs() removes only completed jobs."""
        index = JsonlIndex(sample_jsonl)

        # Create jobs with different states
        with index.batch_processor("in_progress_job") as batch:
            for i, _ in batch:
                if i == 10:
                    batch.checkpoint()
                    break

        with index.batch_processor("completed_job1") as batch:
            for _ in batch:
                pass

        with index.batch_processor("completed_job2") as batch:
            for _ in batch:
                pass

        # Delete completed jobs
        count = index.delete_completed_jobs()
        assert count == 2

        # Only in-progress job remains
        jobs = index.list_jobs()
        assert len(jobs) == 1
        assert jobs[0].job_id == "in_progress_job"

    def test_delete_completed_jobs_returns_zero_when_none(self, sample_jsonl: Path):
        """delete_completed_jobs() returns 0 when no completed jobs."""
        index = JsonlIndex(sample_jsonl)

        # Create only in-progress job
        with index.batch_processor("in_progress") as batch:
            for i, _ in batch:
                if i == 5:
                    batch.checkpoint()
                    break

        count = index.delete_completed_jobs()
        assert count == 0


class TestCustomProgressPath:
    """Tests for custom progress file path."""

    def test_custom_progress_path(self, sample_jsonl: Path, tmp_path: Path):
        """Can use custom progress file path."""
        custom_path = tmp_path / "custom.progress"

        index = JsonlIndex(sample_jsonl)

        with index.batch_processor("custom_path_test", progress_path=custom_path) as batch:
            for i, _ in batch:
                if i == 10:
                    batch.checkpoint()
                    break

        # Progress should be in custom path
        assert custom_path.exists()
        assert not sample_jsonl.with_suffix(".progress").exists()

        # Methods should use same path
        jobs = index.list_jobs(progress_path=custom_path)
        assert len(jobs) == 1
        assert jobs[0].job_id == "custom_path_test"


class TestBatchProcessorReset:
    """Tests for BatchProcessor.reset() method."""

    def test_reset_clears_progress(self, small_jsonl: Path):
        """reset() method clears job progress."""
        index = JsonlIndex(small_jsonl)

        with index.batch_processor("reset_method_test") as batch:
            for i, _ in batch:
                if i == 5:
                    batch.checkpoint()
                    break
            batch.reset()

        # Job should be gone after reset
        assert index.get_job("reset_method_test") is None

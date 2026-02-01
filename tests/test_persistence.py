"""Tests for index persistence (save/load)."""

import json
from pathlib import Path

import pytest

from jsonl_resumable.models import IndexMeta, LineInfo
from jsonl_resumable.persistence import FORMAT_VERSION, load_index, save_index


@pytest.fixture
def sample_meta() -> IndexMeta:
    """Create sample IndexMeta for testing."""
    return IndexMeta(
        file_path="/path/to/file.jsonl",
        file_size=1234,
        file_mtime=1234567890.123,
        total_lines=100,
        checkpoint_interval=10,
        checkpoints={0: 0, 10: 100, 20: 200},
        indexed_at="2024-01-01T00:00:00+00:00",
        version="1.0",
    )


@pytest.fixture
def sample_lines() -> list[LineInfo]:
    """Create sample LineInfo list for testing."""
    return [
        LineInfo(line_number=i, offset=i * 50, length=50)
        for i in range(100)
    ]


class TestSaveIndex:
    """Tests for save_index function."""

    def test_saves_valid_json(
        self, tmp_path: Path, sample_meta: IndexMeta, sample_lines: list[LineInfo]
    ):
        """Saved index is valid JSON."""
        index_path = tmp_path / "test.idx"
        save_index(index_path, sample_meta, sample_lines)

        # Should be parseable as JSON
        with open(index_path) as f:
            data = json.load(f)

        assert "format_version" in data
        assert "meta" in data
        assert "lines" in data

    def test_includes_format_version(
        self, tmp_path: Path, sample_meta: IndexMeta, sample_lines: list[LineInfo]
    ):
        """Saved index includes format version."""
        index_path = tmp_path / "test.idx"
        save_index(index_path, sample_meta, sample_lines)

        with open(index_path) as f:
            data = json.load(f)

        assert data["format_version"] == FORMAT_VERSION

    def test_stores_meta_fields(
        self, tmp_path: Path, sample_meta: IndexMeta, sample_lines: list[LineInfo]
    ):
        """All metadata fields are stored."""
        index_path = tmp_path / "test.idx"
        save_index(index_path, sample_meta, sample_lines)

        with open(index_path) as f:
            data = json.load(f)

        meta = data["meta"]
        assert meta["file_path"] == sample_meta.file_path
        assert meta["file_size"] == sample_meta.file_size
        assert meta["file_mtime"] == sample_meta.file_mtime
        assert meta["total_lines"] == sample_meta.total_lines
        assert meta["checkpoint_interval"] == sample_meta.checkpoint_interval
        assert meta["indexed_at"] == sample_meta.indexed_at
        assert meta["version"] == sample_meta.version

    def test_stores_checkpoints(
        self, tmp_path: Path, sample_meta: IndexMeta, sample_lines: list[LineInfo]
    ):
        """Checkpoints are stored correctly."""
        index_path = tmp_path / "test.idx"
        save_index(index_path, sample_meta, sample_lines)

        with open(index_path) as f:
            data = json.load(f)

        # JSON stringifies dict keys
        checkpoints = data["meta"]["checkpoints"]
        assert checkpoints["0"] == 0
        assert checkpoints["10"] == 100
        assert checkpoints["20"] == 200

    def test_stores_lines_compactly(
        self, tmp_path: Path, sample_meta: IndexMeta, sample_lines: list[LineInfo]
    ):
        """Lines are stored as [offset, length] arrays."""
        index_path = tmp_path / "test.idx"
        save_index(index_path, sample_meta, sample_lines)

        with open(index_path) as f:
            data = json.load(f)

        lines = data["lines"]
        assert len(lines) == 100

        # First line
        assert lines[0] == [0, 50]
        # 50th line
        assert lines[50] == [2500, 50]

    def test_uses_compact_json(
        self, tmp_path: Path, sample_meta: IndexMeta, sample_lines: list[LineInfo]
    ):
        """JSON is written without extra whitespace."""
        index_path = tmp_path / "test.idx"
        save_index(index_path, sample_meta, sample_lines)

        content = index_path.read_text()

        # No pretty-printing (spaces after colons or commas)
        assert ": " not in content
        assert ", " not in content

    def test_overwrites_existing(
        self, tmp_path: Path, sample_meta: IndexMeta, sample_lines: list[LineInfo]
    ):
        """Can overwrite existing index file."""
        index_path = tmp_path / "test.idx"
        index_path.write_text("old content")

        save_index(index_path, sample_meta, sample_lines)

        with open(index_path) as f:
            data = json.load(f)

        assert data["format_version"] == FORMAT_VERSION

    def test_empty_lines(self, tmp_path: Path, sample_meta: IndexMeta):
        """Handles empty lines list."""
        index_path = tmp_path / "test.idx"
        sample_meta.total_lines = 0
        save_index(index_path, sample_meta, [])

        with open(index_path) as f:
            data = json.load(f)

        assert data["lines"] == []


class TestLoadIndex:
    """Tests for load_index function."""

    def test_loads_saved_index(
        self, tmp_path: Path, sample_meta: IndexMeta, sample_lines: list[LineInfo]
    ):
        """Can load a saved index."""
        index_path = tmp_path / "test.idx"
        save_index(index_path, sample_meta, sample_lines)

        result = load_index(index_path)

        assert result is not None
        loaded_meta, loaded_lines = result

        assert loaded_meta.file_path == sample_meta.file_path
        assert loaded_meta.total_lines == sample_meta.total_lines
        assert len(loaded_lines) == len(sample_lines)

    def test_restores_line_info(
        self, tmp_path: Path, sample_meta: IndexMeta, sample_lines: list[LineInfo]
    ):
        """LineInfo objects are fully restored."""
        index_path = tmp_path / "test.idx"
        save_index(index_path, sample_meta, sample_lines)

        result = load_index(index_path)
        assert result is not None
        _, loaded_lines = result

        # Check a few lines
        assert loaded_lines[0].line_number == 0
        assert loaded_lines[0].offset == 0
        assert loaded_lines[0].length == 50

        assert loaded_lines[50].line_number == 50
        assert loaded_lines[50].offset == 2500
        assert loaded_lines[50].length == 50

    def test_restores_checkpoints_as_int_keys(
        self, tmp_path: Path, sample_meta: IndexMeta, sample_lines: list[LineInfo]
    ):
        """Checkpoint keys are restored as integers."""
        index_path = tmp_path / "test.idx"
        save_index(index_path, sample_meta, sample_lines)

        result = load_index(index_path)
        assert result is not None
        loaded_meta, _ = result

        # Keys should be integers, not strings
        assert 0 in loaded_meta.checkpoints
        assert 10 in loaded_meta.checkpoints
        assert "0" not in loaded_meta.checkpoints  # type: ignore[operator]

    def test_missing_file_returns_none(self, tmp_path: Path):
        """Returns None for missing file."""
        result = load_index(tmp_path / "missing.idx")
        assert result is None

    def test_invalid_json_returns_none(self, tmp_path: Path):
        """Returns None for invalid JSON."""
        index_path = tmp_path / "invalid.idx"
        index_path.write_text("not valid json {{{")

        result = load_index(index_path)
        assert result is None

    def test_wrong_version_returns_none(self, tmp_path: Path):
        """Returns None for wrong format version."""
        index_path = tmp_path / "old.idx"
        data = {
            "format_version": "0.1",  # Wrong version
            "meta": {},
            "lines": [],
        }
        index_path.write_text(json.dumps(data))

        result = load_index(index_path)
        assert result is None

    def test_missing_version_returns_none(self, tmp_path: Path):
        """Returns None when format_version is missing."""
        index_path = tmp_path / "no_version.idx"
        data = {"meta": {}, "lines": []}
        index_path.write_text(json.dumps(data))

        result = load_index(index_path)
        assert result is None

    def test_missing_meta_returns_none(self, tmp_path: Path):
        """Returns None when meta section is missing."""
        index_path = tmp_path / "no_meta.idx"
        data = {"format_version": FORMAT_VERSION, "lines": []}
        index_path.write_text(json.dumps(data))

        result = load_index(index_path)
        assert result is None

    def test_missing_lines_returns_none(self, tmp_path: Path):
        """Returns None when lines section is missing."""
        index_path = tmp_path / "no_lines.idx"
        data = {
            "format_version": FORMAT_VERSION,
            "meta": {
                "file_path": "/test",
                "file_size": 100,
                "file_mtime": 123.0,
                "total_lines": 0,
                "checkpoint_interval": 10,
                "checkpoints": {},
                "indexed_at": "2024-01-01",
            },
        }
        index_path.write_text(json.dumps(data))

        result = load_index(index_path)
        assert result is None

    def test_malformed_meta_returns_none(self, tmp_path: Path):
        """Returns None when meta has missing required fields."""
        index_path = tmp_path / "bad_meta.idx"
        data = {
            "format_version": FORMAT_VERSION,
            "meta": {"file_path": "/test"},  # Missing other fields
            "lines": [],
        }
        index_path.write_text(json.dumps(data))

        result = load_index(index_path)
        assert result is None

    def test_malformed_lines_returns_none(self, tmp_path: Path):
        """Returns None when lines have wrong format."""
        index_path = tmp_path / "bad_lines.idx"
        data = {
            "format_version": FORMAT_VERSION,
            "meta": {
                "file_path": "/test",
                "file_size": 100,
                "file_mtime": 123.0,
                "total_lines": 1,
                "checkpoint_interval": 10,
                "checkpoints": {},
                "indexed_at": "2024-01-01",
            },
            "lines": [{"bad": "format"}],  # Should be [offset, length]
        }
        index_path.write_text(json.dumps(data))

        result = load_index(index_path)
        assert result is None

    def test_empty_file_returns_none(self, tmp_path: Path):
        """Returns None for empty file."""
        index_path = tmp_path / "empty.idx"
        index_path.write_text("")

        result = load_index(index_path)
        assert result is None

    def test_default_version_when_missing(
        self, tmp_path: Path, sample_meta: IndexMeta, sample_lines: list[LineInfo]
    ):
        """Uses default version when not in saved data."""
        index_path = tmp_path / "test.idx"

        # Manually create index without version field
        data = {
            "format_version": FORMAT_VERSION,
            "meta": {
                "file_path": sample_meta.file_path,
                "file_size": sample_meta.file_size,
                "file_mtime": sample_meta.file_mtime,
                "total_lines": sample_meta.total_lines,
                "checkpoint_interval": sample_meta.checkpoint_interval,
                "checkpoints": sample_meta.checkpoints,
                "indexed_at": sample_meta.indexed_at,
                # "version" intentionally omitted
            },
            "lines": [[line.offset, line.length] for line in sample_lines],
        }
        index_path.write_text(json.dumps(data))

        result = load_index(index_path)
        assert result is not None
        loaded_meta, _ = result
        assert loaded_meta.version == "1.0"


class TestRoundTrip:
    """Tests for save/load round-trip consistency."""

    def test_full_roundtrip(
        self, tmp_path: Path, sample_meta: IndexMeta, sample_lines: list[LineInfo]
    ):
        """Data survives complete save/load cycle."""
        index_path = tmp_path / "test.idx"

        save_index(index_path, sample_meta, sample_lines)
        result = load_index(index_path)

        assert result is not None
        loaded_meta, loaded_lines = result

        # Meta fields match
        assert loaded_meta.file_path == sample_meta.file_path
        assert loaded_meta.file_size == sample_meta.file_size
        assert loaded_meta.file_mtime == sample_meta.file_mtime
        assert loaded_meta.total_lines == sample_meta.total_lines
        assert loaded_meta.checkpoint_interval == sample_meta.checkpoint_interval
        assert loaded_meta.checkpoints == sample_meta.checkpoints
        assert loaded_meta.indexed_at == sample_meta.indexed_at
        assert loaded_meta.version == sample_meta.version

        # Lines match
        assert len(loaded_lines) == len(sample_lines)
        for original, loaded in zip(sample_lines, loaded_lines):
            assert loaded.line_number == original.line_number
            assert loaded.offset == original.offset
            assert loaded.length == original.length

    def test_roundtrip_with_unicode_path(self, tmp_path: Path, sample_lines: list[LineInfo]):
        """Handles unicode in file paths."""
        meta = IndexMeta(
            file_path="/path/to/文件.jsonl",
            file_size=100,
            file_mtime=123.0,
            total_lines=len(sample_lines),
            checkpoint_interval=10,
            checkpoints={},
        )

        index_path = tmp_path / "test.idx"
        save_index(index_path, meta, sample_lines)
        result = load_index(index_path)

        assert result is not None
        loaded_meta, _ = result
        assert loaded_meta.file_path == "/path/to/文件.jsonl"

    def test_roundtrip_with_large_numbers(self, tmp_path: Path):
        """Handles large file sizes and offsets."""
        # 10GB file
        large_size = 10 * 1024 * 1024 * 1024
        large_offset = 9 * 1024 * 1024 * 1024

        meta = IndexMeta(
            file_path="/huge.jsonl",
            file_size=large_size,
            file_mtime=123.0,
            total_lines=1,
            checkpoint_interval=100,
            checkpoints={0: 0},
        )
        lines = [LineInfo(line_number=0, offset=large_offset, length=1000)]

        index_path = tmp_path / "test.idx"
        save_index(index_path, meta, lines)
        result = load_index(index_path)

        assert result is not None
        loaded_meta, loaded_lines = result
        assert loaded_meta.file_size == large_size
        assert loaded_lines[0].offset == large_offset

    def test_roundtrip_preserves_float_precision(
        self, tmp_path: Path, sample_lines: list[LineInfo]
    ):
        """mtime float precision is preserved."""
        precise_mtime = 1234567890.123456

        meta = IndexMeta(
            file_path="/test.jsonl",
            file_size=100,
            file_mtime=precise_mtime,
            total_lines=len(sample_lines),
            checkpoint_interval=10,
            checkpoints={},
        )

        index_path = tmp_path / "test.idx"
        save_index(index_path, meta, sample_lines)
        result = load_index(index_path)

        assert result is not None
        loaded_meta, _ = result
        assert loaded_meta.file_mtime == precise_mtime

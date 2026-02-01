"""Tests for JsonlIndex."""

import json
from pathlib import Path

import pytest

from jsonl_resumable import JsonlIndex


@pytest.fixture
def sample_jsonl(tmp_path: Path) -> Path:
    """Create a sample JSONL file with 100 lines."""
    file_path = tmp_path / "sample.jsonl"
    with open(file_path, "w") as f:
        for i in range(100):
            f.write(json.dumps({"line": i, "data": f"content_{i}"}) + "\n")
    return file_path


@pytest.fixture
def large_jsonl(tmp_path: Path) -> Path:
    """Create a larger JSONL file with 10000 lines."""
    file_path = tmp_path / "large.jsonl"
    with open(file_path, "w") as f:
        for i in range(10000):
            f.write(json.dumps({"id": i, "value": i * 2}) + "\n")
    return file_path


class TestJsonlIndex:
    def test_creates_index(self, sample_jsonl: Path):
        """Index is created for a valid JSONL file."""
        index = JsonlIndex(sample_jsonl)
        assert index.total_lines == 100
        assert index.file_size > 0

    def test_file_not_found(self, tmp_path: Path):
        """Raises FileNotFoundError for missing file."""
        with pytest.raises(FileNotFoundError):
            JsonlIndex(tmp_path / "missing.jsonl")

    def test_get_offset(self, sample_jsonl: Path):
        """Can get offset for any line."""
        index = JsonlIndex(sample_jsonl)

        # First line starts at 0
        offset, length = index.get_offset(0)
        assert offset == 0
        assert length > 0

        # Later lines have positive offsets
        offset_50, _ = index.get_offset(50)
        assert offset_50 > 0

    def test_get_offset_out_of_range(self, sample_jsonl: Path):
        """Raises IndexError for out of range line numbers."""
        index = JsonlIndex(sample_jsonl)

        with pytest.raises(IndexError):
            index.get_offset(100)  # 0-99 valid

        with pytest.raises(IndexError):
            index.get_offset(-1)

    def test_read_line(self, sample_jsonl: Path):
        """Can read specific lines."""
        index = JsonlIndex(sample_jsonl)

        line_0 = index.read_line(0)
        assert '"line": 0' in line_0

        line_50 = index.read_line(50)
        assert '"line": 50' in line_50

    def test_read_json(self, sample_jsonl: Path):
        """Can read and parse lines as JSON."""
        index = JsonlIndex(sample_jsonl)

        data = index.read_json(42)
        assert data["line"] == 42
        assert data["data"] == "content_42"

    def test_iter_from(self, sample_jsonl: Path):
        """Can iterate from a specific line."""
        index = JsonlIndex(sample_jsonl)

        lines = list(index.iter_from(95))
        assert len(lines) == 5  # Lines 95-99

        # Verify content
        first = json.loads(lines[0])
        assert first["line"] == 95

    def test_iter_from_start(self, sample_jsonl: Path):
        """Iterating from 0 returns all lines."""
        index = JsonlIndex(sample_jsonl)

        lines = list(index.iter_from(0))
        assert len(lines) == 100

    def test_iter_from_past_end(self, sample_jsonl: Path):
        """Iterating past end returns empty."""
        index = JsonlIndex(sample_jsonl)

        lines = list(index.iter_from(1000))
        assert lines == []

    def test_iter_json_from(self, sample_jsonl: Path):
        """Can iterate as parsed JSON."""
        index = JsonlIndex(sample_jsonl)

        items = list(index.iter_json_from(98))
        assert len(items) == 2
        assert items[0]["line"] == 98
        assert items[1]["line"] == 99

    def test_len_and_getitem(self, sample_jsonl: Path):
        """Supports len() and indexing."""
        index = JsonlIndex(sample_jsonl)

        assert len(index) == 100
        assert '"line": 0' in index[0]
        assert '"line": 99' in index[99]

    def test_repr(self, sample_jsonl: Path):
        """Has readable repr."""
        index = JsonlIndex(sample_jsonl)
        r = repr(index)
        assert "JsonlIndex" in r
        assert "lines=100" in r


class TestIndexPersistence:
    def test_saves_and_loads_index(self, sample_jsonl: Path):
        """Index is saved and can be reloaded."""
        index1 = JsonlIndex(sample_jsonl)
        index_path = sample_jsonl.with_suffix(".idx")

        # Index file should exist
        assert index_path.exists()

        # Create new index - should load from disk
        index2 = JsonlIndex(sample_jsonl)
        assert index2.total_lines == index1.total_lines

    def test_rebuilds_on_file_change(self, sample_jsonl: Path):
        """Index rebuilds when file changes."""
        index1 = JsonlIndex(sample_jsonl)
        assert index1.total_lines == 100

        # Modify the file
        with open(sample_jsonl, "a") as f:
            f.write(json.dumps({"line": 100}) + "\n")

        # New index should detect change and rebuild
        index2 = JsonlIndex(sample_jsonl)
        assert index2.total_lines == 101

    def test_custom_index_path(self, sample_jsonl: Path, tmp_path: Path):
        """Can use custom index path."""
        custom_path = tmp_path / "custom.idx"
        JsonlIndex(sample_jsonl, index_path=custom_path)

        assert custom_path.exists()
        assert not sample_jsonl.with_suffix(".idx").exists()

    def test_auto_save_disabled(self, sample_jsonl: Path):
        """Can disable auto-save."""
        index = JsonlIndex(sample_jsonl, auto_save=False)
        index_path = sample_jsonl.with_suffix(".idx")

        # Index file should not exist
        assert not index_path.exists()

        # Manual save works
        index.save()
        assert index_path.exists()


class TestCheckpoints:
    def test_checkpoint_interval(self, large_jsonl: Path):
        """Checkpoints are created at specified interval."""
        index = JsonlIndex(large_jsonl, checkpoint_interval=100)

        # Should have checkpoints at 0, 100, 200, ...
        assert index._meta is not None
        checkpoints = index._meta.checkpoints

        assert 0 in checkpoints
        assert 100 in checkpoints
        assert 1000 in checkpoints
        assert 50 not in checkpoints  # Not on interval

    def test_small_checkpoint_interval(self, sample_jsonl: Path):
        """Smaller interval creates more checkpoints."""
        index = JsonlIndex(sample_jsonl, checkpoint_interval=10)

        assert index._meta is not None
        # Should have checkpoints at 0, 10, 20, ..., 90
        assert len(index._meta.checkpoints) == 10


class TestEdgeCases:
    def test_empty_file(self, tmp_path: Path):
        """Handles empty files."""
        empty_file = tmp_path / "empty.jsonl"
        empty_file.touch()

        index = JsonlIndex(empty_file)
        assert index.total_lines == 0
        assert list(index.iter_from(0)) == []

    def test_single_line(self, tmp_path: Path):
        """Handles single-line files."""
        single_file = tmp_path / "single.jsonl"
        single_file.write_text('{"only": "line"}\n')

        index = JsonlIndex(single_file)
        assert index.total_lines == 1
        assert index.read_json(0) == {"only": "line"}

    def test_no_trailing_newline(self, tmp_path: Path):
        """Handles files without trailing newline."""
        file_path = tmp_path / "no_newline.jsonl"
        file_path.write_text('{"a": 1}\n{"b": 2}')  # No newline after last

        index = JsonlIndex(file_path)
        assert index.total_lines == 2
        assert index.read_json(1) == {"b": 2}

    def test_unicode_content(self, tmp_path: Path):
        """Handles unicode content correctly."""
        file_path = tmp_path / "unicode.jsonl"
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(json.dumps({"emoji": "🎉", "chinese": "中文"}) + "\n")
            f.write(json.dumps({"arabic": "العربية"}) + "\n")

        index = JsonlIndex(file_path)
        assert index.total_lines == 2

        data = index.read_json(0)
        assert data["emoji"] == "🎉"
        assert data["chinese"] == "中文"

    def test_large_lines(self, tmp_path: Path):
        """Handles lines with large content."""
        file_path = tmp_path / "large_lines.jsonl"
        large_data = "x" * 100000

        with open(file_path, "w") as f:
            f.write(json.dumps({"data": large_data}) + "\n")
            f.write(json.dumps({"small": "line"}) + "\n")

        index = JsonlIndex(file_path)
        assert index.total_lines == 2

        # First line is large
        offset_0, length_0 = index.get_offset(0)
        offset_1, length_1 = index.get_offset(1)

        assert length_0 > 100000
        assert length_1 < 100

    def test_rebuild_forced(self, sample_jsonl: Path):
        """Can force rebuild even with valid index."""
        index = JsonlIndex(sample_jsonl)
        original_indexed_at = index._meta.indexed_at

        # Force rebuild
        import time
        time.sleep(0.01)  # Ensure different timestamp
        index.rebuild()

        assert index._meta.indexed_at != original_indexed_at


class TestIncrementalUpdate:
    def test_update_appended_lines(self, sample_jsonl: Path):
        """update() indexes only new appended lines."""
        index = JsonlIndex(sample_jsonl)
        assert index.total_lines == 100

        # Append new lines
        with open(sample_jsonl, "a") as f:
            for i in range(100, 110):
                f.write(json.dumps({"line": i}) + "\n")

        # Update should return count of new lines
        new_count = index.update()
        assert new_count == 10
        assert index.total_lines == 110

        # New lines are accessible
        assert index.read_json(100) == {"line": 100}
        assert index.read_json(109) == {"line": 109}

    def test_update_no_changes(self, sample_jsonl: Path):
        """update() returns 0 when file unchanged."""
        index = JsonlIndex(sample_jsonl)

        new_count = index.update()
        assert new_count == 0
        assert index.total_lines == 100

    def test_update_single_line(self, sample_jsonl: Path):
        """update() handles single line append."""
        index = JsonlIndex(sample_jsonl)

        with open(sample_jsonl, "a") as f:
            f.write(json.dumps({"line": 100}) + "\n")

        new_count = index.update()
        assert new_count == 1
        assert index.total_lines == 101

    def test_update_raises_on_shrink(self, sample_jsonl: Path):
        """update() raises ValueError if file shrunk."""
        index = JsonlIndex(sample_jsonl)

        # Truncate the file
        with open(sample_jsonl, "w") as f:
            f.write(json.dumps({"line": 0}) + "\n")

        with pytest.raises(ValueError, match="shrunk"):
            index.update()

    def test_update_saves_index(self, sample_jsonl: Path):
        """update() persists the updated index."""
        index = JsonlIndex(sample_jsonl)

        # Append and update
        with open(sample_jsonl, "a") as f:
            f.write(json.dumps({"line": 100}) + "\n")
        index.update()

        # Load fresh - should have 101 lines
        index2 = JsonlIndex(sample_jsonl)
        assert index2.total_lines == 101

    def test_update_preserves_checkpoints(self, sample_jsonl: Path):
        """update() adds new checkpoints correctly."""
        index = JsonlIndex(sample_jsonl, checkpoint_interval=10)
        old_checkpoints = set(index._meta.checkpoints.keys())

        # Append 20 more lines (should add checkpoints at 100, 110)
        with open(sample_jsonl, "a") as f:
            for i in range(100, 120):
                f.write(json.dumps({"line": i}) + "\n")

        index.update()

        new_checkpoints = set(index._meta.checkpoints.keys())
        added = new_checkpoints - old_checkpoints

        assert 100 in added
        assert 110 in added

    def test_update_multiple_times(self, sample_jsonl: Path):
        """update() can be called multiple times."""
        index = JsonlIndex(sample_jsonl)

        # First append
        with open(sample_jsonl, "a") as f:
            f.write(json.dumps({"batch": 1}) + "\n")
        assert index.update() == 1

        # Second append
        with open(sample_jsonl, "a") as f:
            f.write(json.dumps({"batch": 2}) + "\n")
            f.write(json.dumps({"batch": 2}) + "\n")
        assert index.update() == 2

        assert index.total_lines == 103
        assert index.read_json(102)["batch"] == 2


class TestBatchReads:
    """Tests for read_line_many() and read_json_many()."""

    def test_read_line_many(self, sample_jsonl: Path):
        """Can read multiple lines with single file open."""
        index = JsonlIndex(sample_jsonl)

        lines = index.read_line_many([0, 50, 99])
        assert len(lines) == 3
        assert '"line": 0' in lines[0]
        assert '"line": 50' in lines[1]
        assert '"line": 99' in lines[2]

    def test_read_json_many(self, sample_jsonl: Path):
        """Can read and parse multiple lines as JSON."""
        index = JsonlIndex(sample_jsonl)

        data = index.read_json_many([10, 20, 30])
        assert len(data) == 3
        assert data[0]["line"] == 10
        assert data[1]["line"] == 20
        assert data[2]["line"] == 30

    def test_read_many_empty_list(self, sample_jsonl: Path):
        """Handles empty list of line numbers."""
        index = JsonlIndex(sample_jsonl)

        assert index.read_line_many([]) == []
        assert index.read_json_many([]) == []

    def test_read_many_single_line(self, sample_jsonl: Path):
        """Handles single line number."""
        index = JsonlIndex(sample_jsonl)

        lines = index.read_line_many([42])
        assert len(lines) == 1
        assert '"line": 42' in lines[0]

    def test_read_many_out_of_range(self, sample_jsonl: Path):
        """Raises IndexError for out-of-range line numbers."""
        index = JsonlIndex(sample_jsonl)

        with pytest.raises(IndexError):
            index.read_line_many([0, 100])  # 100 is out of range

    def test_read_many_preserves_order(self, sample_jsonl: Path):
        """Results are in same order as requested line numbers."""
        index = JsonlIndex(sample_jsonl)

        # Request in non-sequential order
        data = index.read_json_many([99, 0, 50])
        assert data[0]["line"] == 99
        assert data[1]["line"] == 0
        assert data[2]["line"] == 50

    def test_read_many_with_duplicates(self, sample_jsonl: Path):
        """Handles duplicate line numbers."""
        index = JsonlIndex(sample_jsonl)

        data = index.read_json_many([5, 5, 5])
        assert len(data) == 3
        assert all(d["line"] == 5 for d in data)


class TestKeepOpen:
    """Tests for keep_open mode."""

    def test_keep_open_basic(self, sample_jsonl: Path):
        """Can read with keep_open=True."""
        index = JsonlIndex(sample_jsonl, keep_open=True)

        assert index.read_json(0)["line"] == 0
        assert index.read_json(50)["line"] == 50

        index.close()

    def test_keep_open_context_manager(self, sample_jsonl: Path):
        """Context manager closes file handle."""
        with JsonlIndex(sample_jsonl, keep_open=True) as index:
            assert index.read_json(0)["line"] == 0
            assert index._file_handle is not None

        # After context exit, handle should be closed
        assert index._file_handle is None

    def test_keep_open_explicit_close(self, sample_jsonl: Path):
        """Can explicitly close file handle."""
        index = JsonlIndex(sample_jsonl, keep_open=True)
        assert index._file_handle is not None

        index.close()
        assert index._file_handle is None

    def test_keep_open_close_idempotent(self, sample_jsonl: Path):
        """close() is safe to call multiple times."""
        index = JsonlIndex(sample_jsonl, keep_open=True)

        index.close()
        index.close()  # Should not raise

    def test_keep_open_reuses_handle(self, sample_jsonl: Path):
        """Verifies the same handle is reused."""
        with JsonlIndex(sample_jsonl, keep_open=True) as index:
            # Access the file handle
            with index.open() as f1:
                handle_id = id(f1)

            # Should be the same handle
            with index.open() as f2:
                assert id(f2) == handle_id

    def test_without_keep_open_creates_new_handles(self, sample_jsonl: Path):
        """Without keep_open, each open() creates new handle."""
        index = JsonlIndex(sample_jsonl, keep_open=False)

        # Each open() should create a new handle
        with index.open():
            pass

        # No persistent handle
        assert index._file_handle is None

    def test_context_manager_without_keep_open(self, sample_jsonl: Path):
        """Context manager works without keep_open."""
        with JsonlIndex(sample_jsonl) as index:
            assert index.read_json(0)["line"] == 0

        # close() is a no-op without keep_open
        assert index._file_handle is None

    def test_keep_open_with_batch_reads(self, sample_jsonl: Path):
        """keep_open works with batch read methods."""
        with JsonlIndex(sample_jsonl, keep_open=True) as index:
            data = index.read_json_many([0, 50, 99])
            assert len(data) == 3

    def test_keep_open_iter_from(self, sample_jsonl: Path):
        """keep_open works with iter_from."""
        with JsonlIndex(sample_jsonl, keep_open=True) as index:
            lines = list(index.iter_from(95))
            assert len(lines) == 5

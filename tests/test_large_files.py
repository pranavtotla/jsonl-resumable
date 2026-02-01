"""Tests for large file handling and performance characteristics."""

import json
import time
from pathlib import Path

import pytest

from jsonl_resumable import JsonlIndex


@pytest.fixture
def large_jsonl_100k(tmp_path: Path) -> Path:
    """Create a JSONL file with 100,000 lines."""
    file_path = tmp_path / "large_100k.jsonl"
    with open(file_path, "w") as f:
        for i in range(100_000):
            f.write(json.dumps({"id": i, "data": f"content_{i}"}) + "\n")
    return file_path


@pytest.fixture
def variable_length_jsonl(tmp_path: Path) -> Path:
    """Create a JSONL file with variable-length lines."""
    file_path = tmp_path / "variable.jsonl"
    with open(file_path, "w") as f:
        for i in range(10_000):
            # Varying line lengths: some short, some long
            if i % 100 == 0:
                data = "x" * 10_000  # Long line every 100th
            elif i % 10 == 0:
                data = "y" * 1000  # Medium line every 10th
            else:
                data = f"short_{i}"  # Short lines
            f.write(json.dumps({"id": i, "data": data}) + "\n")
    return file_path


class TestLargeFileIndexing:
    """Tests for indexing large files."""

    def test_indexes_100k_lines(self, large_jsonl_100k: Path):
        """Can index a file with 100k lines."""
        index = JsonlIndex(large_jsonl_100k, auto_save=False)

        assert index.total_lines == 100_000
        assert index.file_size > 0

    def test_checkpoints_created_for_large_file(self, large_jsonl_100k: Path):
        """Proper number of checkpoints created."""
        index = JsonlIndex(large_jsonl_100k, checkpoint_interval=1000, auto_save=False)

        assert index._meta is not None
        checkpoints = index._meta.checkpoints

        # Should have checkpoints at 0, 1000, 2000, ..., 99000
        assert len(checkpoints) == 100
        assert 0 in checkpoints
        assert 1000 in checkpoints
        assert 99000 in checkpoints

    def test_random_access_large_file(self, large_jsonl_100k: Path):
        """Can access random lines in large file."""
        index = JsonlIndex(large_jsonl_100k, auto_save=False)

        # Access various positions
        data_0 = index.read_json(0)
        data_middle = index.read_json(50_000)
        data_end = index.read_json(99_999)

        assert data_0["id"] == 0
        assert data_middle["id"] == 50_000
        assert data_end["id"] == 99_999

    def test_iter_from_middle_of_large_file(self, large_jsonl_100k: Path):
        """Can iterate from middle of large file."""
        index = JsonlIndex(large_jsonl_100k, auto_save=False)

        # Get 100 lines from middle
        lines = list(index.iter_from(50_000))[:100]

        assert len(lines) == 100
        first = json.loads(lines[0])
        assert first["id"] == 50_000

    def test_iter_last_lines_of_large_file(self, large_jsonl_100k: Path):
        """Can efficiently get last N lines."""
        index = JsonlIndex(large_jsonl_100k, auto_save=False)

        # Get last 10 lines
        start = index.total_lines - 10
        lines = list(index.iter_from(start))

        assert len(lines) == 10
        last = json.loads(lines[-1])
        assert last["id"] == 99_999


class TestVariableLengthLines:
    """Tests for files with variable-length lines."""

    def test_indexes_variable_length_file(self, variable_length_jsonl: Path):
        """Correctly indexes file with varying line lengths."""
        index = JsonlIndex(variable_length_jsonl, auto_save=False)

        assert index.total_lines == 10_000

    def test_offset_accuracy_variable_lines(self, variable_length_jsonl: Path):
        """Byte offsets are accurate for variable-length lines."""
        index = JsonlIndex(variable_length_jsonl, auto_save=False)

        # Read lines directly and via index, compare
        with open(variable_length_jsonl, "rb") as f:
            for line_num in [0, 100, 500, 1000, 5000, 9999]:
                offset, length = index.get_offset(line_num)
                f.seek(offset)
                raw = f.read(length)

                via_index = index.read_line(line_num)
                assert via_index == raw.decode("utf-8").rstrip("\n\r")

    def test_long_lines_accessible(self, variable_length_jsonl: Path):
        """Long lines (10KB) are correctly indexed and readable."""
        index = JsonlIndex(variable_length_jsonl, auto_save=False)

        # Line 0, 100, 200, etc. have 10KB data
        data = index.read_json(100)
        assert len(data["data"]) == 10_000
        assert data["data"] == "x" * 10_000


class TestSeekPerformance:
    """Tests verifying O(1) seek behavior."""

    def test_seek_time_constant(self, large_jsonl_100k: Path):
        """Seek time should be roughly constant regardless of position."""
        index = JsonlIndex(large_jsonl_100k, auto_save=False)

        # Time seeking to beginning
        start = time.perf_counter()
        for _ in range(100):
            index.read_line(10)
        time_beginning = time.perf_counter() - start

        # Time seeking to end
        start = time.perf_counter()
        for _ in range(100):
            index.read_line(99_990)
        time_end = time.perf_counter() - start

        # Times should be similar (within 3x is reasonable for I/O variance)
        ratio = max(time_beginning, time_end) / max(min(time_beginning, time_end), 0.0001)
        assert ratio < 3.0, f"Seek time ratio {ratio} too high"

    def test_get_offset_is_fast(self, large_jsonl_100k: Path):
        """get_offset should be extremely fast (memory lookup)."""
        index = JsonlIndex(large_jsonl_100k, auto_save=False)

        start = time.perf_counter()
        for i in range(10_000):
            index.get_offset(i * 10)
        elapsed = time.perf_counter() - start

        # 10k lookups should take < 100ms (they're just list indexing)
        assert elapsed < 0.1, f"get_offset took {elapsed:.3f}s for 10k calls"


class TestMemoryEfficiency:
    """Tests for memory-efficient checkpoint behavior."""

    def test_checkpoint_interval_affects_storage(self, large_jsonl_100k: Path):
        """Different checkpoint intervals produce different checkpoint counts."""
        index_small = JsonlIndex(
            large_jsonl_100k, checkpoint_interval=100, auto_save=False
        )
        index_large = JsonlIndex(
            large_jsonl_100k, checkpoint_interval=10000, auto_save=False
        )

        small_checkpoints = len(index_small._meta.checkpoints)
        large_checkpoints = len(index_large._meta.checkpoints)

        # 100k lines / 100 = 1000 checkpoints
        # 100k lines / 10000 = 10 checkpoints
        assert small_checkpoints == 1000
        assert large_checkpoints == 10

    def test_all_lines_stored(self, large_jsonl_100k: Path):
        """All lines are stored regardless of checkpoint interval."""
        index = JsonlIndex(large_jsonl_100k, checkpoint_interval=10000, auto_save=False)

        # Even with sparse checkpoints, all lines are accessible
        assert len(index._lines) == 100_000

        # Can access any line
        assert index.read_json(12345)["id"] == 12345


class TestPersistenceLargeFile:
    """Tests for persisting large file indexes."""

    def test_save_and_load_large_index(self, large_jsonl_100k: Path):
        """Large index can be saved and loaded."""
        # Create and save (index creation triggers save)
        JsonlIndex(large_jsonl_100k)

        # Load from disk
        index2 = JsonlIndex(large_jsonl_100k)

        assert index2.total_lines == 100_000

        # Verify random access still works
        assert index2.read_json(50_000)["id"] == 50_000

    def test_index_file_size_reasonable(self, large_jsonl_100k: Path):
        """Index file size is reasonable for large files."""
        JsonlIndex(large_jsonl_100k)  # Creates the index file
        index_path = large_jsonl_100k.with_suffix(".idx")

        data_size = large_jsonl_100k.stat().st_size
        index_size = index_path.stat().st_size

        # Index should be much smaller than data
        # Each line stores [offset, length] as JSON, ~20 bytes per line
        # 100k lines = ~2MB index, data is ~4MB
        ratio = index_size / data_size
        assert ratio < 1.0, f"Index size ratio {ratio} seems too large"


class TestLargeLineContent:
    """Tests for files with very large individual lines."""

    @pytest.fixture
    def huge_lines_jsonl(self, tmp_path: Path) -> Path:
        """Create file with some very large lines (1MB each)."""
        file_path = tmp_path / "huge_lines.jsonl"
        with open(file_path, "w") as f:
            for i in range(10):
                if i in [3, 7]:
                    # 1MB lines
                    data = "x" * (1024 * 1024)
                else:
                    data = f"small_{i}"
                f.write(json.dumps({"id": i, "data": data}) + "\n")
        return file_path

    def test_indexes_huge_lines(self, huge_lines_jsonl: Path):
        """Can index file with MB-sized lines."""
        index = JsonlIndex(huge_lines_jsonl, auto_save=False)

        assert index.total_lines == 10

        # Verify offsets make sense
        offset_3, length_3 = index.get_offset(3)
        offset_4, length_4 = index.get_offset(4)

        # Line 3 is huge, line 4 starts after it
        assert length_3 > 1024 * 1024
        assert offset_4 > offset_3 + length_3 - 1

    def test_reads_huge_lines(self, huge_lines_jsonl: Path):
        """Can read MB-sized lines correctly."""
        index = JsonlIndex(huge_lines_jsonl, auto_save=False)

        data = index.read_json(3)
        assert len(data["data"]) == 1024 * 1024
        assert data["data"] == "x" * (1024 * 1024)

    def test_seeks_past_huge_lines(self, huge_lines_jsonl: Path):
        """Can seek past huge lines to read small lines."""
        index = JsonlIndex(huge_lines_jsonl, auto_save=False)

        # Line 8 comes after two huge lines
        data = index.read_json(8)
        assert data["id"] == 8
        assert data["data"] == "small_8"


class TestIncrementalUpdateLarge:
    """Tests for incremental updates on large files."""

    def test_update_appends_to_large_file(self, large_jsonl_100k: Path):
        """Can incrementally update a large indexed file."""
        index = JsonlIndex(large_jsonl_100k)
        assert index.total_lines == 100_000

        # Append 1000 more lines
        with open(large_jsonl_100k, "a") as f:
            for i in range(100_000, 101_000):
                f.write(json.dumps({"id": i}) + "\n")

        new_count = index.update()

        assert new_count == 1000
        assert index.total_lines == 101_000
        assert index.read_json(100_500)["id"] == 100_500

    def test_update_faster_than_rebuild(self, large_jsonl_100k: Path):
        """Incremental update should be faster than full rebuild."""
        index = JsonlIndex(large_jsonl_100k)

        # Append a small amount
        with open(large_jsonl_100k, "a") as f:
            for i in range(100):
                f.write(json.dumps({"id": i}) + "\n")

        # Time update
        start = time.perf_counter()
        index.update()
        update_time = time.perf_counter() - start

        # Time rebuild
        start = time.perf_counter()
        index.rebuild()
        rebuild_time = time.perf_counter() - start

        # Update should be faster than rebuild
        # Note: on fast SSDs with OS caching, the difference may be modest
        assert update_time < rebuild_time, (
            f"Update ({update_time:.3f}s) should be faster than rebuild ({rebuild_time:.3f}s)"
        )


class TestBatchReadsLarge:
    """Tests for batch reads on large files."""

    def test_read_many_scattered_lines(self, large_jsonl_100k: Path):
        """Can batch read scattered lines efficiently."""
        index = JsonlIndex(large_jsonl_100k, auto_save=False)

        # Read 100 scattered lines
        line_numbers = list(range(0, 100_000, 1000))
        data = index.read_json_many(line_numbers)

        assert len(data) == 100
        assert data[0]["id"] == 0
        assert data[50]["id"] == 50_000
        assert data[99]["id"] == 99_000

    def test_batch_read_faster_than_individual(self, large_jsonl_100k: Path):
        """Batch read should be faster than individual reads."""
        index = JsonlIndex(large_jsonl_100k, auto_save=False)
        line_numbers = list(range(0, 1000, 10))  # 100 lines

        # Time individual reads
        start = time.perf_counter()
        for n in line_numbers:
            index.read_json(n)
        individual_time = time.perf_counter() - start

        # Time batch read
        start = time.perf_counter()
        index.read_json_many(line_numbers)
        batch_time = time.perf_counter() - start

        # Batch should be faster (fewer file opens)
        # Note: difference may be small due to OS caching
        assert batch_time <= individual_time * 1.5, (
            f"Batch ({batch_time:.3f}s) slower than individual ({individual_time:.3f}s)"
        )

"""Tests for async iteration functionality."""

import asyncio
import json
from pathlib import Path

import pytest

from jsonl_resumable import (
    FileDeletedError,
    FileTruncatedError,
    JsonlIndex,
    LineCorruptedError,
)

# ─────────────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────────────


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
    """Create a larger JSONL file with 1000 lines."""
    file_path = tmp_path / "large.jsonl"
    with open(file_path, "w") as f:
        for i in range(1000):
            f.write(json.dumps({"id": i, "value": i * 2}) + "\n")
    return file_path


@pytest.fixture
def mixed_jsonl(tmp_path: Path) -> Path:
    """Create JSONL with some invalid JSON lines."""
    file_path = tmp_path / "mixed.jsonl"
    with open(file_path, "w") as f:
        f.write('{"valid": 1}\n')
        f.write('invalid json line\n')
        f.write('{"valid": 2}\n')
        f.write('also not json\n')
        f.write('{"valid": 3}\n')
    return file_path


# ─────────────────────────────────────────────────────────────────────
# Basic Async Iteration Tests
# ─────────────────────────────────────────────────────────────────────


class TestAiterFrom:
    """Tests for aiter_from() method."""

    async def test_basic_iteration(self, sample_jsonl: Path):
        """Can async iterate all lines."""
        index = JsonlIndex(sample_jsonl)
        lines = [line async for line in index.aiter_from()]
        assert len(lines) == 100
        assert '"line": 0' in lines[0]
        assert '"line": 99' in lines[99]

    async def test_start_from_middle(self, sample_jsonl: Path):
        """Can start iteration from middle of file."""
        index = JsonlIndex(sample_jsonl)
        lines = [line async for line in index.aiter_from(95)]
        assert len(lines) == 5

        # Verify content
        first = json.loads(lines[0])
        assert first["line"] == 95

    async def test_start_past_end(self, sample_jsonl: Path):
        """Starting past end returns empty."""
        index = JsonlIndex(sample_jsonl)
        lines = [line async for line in index.aiter_from(1000)]
        assert lines == []

    async def test_negative_start(self, sample_jsonl: Path):
        """Negative start is treated as 0."""
        index = JsonlIndex(sample_jsonl)
        lines = [line async for line in index.aiter_from(-10)]
        assert len(lines) == 100

    async def test_skip_parameter(self, sample_jsonl: Path):
        """Skip parameter skips lines from start."""
        index = JsonlIndex(sample_jsonl)
        lines = [line async for line in index.aiter_from(0, skip=90)]
        assert len(lines) == 10

        first = json.loads(lines[0])
        assert first["line"] == 90

    async def test_limit_parameter(self, sample_jsonl: Path):
        """Limit parameter caps output."""
        index = JsonlIndex(sample_jsonl)
        lines = [line async for line in index.aiter_from(0, limit=10)]
        assert len(lines) == 10

    async def test_skip_and_limit(self, sample_jsonl: Path):
        """Skip and limit work together."""
        index = JsonlIndex(sample_jsonl)
        lines = [line async for line in index.aiter_from(0, skip=50, limit=10)]
        assert len(lines) == 10

        first = json.loads(lines[0])
        assert first["line"] == 50

        last = json.loads(lines[-1])
        assert last["line"] == 59

    async def test_limit_exceeds_remaining(self, sample_jsonl: Path):
        """Limit larger than remaining lines returns what's available."""
        index = JsonlIndex(sample_jsonl)
        lines = [line async for line in index.aiter_from(95, limit=100)]
        assert len(lines) == 5


class TestAiterJsonFrom:
    """Tests for aiter_json_from() method."""

    async def test_basic_json_iteration(self, sample_jsonl: Path):
        """Can async iterate as parsed JSON."""
        index = JsonlIndex(sample_jsonl)
        items = [item async for item in index.aiter_json_from()]
        assert len(items) == 100
        assert items[0]["line"] == 0
        assert items[99]["line"] == 99

    async def test_json_with_skip_and_limit(self, sample_jsonl: Path):
        """Skip and limit work with JSON iteration."""
        index = JsonlIndex(sample_jsonl)
        items = [item async for item in index.aiter_json_from(0, skip=10, limit=5)]
        assert len(items) == 5
        assert items[0]["line"] == 10

    async def test_on_decode_error_raise(self, mixed_jsonl: Path):
        """on_decode_error='raise' raises JSONDecodeError."""
        index = JsonlIndex(mixed_jsonl)
        with pytest.raises(json.JSONDecodeError):
            [item async for item in index.aiter_json_from(on_decode_error="raise")]

    async def test_on_decode_error_skip(self, mixed_jsonl: Path):
        """on_decode_error='skip' skips invalid lines."""
        index = JsonlIndex(mixed_jsonl)
        items = [item async for item in index.aiter_json_from(on_decode_error="skip")]
        assert len(items) == 3
        assert items[0]["valid"] == 1
        assert items[1]["valid"] == 2
        assert items[2]["valid"] == 3

    async def test_on_decode_error_raw(self, mixed_jsonl: Path):
        """on_decode_error='raw' yields raw string for invalid lines."""
        index = JsonlIndex(mixed_jsonl)
        items = [item async for item in index.aiter_json_from(on_decode_error="raw")]
        assert len(items) == 5
        assert items[0]["valid"] == 1
        assert items[1] == "invalid json line"
        assert items[2]["valid"] == 2
        assert items[3] == "also not json"
        assert items[4]["valid"] == 3


class TestAiterRawFrom:
    """Tests for aiter_raw_from() method."""

    async def test_raw_bytes_iteration(self, sample_jsonl: Path):
        """Can async iterate raw bytes."""
        index = JsonlIndex(sample_jsonl)
        raw_lines = [raw async for raw in index.aiter_raw_from()]
        assert len(raw_lines) == 100

        # Raw bytes include newline
        assert raw_lines[0].endswith(b"\n")
        assert b'"line": 0' in raw_lines[0]

    async def test_raw_with_skip_and_limit(self, sample_jsonl: Path):
        """Skip and limit work with raw iteration."""
        index = JsonlIndex(sample_jsonl)
        raw_lines = [raw async for raw in index.aiter_raw_from(0, skip=90, limit=5)]
        assert len(raw_lines) == 5
        assert b'"line": 90' in raw_lines[0]


# ─────────────────────────────────────────────────────────────────────
# Batch Size Tests
# ─────────────────────────────────────────────────────────────────────


class TestBatchSize:
    """Tests for batch_size parameter."""

    async def test_batch_size_1(self, sample_jsonl: Path):
        """batch_size=1 works (max thread hops)."""
        index = JsonlIndex(sample_jsonl)
        lines = [line async for line in index.aiter_from(batch_size=1)]
        assert len(lines) == 100

    async def test_batch_size_10(self, sample_jsonl: Path):
        """batch_size=10 works."""
        index = JsonlIndex(sample_jsonl)
        lines = [line async for line in index.aiter_from(batch_size=10)]
        assert len(lines) == 100

    async def test_batch_size_larger_than_file(self, sample_jsonl: Path):
        """batch_size larger than file works."""
        index = JsonlIndex(sample_jsonl)
        lines = [line async for line in index.aiter_from(batch_size=1000)]
        assert len(lines) == 100

    async def test_batch_size_with_limit(self, large_jsonl: Path):
        """batch_size respects limit across batches."""
        index = JsonlIndex(large_jsonl)
        lines = [line async for line in index.aiter_from(batch_size=30, limit=55)]
        assert len(lines) == 55


# ─────────────────────────────────────────────────────────────────────
# AsyncStreamContext Tests
# ─────────────────────────────────────────────────────────────────────


class TestAsyncStreamContext:
    """Tests for async_stream() context manager."""

    async def test_basic_stream(self, sample_jsonl: Path):
        """Can use async_stream context manager."""
        index = JsonlIndex(sample_jsonl)
        async with index.async_stream() as stream:
            items = [item async for item in stream]

        assert len(items) == 100
        assert stream.yielded_count == 100
        assert stream.closed

    async def test_stream_with_limit(self, sample_jsonl: Path):
        """Limit works in stream context."""
        index = JsonlIndex(sample_jsonl)
        async with index.async_stream(limit=10) as stream:
            items = [item async for item in stream]

        assert len(items) == 10
        assert stream.yielded_count == 10

    async def test_stream_as_strings(self, sample_jsonl: Path):
        """as_json=False returns strings."""
        index = JsonlIndex(sample_jsonl)
        async with index.async_stream(as_json=False) as stream:
            items = [item async for item in stream]

        assert len(items) == 100
        assert isinstance(items[0], str)
        assert '"line": 0' in items[0]

    async def test_stream_position_tracking(self, sample_jsonl: Path):
        """Position is tracked during iteration."""
        index = JsonlIndex(sample_jsonl)
        async with index.async_stream(start_line=50, limit=10) as stream:
            positions = []
            async for _ in stream:
                positions.append(stream.position)

        # Position advances as we iterate
        assert positions == list(range(51, 61))

    async def test_stream_error_handling(self, mixed_jsonl: Path):
        """on_decode_error works in stream context."""
        index = JsonlIndex(mixed_jsonl)
        async with index.async_stream(on_decode_error="skip") as stream:
            items = [item async for item in stream]

        assert len(items) == 3
        assert stream.yielded_count == 3


class TestAsyncRawStreamContext:
    """Tests for async_raw_stream() context manager."""

    async def test_raw_stream(self, sample_jsonl: Path):
        """Can use async_raw_stream context manager."""
        index = JsonlIndex(sample_jsonl)
        async with index.async_raw_stream() as stream:
            items = [item async for item in stream]

        assert len(items) == 100
        assert isinstance(items[0], bytes)
        assert stream.yielded_count == 100


# ─────────────────────────────────────────────────────────────────────
# File Modification Tests
# ─────────────────────────────────────────────────────────────────────


class TestFileModification:
    """Tests for handling file modifications during iteration."""

    async def test_file_deleted_during_iteration(self, sample_jsonl: Path):
        """FileDeletedError raised when file is deleted."""
        index = JsonlIndex(sample_jsonl)

        # Delete file before starting
        sample_jsonl.unlink()

        with pytest.raises(FileDeletedError) as exc:
            async with index.async_stream():
                pass

        assert exc.value.file_path == sample_jsonl

    async def test_file_truncated_on_entry(self, sample_jsonl: Path):
        """FileTruncatedError raised when file shrunk."""
        index = JsonlIndex(sample_jsonl)
        original_size = index.file_size

        # Truncate the file
        with open(sample_jsonl, "w") as f:
            f.write('{"only": "one"}\n')

        with pytest.raises(FileTruncatedError) as exc:
            async with index.async_stream():
                pass

        assert exc.value.file_path == sample_jsonl
        assert exc.value.expected_size == original_size

    async def test_file_appended_during_iteration(self, sample_jsonl: Path):
        """File appends don't break iteration (index unchanged)."""
        index = JsonlIndex(sample_jsonl)

        items = []
        async with index.async_stream() as stream:
            count = 0
            async for item in stream:
                items.append(item)
                count += 1
                # Append after reading 50 items
                if count == 50:
                    with open(sample_jsonl, "a") as f:
                        f.write(json.dumps({"new": "line"}) + "\n")

        # Should still read original 100 lines (index wasn't updated)
        assert len(items) == 100

    async def test_line_corrupted_detection(self, sample_jsonl: Path):
        """LineCorruptedError raised when bytes don't match index."""
        index = JsonlIndex(sample_jsonl)

        # Corrupt the file by changing content length
        with open(sample_jsonl, "r+b") as f:
            # Truncate to simulate corruption
            f.truncate(50)

        with pytest.raises(LineCorruptedError):
            [line async for line in index.aiter_from()]


# ─────────────────────────────────────────────────────────────────────
# Generator Cleanup Tests
# ─────────────────────────────────────────────────────────────────────


class TestGeneratorCleanup:
    """Tests for proper generator cleanup."""

    async def test_break_early(self, sample_jsonl: Path):
        """Breaking early still closes properly."""
        index = JsonlIndex(sample_jsonl)

        count = 0
        async with index.async_stream() as stream:
            async for _ in stream:
                count += 1
                if count >= 10:
                    break

        assert count == 10
        assert stream.closed

    async def test_exception_during_iteration(self, sample_jsonl: Path):
        """Exception during iteration still closes properly."""
        index = JsonlIndex(sample_jsonl)

        with pytest.raises(ValueError):
            async with index.async_stream() as stream:
                count = 0
                async for _ in stream:
                    count += 1
                    if count >= 10:
                        raise ValueError("test error")

        assert stream.closed

    async def test_explicit_aclose(self, sample_jsonl: Path):
        """Can explicitly close async generator."""
        index = JsonlIndex(sample_jsonl)

        gen = index.aiter_from()
        items = []
        async for item in gen:
            items.append(item)
            if len(items) >= 5:
                await gen.aclose()
                break

        assert len(items) == 5

    async def test_context_guarantees_cleanup(self, sample_jsonl: Path):
        """Context manager always cleans up."""
        index = JsonlIndex(sample_jsonl)

        stream = index.async_stream()
        assert not stream.closed

        async with stream:
            pass

        assert stream.closed


# ─────────────────────────────────────────────────────────────────────
# Concurrency Tests
# ─────────────────────────────────────────────────────────────────────


class TestConcurrency:
    """Tests for concurrent async iterations."""

    async def test_multiple_concurrent_iterations(self, sample_jsonl: Path):
        """Can run multiple iterations concurrently."""
        index = JsonlIndex(sample_jsonl)

        async def iterate_slice(start: int, limit: int) -> list[dict]:
            items = []
            async for item in index.aiter_json_from(start, limit=limit):
                items.append(item)
            return items

        # Run three iterations concurrently
        results = await asyncio.gather(
            iterate_slice(0, 30),
            iterate_slice(30, 30),
            iterate_slice(60, 40),
        )

        assert len(results[0]) == 30
        assert len(results[1]) == 30
        assert len(results[2]) == 40

        # Check they got different data
        assert results[0][0]["line"] == 0
        assert results[1][0]["line"] == 30
        assert results[2][0]["line"] == 60

    async def test_concurrent_stream_contexts(self, sample_jsonl: Path):
        """Can use multiple stream contexts concurrently."""
        index = JsonlIndex(sample_jsonl)

        async def stream_slice(start: int, limit: int) -> int:
            async with index.async_stream(start_line=start, limit=limit) as stream:
                count = 0
                async for _ in stream:
                    count += 1
                return count

        results = await asyncio.gather(
            stream_slice(0, 50),
            stream_slice(50, 50),
        )

        assert results == [50, 50]


# ─────────────────────────────────────────────────────────────────────
# Edge Cases
# ─────────────────────────────────────────────────────────────────────


class TestEdgeCases:
    """Tests for edge cases."""

    async def test_empty_file(self, tmp_path: Path):
        """Empty file yields nothing."""
        empty_file = tmp_path / "empty.jsonl"
        empty_file.touch()

        index = JsonlIndex(empty_file)
        lines = [line async for line in index.aiter_from()]
        assert lines == []

    async def test_single_line_file(self, tmp_path: Path):
        """Single line file works."""
        single_file = tmp_path / "single.jsonl"
        single_file.write_text('{"only": "line"}\n')

        index = JsonlIndex(single_file)
        items = [item async for item in index.aiter_json_from()]
        assert len(items) == 1
        assert items[0]["only"] == "line"

    async def test_unicode_content(self, tmp_path: Path):
        """Unicode content handled correctly."""
        unicode_file = tmp_path / "unicode.jsonl"
        with open(unicode_file, "w", encoding="utf-8") as f:
            f.write(json.dumps({"emoji": "🎉", "chinese": "中文"}) + "\n")
            f.write(json.dumps({"arabic": "العربية"}) + "\n")

        index = JsonlIndex(unicode_file)
        items = [item async for item in index.aiter_json_from()]

        assert len(items) == 2
        assert items[0]["emoji"] == "🎉"
        assert items[0]["chinese"] == "中文"
        assert items[1]["arabic"] == "العربية"

    async def test_very_long_line(self, tmp_path: Path):
        """Very long lines work."""
        long_file = tmp_path / "long.jsonl"
        large_data = "x" * 100_000

        with open(long_file, "w") as f:
            f.write(json.dumps({"data": large_data}) + "\n")
            f.write(json.dumps({"small": "line"}) + "\n")

        index = JsonlIndex(long_file)
        items = [item async for item in index.aiter_json_from()]

        assert len(items) == 2
        assert len(items[0]["data"]) == 100_000
        assert items[1]["small"] == "line"

    async def test_whitespace_only_lines(self, tmp_path: Path):
        """Lines with only whitespace handled correctly."""
        ws_file = tmp_path / "whitespace.jsonl"
        with open(ws_file, "w") as f:
            f.write('{"a": 1}\n')
            f.write('   \n')  # Whitespace line
            f.write('{"b": 2}\n')

        index = JsonlIndex(ws_file)

        # Should raise on whitespace line (not valid JSON)
        with pytest.raises(json.JSONDecodeError):
            [item async for item in index.aiter_json_from(on_decode_error="raise")]

        # Skip mode handles it
        items = [item async for item in index.aiter_json_from(on_decode_error="skip")]
        assert len(items) == 2

    async def test_no_trailing_newline(self, tmp_path: Path):
        """File without trailing newline works."""
        no_newline = tmp_path / "no_newline.jsonl"
        no_newline.write_text('{"a": 1}\n{"b": 2}')

        index = JsonlIndex(no_newline)
        items = [item async for item in index.aiter_json_from()]
        assert len(items) == 2
        assert items[0]["a"] == 1
        assert items[1]["b"] == 2


# ─────────────────────────────────────────────────────────────────────
# Sync Raw Iteration Tests
# ─────────────────────────────────────────────────────────────────────


class TestIterRawFrom:
    """Tests for sync iter_raw_from() method."""

    def test_raw_sync_iteration(self, sample_jsonl: Path):
        """Can sync iterate raw bytes."""
        index = JsonlIndex(sample_jsonl)
        raw_lines = list(index.iter_raw_from())
        assert len(raw_lines) == 100
        assert raw_lines[0].endswith(b"\n")
        assert b'"line": 0' in raw_lines[0]

    def test_raw_from_middle(self, sample_jsonl: Path):
        """Can start raw iteration from middle."""
        index = JsonlIndex(sample_jsonl)
        raw_lines = list(index.iter_raw_from(95))
        assert len(raw_lines) == 5
        assert b'"line": 95' in raw_lines[0]

    def test_raw_past_end(self, sample_jsonl: Path):
        """Raw iteration past end returns empty."""
        index = JsonlIndex(sample_jsonl)
        raw_lines = list(index.iter_raw_from(1000))
        assert raw_lines == []

    def test_raw_negative_start(self, sample_jsonl: Path):
        """Negative start is treated as 0."""
        index = JsonlIndex(sample_jsonl)
        raw_lines = list(index.iter_raw_from(-10))
        assert len(raw_lines) == 100


# ─────────────────────────────────────────────────────────────────────
# Integration with keep_open
# ─────────────────────────────────────────────────────────────────────


class TestKeepOpenAsync:
    """Tests for async iteration with keep_open mode."""

    async def test_async_with_keep_open(self, sample_jsonl: Path):
        """Async iteration works with keep_open=True."""
        with JsonlIndex(sample_jsonl, keep_open=True) as index:
            items = [item async for item in index.aiter_json_from(limit=10)]
            assert len(items) == 10

    async def test_concurrent_async_with_keep_open(self, sample_jsonl: Path):
        """Concurrent async iterations work with keep_open=True."""
        with JsonlIndex(sample_jsonl, keep_open=True) as index:
            results = await asyncio.gather(
                self._collect(index.aiter_json_from(0, limit=50)),
                self._collect(index.aiter_json_from(50, limit=50)),
            )

            assert len(results[0]) == 50
            assert len(results[1]) == 50

    @staticmethod
    async def _collect(gen):
        return [item async for item in gen]

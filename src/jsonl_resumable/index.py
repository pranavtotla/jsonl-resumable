"""Core indexing and seeking functionality."""

from __future__ import annotations

import json
import random
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import IO, Any, Iterator, Union

from .batch import BatchProcessor
from .models import IndexMeta, JobInfo, JobProgress, LineInfo
from .persistence import load_index, save_index
from .progress import delete_job_progress, load_progress


class JsonlIndex:
    """Byte-offset index for O(1) seeking in JSONL files.

    Builds an index mapping line numbers to byte offsets, enabling instant
    random access to any line without parsing the entire file.

    Example:
        >>> index = JsonlIndex("events.jsonl")
        >>> print(f"Total lines: {index.total_lines}")
        >>> for line in index.iter_from(1000):
        ...     event = json.loads(line)
        ...     process(event)
    """

    def __init__(
        self,
        file_path: Union[str, Path],
        checkpoint_interval: int = 100,
        index_path: Union[str, Path, None] = None,
        auto_save: bool = True,
        keep_open: bool = False,
    ) -> None:
        """Create or load an index for a JSONL file.

        Args:
            file_path: Path to the JSONL file to index
            checkpoint_interval: Store checkpoint every N lines (lower = more memory,
                faster seeking; higher = less memory, slightly slower seeking)
            index_path: Where to persist the index. Defaults to {file_path}.idx
            auto_save: Automatically save index after building
            keep_open: Keep file handle open for repeated reads (use with context manager)
        """
        self._file_path = Path(file_path).resolve()
        self._checkpoint_interval = checkpoint_interval
        self._index_path = Path(index_path) if index_path else self._file_path.with_suffix(".idx")
        self._auto_save = auto_save
        self._keep_open = keep_open
        self._file_handle: IO[bytes] | None = None

        self._meta: IndexMeta | None = None
        self._lines: list[LineInfo] = []

        self._load_or_build()

        if keep_open:
            self._file_handle = open(self._file_path, "rb")

    def _load_or_build(self) -> None:
        """Load existing index if fresh, otherwise build new one."""
        if not self._file_path.exists():
            raise FileNotFoundError(f"JSONL file not found: {self._file_path}")

        stat = self._file_path.stat()

        # Try loading existing index
        if self._index_path.exists():
            loaded = load_index(self._index_path)
            if loaded and loaded[0].is_fresh(stat.st_size, stat.st_mtime):
                self._meta, self._lines = loaded
                return

        # Build fresh index
        self._build_index(stat.st_size, stat.st_mtime)

        if self._auto_save:
            self.save()

    def _build_index(self, file_size: int, file_mtime: float) -> None:
        """Build byte-offset index for the JSONL file."""
        lines: list[LineInfo] = []
        checkpoints: dict[int, int] = {}
        offset = 0

        with open(self._file_path, "rb") as f:
            for line_number, line in enumerate(f):
                lines.append(
                    LineInfo(
                        line_number=line_number,
                        offset=offset,
                        length=len(line),
                    )
                )

                if line_number % self._checkpoint_interval == 0:
                    checkpoints[line_number] = offset

                offset += len(line)

        self._lines = lines
        self._meta = IndexMeta(
            file_path=str(self._file_path),
            file_size=file_size,
            file_mtime=file_mtime,
            total_lines=len(lines),
            checkpoint_interval=self._checkpoint_interval,
            checkpoints=checkpoints,
        )

    @property
    def total_lines(self) -> int:
        """Total number of lines in the indexed file."""
        return self._meta.total_lines if self._meta else 0

    @property
    def file_size(self) -> int:
        """Size of the indexed file in bytes."""
        return self._meta.file_size if self._meta else 0

    @property
    def file_path(self) -> Path:
        """Path to the indexed file."""
        return self._file_path

    def get_offset(self, line_number: int) -> tuple[int, int]:
        """Get byte offset and length for a specific line.

        Args:
            line_number: 0-indexed line number

        Returns:
            Tuple of (byte_offset, length)

        Raises:
            IndexError: If line_number is out of range
        """
        if line_number < 0 or line_number >= len(self._lines):
            raise IndexError(
                f"Line {line_number} out of range (0-{len(self._lines) - 1})"
            )
        info = self._lines[line_number]
        return info.offset, info.length

    def seek_line(self, file_handle: IO[bytes], line_number: int) -> str:
        """Seek to and read a specific line.

        Args:
            file_handle: Open file handle in binary mode
            line_number: 0-indexed line number

        Returns:
            The line content (decoded as UTF-8, newline stripped)

        Raises:
            IndexError: If line_number is out of range
        """
        offset, length = self.get_offset(line_number)
        file_handle.seek(offset)
        return file_handle.read(length).decode("utf-8").rstrip("\n\r")

    def read_line(self, line_number: int) -> str:
        """Read a specific line (opens file internally).

        Args:
            line_number: 0-indexed line number

        Returns:
            The line content (decoded as UTF-8, newline stripped)

        Raises:
            IndexError: If line_number is out of range
        """
        with self.open() as f:
            return self.seek_line(f, line_number)

    def read_json(self, line_number: int) -> Any:
        """Read and parse a specific line as JSON.

        Args:
            line_number: 0-indexed line number

        Returns:
            Parsed JSON object

        Raises:
            IndexError: If line_number is out of range
            json.JSONDecodeError: If line is not valid JSON
        """
        return json.loads(self.read_line(line_number))

    def read_line_many(self, line_numbers: list[int]) -> list[str]:
        """Read multiple lines with a single file open.

        More efficient than calling read_line() in a loop when you need
        multiple random lines, as it opens the file only once.

        Args:
            line_numbers: List of 0-indexed line numbers

        Returns:
            List of line contents in the same order as requested

        Raises:
            IndexError: If any line_number is out of range
        """
        with self.open() as f:
            return [self.seek_line(f, n) for n in line_numbers]

    def read_json_many(self, line_numbers: list[int]) -> list[Any]:
        """Read and parse multiple lines as JSON with a single file open.

        More efficient than calling read_json() in a loop when you need
        multiple random records, as it opens the file only once.

        Args:
            line_numbers: List of 0-indexed line numbers

        Returns:
            List of parsed JSON objects in the same order as requested

        Raises:
            IndexError: If any line_number is out of range
            json.JSONDecodeError: If any line is not valid JSON
        """
        return [json.loads(line) for line in self.read_line_many(line_numbers)]

    def iter_from(self, start_line: int = 0) -> Iterator[str]:
        """Iterate lines starting from a specific line.

        Args:
            start_line: 0-indexed line to start from (default: 0)

        Yields:
            Lines as strings (decoded, newline stripped)
        """
        if start_line < 0:
            start_line = 0
        if start_line >= len(self._lines):
            return

        with self.open() as f:
            # Seek to start position
            offset, _ = self.get_offset(start_line)
            f.seek(offset)

            # Read remaining lines
            for line in f:
                yield line.decode("utf-8").rstrip("\n\r")

    def iter_json_from(self, start_line: int = 0) -> Iterator[Any]:
        """Iterate lines as parsed JSON starting from a specific line.

        Args:
            start_line: 0-indexed line to start from (default: 0)

        Yields:
            Parsed JSON objects
        """
        for line in self.iter_from(start_line):
            yield json.loads(line)

    @contextmanager
    def open(self) -> Iterator[IO[bytes]]:
        """Open the indexed file for binary reading.

        If keep_open=True was set, reuses the persistent file handle.

        Yields:
            File handle in binary read mode
        """
        if self._file_handle:
            yield self._file_handle
        else:
            with open(self._file_path, "rb") as f:
                yield f

    def close(self) -> None:
        """Close the file handle if keep_open=True was used."""
        if self._file_handle:
            self._file_handle.close()
            self._file_handle = None

    def __enter__(self) -> "JsonlIndex":
        """Enter context manager."""
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> None:
        """Exit context manager and close file handle."""
        self.close()

    def rebuild(self) -> None:
        """Force rebuild the index, ignoring any cached version."""
        stat = self._file_path.stat()
        self._build_index(stat.st_size, stat.st_mtime)
        if self._auto_save:
            self.save()

    def update(self) -> int:
        """Incrementally index new lines appended to the file.

        Call this after appending to the JSONL file to index only the new
        portion, avoiding a full rebuild.

        Returns:
            Number of new lines indexed

        Raises:
            ValueError: If file was modified (not just appended) - use rebuild() instead

        Example:
            >>> index = JsonlIndex("events.jsonl")
            >>> # ... append new events to file ...
            >>> new_count = index.update()
            >>> print(f"Indexed {new_count} new lines")
        """
        if not self._meta:
            self.rebuild()
            return self.total_lines

        stat = self._file_path.stat()
        old_size = self._meta.file_size

        # No changes
        if stat.st_size == old_size:
            return 0

        # File shrunk or was modified - require full rebuild
        if stat.st_size < old_size:
            raise ValueError(
                f"File shrunk from {old_size} to {stat.st_size} bytes. "
                "Use rebuild() for modified files."
            )

        # Index only the new portion
        new_lines: list[LineInfo] = []
        new_checkpoints: dict[int, int] = {}
        line_number = len(self._lines)
        offset = old_size

        with open(self._file_path, "rb") as f:
            f.seek(old_size)
            for line in f:
                new_lines.append(
                    LineInfo(
                        line_number=line_number,
                        offset=offset,
                        length=len(line),
                    )
                )

                if line_number % self._checkpoint_interval == 0:
                    new_checkpoints[line_number] = offset

                offset += len(line)
                line_number += 1

        # Merge new data
        self._lines.extend(new_lines)
        self._meta.checkpoints.update(new_checkpoints)
        self._meta.file_size = stat.st_size
        self._meta.file_mtime = stat.st_mtime
        self._meta.total_lines = len(self._lines)

        if self._auto_save:
            self.save()

        return len(new_lines)

    def save(self) -> None:
        """Persist index to disk."""
        if self._meta:
            save_index(self._index_path, self._meta, self._lines)

    def __len__(self) -> int:
        """Return total number of lines."""
        return self.total_lines

    def __getitem__(self, line_number: int) -> str:
        """Get a line by index (e.g., index[100])."""
        return self.read_line(line_number)

    def sample(self, n: int, *, seed: int | None = None) -> list[Any]:
        """Random sample of n records from the file.

        Efficiently samples n random lines by:
        1. Selecting random line numbers
        2. Sorting them for sequential disk access (minimizes seeks)
        3. Reading and parsing as JSON

        Args:
            n: Number of records to sample. If n > total_lines, returns all lines.
            seed: Random seed for reproducibility. If None, uses system randomness.

        Returns:
            List of n parsed JSON objects in random order (not sorted by line number).

        Example:
            >>> index = JsonlIndex("events.jsonl")
            >>> sample = index.sample(100, seed=42)  # reproducible sample
        """
        if self.total_lines == 0:
            return []

        # Use local Random instance to avoid affecting global state
        rng = random.Random(seed)

        # Clamp n to available lines
        n = min(n, self.total_lines)

        # Select random line numbers
        line_numbers = rng.sample(range(self.total_lines), n)

        # Sort for sequential disk access, but remember original order
        sorted_indices = sorted(range(n), key=lambda i: line_numbers[i])
        sorted_line_numbers = [line_numbers[i] for i in sorted_indices]

        # Read in sorted order for disk efficiency
        sorted_records = self.read_json_many(sorted_line_numbers)

        # Restore original random order
        result: list[Any] = [None] * n
        for original_idx, record in zip(sorted_indices, sorted_records):
            result[original_idx] = record

        return result

    def __repr__(self) -> str:
        return f"JsonlIndex({self._file_path!r}, lines={self.total_lines})"

    # ─────────────────────────────────────────────────────────────────────
    # Batch Processing API
    # ─────────────────────────────────────────────────────────────────────

    def batch_processor(
        self,
        job_id: str,
        progress_path: Union[str, Path, None] = None,
        as_json: bool = True,
    ) -> BatchProcessor:
        """Create a resumable batch processor for this file.

        Args:
            job_id: Unique identifier for this processing job
            progress_path: Where to store progress. Defaults to {file}.progress
            as_json: If True, parse lines as JSON; if False, return raw strings

        Returns:
            BatchProcessor that must be used as a context manager

        Example:
            >>> with index.batch_processor("my_job") as batch:
            ...     for line_num, record in batch:
            ...         result = process(record)
            ...         batch.checkpoint()
        """
        path = Path(progress_path) if progress_path else None
        return BatchProcessor(self, job_id, path, as_json)

    def list_jobs(
        self, progress_path: Union[str, Path, None] = None
    ) -> list[JobInfo]:
        """List all jobs for this file.

        Args:
            progress_path: Path to progress file. Defaults to {file}.progress

        Returns:
            List of JobInfo objects for all known jobs
        """
        path = (
            Path(progress_path)
            if progress_path
            else self._file_path.with_suffix(".progress")
        )
        jobs = load_progress(path)
        if not jobs:
            return []

        stat = self._file_path.stat()
        result = []
        for job in jobs.values():
            result.append(self._job_to_info(job, stat.st_size, stat.st_mtime))
        return result

    def get_job(
        self, job_id: str, progress_path: Union[str, Path, None] = None
    ) -> JobInfo | None:
        """Get information about a specific job.

        Args:
            job_id: The job identifier
            progress_path: Path to progress file. Defaults to {file}.progress

        Returns:
            JobInfo if job exists, None otherwise
        """
        path = (
            Path(progress_path)
            if progress_path
            else self._file_path.with_suffix(".progress")
        )
        jobs = load_progress(path)
        if not jobs or job_id not in jobs:
            return None

        stat = self._file_path.stat()
        return self._job_to_info(jobs[job_id], stat.st_size, stat.st_mtime)

    def reset_job(
        self, job_id: str, progress_path: Union[str, Path, None] = None
    ) -> bool:
        """Reset a job to start from the beginning.

        Args:
            job_id: The job identifier
            progress_path: Path to progress file. Defaults to {file}.progress

        Returns:
            True if job was found and reset, False if job didn't exist
        """
        path = (
            Path(progress_path)
            if progress_path
            else self._file_path.with_suffix(".progress")
        )
        return delete_job_progress(path, job_id)

    def delete_job(
        self, job_id: str, progress_path: Union[str, Path, None] = None
    ) -> bool:
        """Delete a job's progress record.

        Args:
            job_id: The job identifier
            progress_path: Path to progress file. Defaults to {file}.progress

        Returns:
            True if job was found and deleted, False if job didn't exist
        """
        return self.reset_job(job_id, progress_path)

    def delete_completed_jobs(
        self, progress_path: Union[str, Path, None] = None
    ) -> int:
        """Delete all completed jobs for this file.

        Args:
            progress_path: Path to progress file. Defaults to {file}.progress

        Returns:
            Number of jobs deleted
        """
        from .progress import save_progress

        path = (
            Path(progress_path)
            if progress_path
            else self._file_path.with_suffix(".progress")
        )
        jobs = load_progress(path)
        if not jobs:
            return 0

        completed = [jid for jid, job in jobs.items() if job.status == "completed"]
        for jid in completed:
            del jobs[jid]

        if completed:
            save_progress(path, jobs)

        return len(completed)

    def _job_to_info(
        self,
        job: JobProgress,
        current_size: int,
        current_mtime: float,
    ) -> JobInfo:
        """Convert internal JobProgress to external JobInfo."""
        is_stale = job.file_size != current_size or job.file_mtime != current_mtime
        progress_pct = (
            (job.position / self.total_lines * 100.0) if self.total_lines > 0 else 100.0
        )

        return JobInfo(
            job_id=job.job_id,
            position=job.position,
            status=job.status,
            total_lines=self.total_lines,
            progress_pct=progress_pct,
            created_at=datetime.fromisoformat(job.created_at),
            last_checkpoint_at=datetime.fromisoformat(job.last_checkpoint_at),
            completed_at=(
                datetime.fromisoformat(job.completed_at) if job.completed_at else None
            ),
            is_stale=is_stale,
        )

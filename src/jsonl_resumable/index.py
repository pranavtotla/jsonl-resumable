"""Core indexing and seeking functionality."""

from __future__ import annotations

import json
from contextlib import contextmanager
from pathlib import Path
from typing import IO, Any, Iterator, Union

from .models import IndexMeta, LineInfo
from .persistence import load_index, save_index


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

    def __repr__(self) -> str:
        return f"JsonlIndex({self._file_path!r}, lines={self.total_lines})"

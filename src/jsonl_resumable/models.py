"""Data models for jsonl-resumable."""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict


@dataclass(frozen=True, slots=True)
class LineInfo:
    """Information about a single line in the file.

    Attributes:
        line_number: 0-indexed line number
        offset: Byte offset from start of file
        length: Length in bytes (including newline)
    """

    line_number: int
    offset: int
    length: int


@dataclass
class IndexMeta:
    """Metadata about an indexed JSONL file.

    Attributes:
        file_path: Absolute path to the indexed file
        file_size: Size of file in bytes at index time
        file_mtime: File modification time at index time
        total_lines: Total number of lines in file
        checkpoint_interval: Lines between stored checkpoints
        checkpoints: Mapping of line_number -> byte_offset for quick seeking
        indexed_at: ISO timestamp when index was built
        version: Index format version
    """

    file_path: str
    file_size: int
    file_mtime: float
    total_lines: int
    checkpoint_interval: int
    checkpoints: Dict[int, int] = field(default_factory=dict)
    indexed_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    version: str = "1.0"

    def is_fresh(self, current_size: int, current_mtime: float) -> bool:
        """Check if index is still valid for the file.

        Returns True if file size and mtime match the indexed values.
        """
        return self.file_size == current_size and self.file_mtime == current_mtime

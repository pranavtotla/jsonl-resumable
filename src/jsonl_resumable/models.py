"""Data models for jsonl-resumable."""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, Literal


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


@dataclass
class JobProgress:
    """Progress state for a batch processing job.

    Attributes:
        job_id: Unique identifier for this job
        position: Next line number to process (0-indexed)
        file_size: File size in bytes at last checkpoint
        file_mtime: File modification time at last checkpoint
        status: Current job status
        created_at: ISO timestamp when job was created
        last_checkpoint_at: ISO timestamp of last checkpoint
        completed_at: ISO timestamp when job completed (None if in progress)
    """

    job_id: str
    position: int
    file_size: int
    file_mtime: float
    status: Literal["in_progress", "completed"]
    created_at: str
    last_checkpoint_at: str
    completed_at: str | None = None


@dataclass(frozen=True)
class JobInfo:
    """Read-only job information exposed to users.

    Attributes:
        job_id: Unique identifier for this job
        position: Next line number to process (0-indexed)
        status: Current job status
        total_lines: Total lines in the file
        progress_pct: Completion percentage (0.0-100.0)
        created_at: When job was created
        last_checkpoint_at: When last checkpoint was saved
        completed_at: When job completed (None if in progress)
        is_stale: True if file changed since last checkpoint
    """

    job_id: str
    position: int
    status: Literal["in_progress", "completed"]
    total_lines: int
    progress_pct: float
    created_at: datetime
    last_checkpoint_at: datetime
    completed_at: datetime | None
    is_stale: bool

"""Batch processing with checkpointing for JSONL files."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, Iterator

from .exceptions import InvalidCheckpointError, StaleCheckpointError
from .models import JobProgress
from .progress import delete_job_progress, load_progress, update_job_progress

if TYPE_CHECKING:
    from .index import JsonlIndex


class BatchProcessor:
    """Resumable batch processor with checkpoint support.

    Provides resumable iteration over a JSONL file with persistent checkpoints.
    If processing is interrupted, the next run will resume from the last
    checkpoint rather than starting over.

    Example:
        >>> with index.batch_processor("my_job") as batch:
        ...     for line_num, record in batch:
        ...         result = process(record)
        ...         batch.checkpoint()  # Save progress periodically
    """

    def __init__(
        self,
        index: "JsonlIndex",
        job_id: str,
        progress_path: Path | None = None,
        as_json: bool = True,
    ) -> None:
        """Create a batch processor.

        Args:
            index: The JsonlIndex to iterate over
            job_id: Unique identifier for this processing job
            progress_path: Where to store progress. Defaults to {file}.progress
            as_json: If True, parse lines as JSON; if False, return raw strings
        """
        self._index = index
        self._job_id = job_id
        self._progress_path = progress_path or index.file_path.with_suffix(".progress")
        self._as_json = as_json

        self._job: JobProgress | None = None
        self._position = 0
        self._exhausted = False
        self._entered = False

    def __enter__(self) -> "BatchProcessor":
        """Enter context manager: load or create job progress."""
        self._entered = True
        stat = self._index.file_path.stat()

        # Try to load existing progress
        jobs = load_progress(self._progress_path)
        if jobs and self._job_id in jobs:
            self._job = jobs[self._job_id]

            # Validate freshness
            if (
                self._job.file_size != stat.st_size
                or self._job.file_mtime != stat.st_mtime
            ):
                raise StaleCheckpointError(
                    f"File has changed since last checkpoint for job '{self._job_id}'. "
                    f"Expected size={self._job.file_size}, mtime={self._job.file_mtime}; "
                    f"got size={stat.st_size}, mtime={stat.st_mtime}. "
                    "Use reset_job() to restart from the beginning."
                )

            # Validate position
            if self._job.position > self._index.total_lines:
                raise InvalidCheckpointError(
                    f"Checkpoint position {self._job.position} exceeds "
                    f"total lines {self._index.total_lines} for job '{self._job_id}'."
                )

            self._position = self._job.position
        else:
            # Create new job
            now = datetime.now(timezone.utc).isoformat()
            self._job = JobProgress(
                job_id=self._job_id,
                position=0,
                file_size=stat.st_size,
                file_mtime=stat.st_mtime,
                status="in_progress",
                created_at=now,
                last_checkpoint_at=now,
            )
            update_job_progress(self._progress_path, self._job)
            self._position = 0

        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> None:
        """Exit context manager: mark complete if exhausted without error."""
        if exc_type is None and self._exhausted and self._job:
            # Successfully processed all items
            now = datetime.now(timezone.utc).isoformat()
            self._job.status = "completed"
            self._job.completed_at = now
            self._job.last_checkpoint_at = now
            update_job_progress(self._progress_path, self._job)

    def __iter__(self) -> Iterator[tuple[int, Any]]:
        """Iterate over remaining items, yielding (line_number, content).

        Yields:
            Tuples of (line_number, content) where content is either
            parsed JSON or raw string depending on as_json setting.
        """
        if not self._entered:
            raise RuntimeError(
                "BatchProcessor must be used as a context manager. "
                "Use 'with index.batch_processor(job_id) as batch:'"
            )

        if self._job and self._job.status == "completed":
            # Already completed - nothing to yield
            self._exhausted = True
            return

        for line in self._index.iter_from(self._position):
            line_num = self._position
            self._position += 1

            if self._as_json:
                yield line_num, json.loads(line)
            else:
                yield line_num, line

        self._exhausted = True

    def checkpoint(self) -> None:
        """Save current progress to disk.

        Call this periodically during processing to persist progress.
        On resume, processing will continue from the last checkpoint.
        """
        if not self._job:
            raise RuntimeError("Cannot checkpoint outside of context manager")

        now = datetime.now(timezone.utc).isoformat()
        self._job.position = self._position
        self._job.last_checkpoint_at = now
        update_job_progress(self._progress_path, self._job)

    @property
    def position(self) -> int:
        """Current position (next line to be processed)."""
        return self._position

    @property
    def total_lines(self) -> int:
        """Total number of lines in the file."""
        return self._index.total_lines

    @property
    def progress(self) -> float:
        """Progress percentage (0.0 to 100.0)."""
        if self.total_lines == 0:
            return 100.0
        return (self._position / self.total_lines) * 100.0

    @property
    def job_id(self) -> str:
        """The job identifier."""
        return self._job_id

    def reset(self) -> None:
        """Reset this job to start from the beginning.

        Removes the existing checkpoint and resets position to 0.
        """
        delete_job_progress(self._progress_path, self._job_id)
        self._position = 0
        self._job = None
        self._exhausted = False

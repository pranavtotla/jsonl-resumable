"""Custom exceptions for jsonl-resumable."""

from pathlib import Path


class StaleCheckpointError(Exception):
    """Raised when file has been modified since the last checkpoint.

    This indicates the checkpoint is no longer valid because the underlying
    file has changed (different size or modification time). The job should
    be reset and restarted from the beginning.
    """


class InvalidCheckpointError(Exception):
    """Raised when checkpoint position exceeds total lines in file.

    This can happen if:
    - The file was truncated
    - The checkpoint data is corrupted
    - The checkpoint belongs to a different file
    """


# ─────────────────────────────────────────────────────────────────────
# Async Iteration Exceptions
# ─────────────────────────────────────────────────────────────────────


class AsyncIterationError(Exception):
    """Base class for async iteration errors.

    All errors that can occur during async iteration inherit from this,
    allowing users to catch all async-related errors with a single except.
    """


class FileDeletedError(AsyncIterationError):
    """File was deleted during iteration.

    Raised when the underlying JSONL file is deleted while an async
    iteration is in progress.

    Attributes:
        file_path: Path to the deleted file
    """

    def __init__(self, file_path: Path | str) -> None:
        self.file_path = Path(file_path)
        super().__init__(f"File was deleted during iteration: {self.file_path}")


class FileTruncatedError(AsyncIterationError):
    """File was truncated during iteration.

    Raised when the file size shrinks during iteration, indicating
    the file was modified in a way that invalidates the index.

    Attributes:
        file_path: Path to the truncated file
        expected_size: Size in bytes when iteration started
        actual_size: Current size in bytes
    """

    def __init__(
        self, file_path: Path | str, expected_size: int, actual_size: int
    ) -> None:
        self.file_path = Path(file_path)
        self.expected_size = expected_size
        self.actual_size = actual_size
        super().__init__(
            f"File was truncated during iteration: {self.file_path} "
            f"(expected {expected_size} bytes, got {actual_size} bytes)"
        )


class LineCorruptedError(AsyncIterationError):
    """Line doesn't match expected byte length.

    Raised when the bytes read for a line don't match what the index
    expected, indicating file corruption or concurrent modification.

    Attributes:
        line_number: The line number that couldn't be read correctly
        expected_length: Expected byte length from index
        actual_length: Actual bytes read
    """

    def __init__(
        self, line_number: int, expected_length: int, actual_length: int
    ) -> None:
        self.line_number = line_number
        self.expected_length = expected_length
        self.actual_length = actual_length
        super().__init__(
            f"Line {line_number} corrupted: expected {expected_length} bytes, "
            f"got {actual_length} bytes"
        )

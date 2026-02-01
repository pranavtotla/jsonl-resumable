"""Custom exceptions for jsonl-resumable."""


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

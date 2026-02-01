"""jsonl-resumable: O(1) resume for large JSONL streams via byte-offset indexing.

Example:
    >>> from jsonl_resumable import JsonlIndex
    >>> index = JsonlIndex("events.jsonl")
    >>> print(f"Total lines: {index.total_lines}")
    >>>
    >>> # Random access
    >>> event = index.read_json(5000)
    >>>
    >>> # Resume from line 1000
    >>> for event in index.iter_json_from(1000):
    ...     process(event)
    >>>
    >>> # Batch processing with checkpoints
    >>> with index.batch_processor("my_job") as batch:
    ...     for line_num, record in batch:
    ...         process(record)
    ...         batch.checkpoint()
    >>>
    >>> # Async iteration (for web frameworks like FastAPI)
    >>> async with index.async_stream(start_line=1000) as stream:
    ...     async for record in stream:
    ...         await process(record)
"""

from .async_stream import AsyncRawStreamContext, AsyncStreamContext
from .batch import BatchProcessor
from .exceptions import (
    AsyncIterationError,
    FileDeletedError,
    FileTruncatedError,
    InvalidCheckpointError,
    LineCorruptedError,
    StaleCheckpointError,
)
from .index import JsonlIndex
from .models import IndexMeta, JobInfo, LineInfo

__version__ = "0.5.0"
__all__ = [
    # Core
    "JsonlIndex",
    "IndexMeta",
    "LineInfo",
    # Batch Processing
    "BatchProcessor",
    "JobInfo",
    # Async Streaming
    "AsyncStreamContext",
    "AsyncRawStreamContext",
    # Exceptions
    "StaleCheckpointError",
    "InvalidCheckpointError",
    "AsyncIterationError",
    "FileDeletedError",
    "FileTruncatedError",
    "LineCorruptedError",
]

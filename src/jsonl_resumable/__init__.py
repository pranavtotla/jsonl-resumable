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
"""

from .index import JsonlIndex
from .models import IndexMeta, LineInfo

__version__ = "0.3.0"
__all__ = ["JsonlIndex", "IndexMeta", "LineInfo"]

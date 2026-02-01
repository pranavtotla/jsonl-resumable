"""Async streaming context manager for jsonl-resumable."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, AsyncIterator, Literal

from .exceptions import FileDeletedError, FileTruncatedError

if TYPE_CHECKING:
    from .index import JsonlIndex


class AsyncStreamContext:
    """Async context manager with proper lifecycle management.

    Provides a clean interface for async iteration with:
    - File existence/truncation validation on entry
    - Guaranteed cleanup on exit (even on break/exception)
    - Progress tracking (position, yielded_count)

    Example:
        >>> async with index.async_stream(start_line=1000) as stream:
        ...     async for record in stream:
        ...         await process(record)
        ...     print(f"Processed {stream.yielded_count} records")
        ... # File handle guaranteed closed here
    """

    def __init__(
        self,
        index: "JsonlIndex",
        start_line: int = 0,
        *,
        batch_size: int = 100,
        skip: int = 0,
        limit: int | None = None,
        on_decode_error: Literal["raise", "skip", "raw"] = "raise",
        as_json: bool = True,
    ) -> None:
        """Initialize async stream context.

        Args:
            index: The JsonlIndex to iterate over
            start_line: 0-indexed line to start from
            batch_size: Number of lines to read per thread hop
            skip: Number of lines to skip from start_line
            limit: Maximum number of items to yield (None = unlimited)
            on_decode_error: How to handle JSON decode errors:
                - "raise": Re-raise the JSONDecodeError
                - "skip": Skip invalid lines silently
                - "raw": Yield the raw string instead of parsed JSON
            as_json: If True, parse lines as JSON; if False, return strings
        """
        self._index = index
        self._start_line = start_line
        self._batch_size = batch_size
        self._skip = skip
        self._limit = limit
        self._on_decode_error = on_decode_error
        self._as_json = as_json

        self._closed = False
        self._position = start_line + skip
        self._yielded_count = 0
        self._iterator: AsyncIterator[Any] | None = None
        self._initial_file_size: int = 0

    async def __aenter__(self) -> "AsyncStreamContext":
        """Enter async context, validating file state.

        Raises:
            FileDeletedError: If the file doesn't exist
            FileTruncatedError: If the file has shrunk since indexing
        """
        # Validate file exists
        if not self._index.file_path.exists():
            raise FileDeletedError(self._index.file_path)

        # Validate file hasn't shrunk
        current_size = self._index.file_path.stat().st_size
        indexed_size = self._index.file_size

        if current_size < indexed_size:
            raise FileTruncatedError(
                self._index.file_path, indexed_size, current_size
            )

        self._initial_file_size = current_size
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> None:
        """Exit async context, ensuring cleanup."""
        self._closed = True
        if self._iterator is not None and hasattr(self._iterator, "aclose"):
            await self._iterator.aclose()
            self._iterator = None

    def __aiter__(self) -> AsyncIterator[Any]:
        """Return async iterator."""
        return self._iterate()

    async def _iterate(self) -> AsyncIterator[Any]:
        """Internal iteration logic."""
        if self._closed:
            return

        if self._as_json:
            async for item in self._index.aiter_json_from(
                self._start_line,
                batch_size=self._batch_size,
                skip=self._skip,
                limit=self._limit,
                on_decode_error=self._on_decode_error,
            ):
                self._position += 1
                self._yielded_count += 1
                yield item
        else:
            async for line in self._index.aiter_from(
                self._start_line,
                batch_size=self._batch_size,
                skip=self._skip,
                limit=self._limit,
            ):
                self._position += 1
                self._yielded_count += 1
                yield line

    @property
    def position(self) -> int:
        """Current line position (next line to be yielded)."""
        return self._position

    @property
    def yielded_count(self) -> int:
        """Number of items yielded so far."""
        return self._yielded_count

    @property
    def closed(self) -> bool:
        """Whether the stream has been closed."""
        return self._closed

    @property
    def initial_file_size(self) -> int:
        """File size when the stream was opened."""
        return self._initial_file_size


class AsyncRawStreamContext:
    """Async context manager for raw bytes iteration.

    Similar to AsyncStreamContext but yields raw bytes instead of
    decoded strings or JSON. Most efficient for proxying data.

    Example:
        >>> async with index.async_raw_stream(start_line=1000) as stream:
        ...     async for raw_bytes in stream:
        ...         await response.write(raw_bytes)
    """

    def __init__(
        self,
        index: "JsonlIndex",
        start_line: int = 0,
        *,
        batch_size: int = 100,
        skip: int = 0,
        limit: int | None = None,
    ) -> None:
        """Initialize async raw stream context.

        Args:
            index: The JsonlIndex to iterate over
            start_line: 0-indexed line to start from
            batch_size: Number of lines to read per thread hop
            skip: Number of lines to skip from start_line
            limit: Maximum number of items to yield (None = unlimited)
        """
        self._index = index
        self._start_line = start_line
        self._batch_size = batch_size
        self._skip = skip
        self._limit = limit

        self._closed = False
        self._position = start_line + skip
        self._yielded_count = 0
        self._iterator: AsyncIterator[bytes] | None = None
        self._initial_file_size: int = 0

    async def __aenter__(self) -> "AsyncRawStreamContext":
        """Enter async context, validating file state."""
        if not self._index.file_path.exists():
            raise FileDeletedError(self._index.file_path)

        current_size = self._index.file_path.stat().st_size
        indexed_size = self._index.file_size

        if current_size < indexed_size:
            raise FileTruncatedError(
                self._index.file_path, indexed_size, current_size
            )

        self._initial_file_size = current_size
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> None:
        """Exit async context, ensuring cleanup."""
        self._closed = True
        if self._iterator is not None and hasattr(self._iterator, "aclose"):
            await self._iterator.aclose()
            self._iterator = None

    def __aiter__(self) -> AsyncIterator[bytes]:
        """Return async iterator."""
        return self._iterate()

    async def _iterate(self) -> AsyncIterator[bytes]:
        """Internal iteration logic."""
        if self._closed:
            return

        async for raw in self._index.aiter_raw_from(
            self._start_line,
            batch_size=self._batch_size,
            skip=self._skip,
            limit=self._limit,
        ):
            self._position += 1
            self._yielded_count += 1
            yield raw

    @property
    def position(self) -> int:
        """Current line position."""
        return self._position

    @property
    def yielded_count(self) -> int:
        """Number of items yielded so far."""
        return self._yielded_count

    @property
    def closed(self) -> bool:
        """Whether the stream has been closed."""
        return self._closed

    @property
    def initial_file_size(self) -> int:
        """File size when the stream was opened."""
        return self._initial_file_size

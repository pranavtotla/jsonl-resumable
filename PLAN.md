# jsonl-resumable

**O(1) resume for large JSONL streams via byte-offset indexing.**

## Problem

When streaming large JSONL files (LLM responses, ETL pipelines, event logs), you often need to:
- Resume from line N after a crash/disconnect
- Seek to a specific line without parsing everything before it
- Know the exact byte position of any line

Naive approach: Read and count lines from the start. **O(n)** every time.

This package: Build an index once, seek instantly. **O(1)** forever.

## Core API

```python
from jsonl_resumable import JsonlIndex

# Index a file (builds byte-offset map)
index = JsonlIndex("events.jsonl")

# Get total lines
print(index.total_lines)  # 50000

# Seek to line 25000 instantly
with index.open() as f:
    line = index.seek_line(f, 25000)
    data = json.loads(line)

# Get byte offset for any line
offset, length = index.get_offset(25000)

# Iterate from line N
for line in index.iter_from(25000):
    process(json.loads(line))
```

## Features

| Feature | Description |
|---------|-------------|
| **O(1) seek** | Jump to any line by byte offset |
| **Checkpoints** | Every N lines for memory efficiency |
| **Persistence** | Save/load index to avoid rebuild |
| **Freshness check** | Auto-rebuild if file changed |
| **Streaming** | Iterate from any line forward |
| **Type hints** | Full typing support |

## Package Structure

```
jsonl-resumable/
├── pyproject.toml
├── README.md
├── PLAN.md
├── src/
│   └── jsonl_resumable/
│       ├── __init__.py
│       ├── index.py        # JsonlIndex class
│       ├── models.py       # LineInfo, IndexMeta dataclasses
│       └── persistence.py  # Save/load index
└── tests/
    ├── __init__.py
    ├── test_index.py
    ├── test_persistence.py
    └── test_large_files.py
```

## Implementation Plan

### Phase 1: Core (2-3 hours)
- [ ] `models.py` - LineInfo, IndexMeta dataclasses
- [ ] `index.py` - JsonlIndex with build, seek, iterate
- [ ] Basic tests

### Phase 2: Persistence (1-2 hours)
- [ ] `persistence.py` - Pickle save/load with freshness
- [ ] Auto-rebuild on file modification
- [ ] Tests for persistence

### Phase 3: Polish (1-2 hours)
- [ ] README with examples
- [ ] pyproject.toml for pip
- [ ] Type hints throughout
- [ ] Edge cases (empty files, single line, etc.)

## API Design

### JsonlIndex

```python
class JsonlIndex:
    def __init__(
        self,
        file_path: str | Path,
        checkpoint_interval: int = 100,
        index_path: str | Path | None = None,  # defaults to {file}.idx
    ):
        """
        Create or load an index for a JSONL file.

        Args:
            file_path: Path to the JSONL file
            checkpoint_interval: Store checkpoint every N lines (memory vs speed)
            index_path: Where to persist the index (None = alongside file)
        """

    @property
    def total_lines(self) -> int:
        """Total number of lines in the indexed file."""

    @property
    def file_size(self) -> int:
        """Size of the indexed file in bytes."""

    def get_offset(self, line_number: int) -> tuple[int, int]:
        """
        Get byte offset and length for a specific line.

        Returns:
            (offset, length) tuple

        Raises:
            IndexError: If line_number out of range
        """

    def seek_line(self, file_handle: IO, line_number: int) -> str:
        """
        Seek to and read a specific line.

        Args:
            file_handle: Open file handle (binary mode)
            line_number: 0-indexed line number

        Returns:
            The line content (with newline stripped)
        """

    def iter_from(self, start_line: int = 0) -> Iterator[str]:
        """
        Iterate lines starting from a specific line.

        Args:
            start_line: 0-indexed line to start from

        Yields:
            Lines (with newlines stripped)
        """

    def open(self) -> ContextManager[IO]:
        """Open the indexed file for reading."""

    def rebuild(self) -> None:
        """Force rebuild the index."""

    def save(self) -> None:
        """Persist index to disk."""
```

### LineInfo

```python
@dataclass
class LineInfo:
    """Information about a single line in the file."""
    line_number: int
    offset: int      # Byte offset from start
    length: int      # Length in bytes (including newline)
```

### IndexMeta

```python
@dataclass
class IndexMeta:
    """Metadata about an indexed file."""
    file_path: str
    file_size: int
    file_mtime: float
    total_lines: int
    checkpoint_interval: int
    checkpoints: dict[int, int]  # line_number -> byte_offset
    indexed_at: str
    version: str = "1.0"
```

## Differentiators

| This Package | Alternatives |
|--------------|--------------|
| Purpose-built for JSONL resume | Generic line reading |
| Checkpoint-based (memory efficient) | Store every line offset |
| Auto-persistence with freshness | Manual save/load |
| Simple API (3 methods) | Complex configuration |

## Usage Examples

### Resume after crash

```python
# Save progress periodically
last_processed = 0
for i, line in enumerate(index.iter_from(last_processed)):
    process(json.loads(line))
    if i % 1000 == 0:
        save_checkpoint(i)  # Your checkpoint logic
```

### Random access

```python
# Jump to specific event
event_42000 = json.loads(index.seek_line(f, 42000))
```

### Tail-like behavior

```python
# Process last 100 lines
for line in index.iter_from(index.total_lines - 100):
    print(line)
```

## Publishing

```bash
# Build
python -m build

# Upload to PyPI
twine upload dist/*

# Or test on TestPyPI first
twine upload --repository testpypi dist/*
```

## Name alternatives (if jsonl-resumable taken)

- `jsonl-index`
- `jsonl-seek`
- `lineseek`
- `byteindex`

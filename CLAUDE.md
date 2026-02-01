# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Architecture

A Python library for O(1) random access and resumable iteration in large JSONL files via byte-offset indexing.

### Module Structure

```
src/jsonl_resumable/
├── __init__.py      # Public API exports (JsonlIndex, IndexMeta, LineInfo)
├── index.py         # Core JsonlIndex class - seeking, reading, iteration
├── models.py        # Data classes: LineInfo (per-line metadata), IndexMeta (file metadata)
└── persistence.py   # JSON-based index serialization (save_index, load_index)
```

### Key Concepts

- **LineInfo**: Stores `(line_number, byte_offset, length)` for each line
- **IndexMeta**: File metadata (path, size, mtime) + checkpoints for freshness detection
- **Checkpoints**: Sparse map of `line_number → byte_offset` at intervals (trades memory for seek speed)
- **Freshness**: Index auto-rebuilds when file size/mtime changes; `update()` handles append-only growth

### Data Flow

1. `JsonlIndex(file)` → loads existing `.idx` or builds new index
2. Index stores byte offset of every line + periodic checkpoints
3. `read_json(n)` → `seek(offset)` → read `length` bytes → decode → parse JSON
4. `iter_from(n)` → seek to line n's offset → stream remaining lines

## Development Commands

```bash
# Setup
pip install -e ".[dev]"

# Testing
pytest                              # All tests
pytest tests/test_index.py -v       # Single file
pytest -k "test_update"             # Tests matching pattern
pytest --cov=src/jsonl_resumable    # With coverage

# Linting
ruff check src tests                # Lint
ruff check --fix                    # Auto-fix
mypy src                            # Type check
```

## Git Workflow

- `main` is protected - all changes via PR
- CI runs tests on Python 3.10, 3.11, 3.12 + linting + mypy

## Releasing

Tag-based releases with PyPI Trusted Publishing:

1. Update version in `pyproject.toml`
2. Merge to `main` via PR
3. `git tag v{version} && git push origin v{version}`
4. `publish.yml` workflow auto-publishes to PyPI

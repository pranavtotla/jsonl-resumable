# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.3.0] - 2026-02-01

### Added

- **CLI interface** with three subcommands:
  - `jsonl-index info <file>` - Show line count, file size, index status
  - `jsonl-index read <file> <line...>` - Read specific lines by number
  - `jsonl-index sample <file> <n>` - Random sample of n records
- **Module execution** via `python -m jsonl_resumable`
- **`sample()` method** for random record selection with optional seed for reproducibility
- **Resumable batch processing** with automatic checkpointing:
  - `BatchProcessor` class for processing large files with progress persistence
  - `index.batch_processor(job_id)` context manager for easy batch processing
  - Progress saved to `.progress/` directory, survives crashes/restarts
  - `JobInfo` model for tracking job metadata and progress
- **Custom exceptions** for better error handling:
  - `StaleCheckpointError` - When source file changed since checkpoint
  - `InvalidCheckpointError` - When checkpoint data is corrupted
- CLI flags: `--json` (machine-readable output), `--pretty` (indented JSON), `--seed` (reproducible sampling)

### Changed

- Zero new dependencies - CLI uses stdlib `argparse`

## [0.2.0] - 2026-02-01

### Added

- **Batch read methods** for efficient multi-line access:
  - `read_line_many(line_numbers)` - Read multiple lines with single file open
  - `read_json_many(line_numbers)` - Read and parse multiple lines as JSON
- **`keep_open` mode** - Persistent file handle for repeated reads (use with context manager)
- **`update()` method** - Incrementally index appended lines without full rebuild
- **CI/CD workflows** - Automated testing on Python 3.10, 3.11, 3.12
- **PyPI publishing** - Tag-based releases with Trusted Publishing

### Changed

- Improved README with clearer examples and API documentation

## [0.1.0] - 2026-02-01

### Added

- **`JsonlIndex` class** - Core indexing and seeking functionality
- **O(1) random access** - Instant access to any line via byte offset
- **Resumable iteration** - `iter_from(n)` to resume from any line
- **JSON parsing** - `read_json(n)` and `iter_json_from(n)` convenience methods
- **Index persistence** - Automatic save/load of `.idx` files
- **Freshness detection** - Auto-rebuild when source file changes
- **Checkpoints** - Configurable sparse checkpoints for memory/speed tradeoff
- **Data models** - `LineInfo` and `IndexMeta` for index structure

[0.3.0]: https://github.com/pranavtotla/jsonl-resumable/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/pranavtotla/jsonl-resumable/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/pranavtotla/jsonl-resumable/releases/tag/v0.1.0

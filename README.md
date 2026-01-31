# jsonl-resumable

Index JSONL files for instant random access and resumable iteration.

[![PyPI version](https://badge.fury.io/py/jsonl-resumable.svg)](https://pypi.org/project/jsonl-resumable/)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## The problem

You have a 10GB JSONL file. Your processing script crashes at line 25 million. To resume, you have to iterate through all 25 million lines you already processed just to get back to where you were:

```python
for i, line in enumerate(open("huge.jsonl")):
    if i < 25_000_000:
        continue  # this takes forever
    process(line)
```

This library builds a byte-offset index of your file so you can seek directly to any line:

```python
from jsonl_resumable import JsonlIndex

index = JsonlIndex("huge.jsonl")
for event in index.iter_json_from(25_000_000):  # instant
    process(event)
```

## Install

```bash
pip install jsonl-resumable
```

## Basic usage

```python
from jsonl_resumable import JsonlIndex

# First run builds the index (takes a while for big files)
# Subsequent runs load it from disk
index = JsonlIndex("events.jsonl")

# Jump to any line
event = index.read_json(1_000_000)

# Iterate from a specific line
for event in index.iter_json_from(last_processed):
    process(event)

# If the file grew, update the index (only scans new bytes)
index.update()
```

Useful for data pipelines, log analysis, ML training data—anywhere you're dealing with large JSONL files and don't want to start over every time something fails.

## API

```python
index = JsonlIndex("data.jsonl")

# Read a single line (parsed or raw)
index.read_json(1000)        # returns dict or list
index.read_line(1000)        # returns raw string
index[1000]                  # same as read_line

# Iterate starting from line N
index.iter_json_from(5000)   # yields parsed JSON
index.iter_from(5000)        # yields raw strings

# When the file grows
index.update()               # indexes new lines, returns count added

# Properties
index.total_lines
index.file_size
```

Constructor options:

```python
JsonlIndex(
    "data.jsonl",
    checkpoint_interval=100,  # trade memory for speed (lower = more memory)
    index_path="custom.idx",  # custom index file location
    auto_save=True,           # save index to disk after build/update
)
```

You can also call `rebuild()` to force a full re-index, or `save()` to persist manually.

## Incremental updates

If you're appending to your JSONL file over time, you don't need to rebuild the whole index:

```python
index = JsonlIndex("events.jsonl")
print(index.total_lines)  # 1000

# ... later, after appending more data ...

new_count = index.update()
print(new_count)          # 50
print(index.total_lines)  # 1050
```

`update()` picks up where the index left off and only scans the new bytes.

## How it works

The library scans your file once and records the byte offset of each line. These offsets get saved to `{filename}.idx`. When you want line N, it just does `file.seek(offset)` instead of reading through the whole file.

If the file's size or modification time changes, it detects that and rebuilds automatically.

## Examples

**Checkpointing for crash recovery:**

```python
from pathlib import Path
from jsonl_resumable import JsonlIndex

checkpoint = Path("progress.txt")
index = JsonlIndex("events.jsonl")

start = int(checkpoint.read_text()) if checkpoint.exists() else 0

for i, event in enumerate(index.iter_json_from(start), start=start):
    process(event)
    if i % 1000 == 0:
        checkpoint.write_text(str(i))
```

**Random sampling:**

```python
import random
from jsonl_resumable import JsonlIndex

index = JsonlIndex("training_data.jsonl")
sample_ids = random.sample(range(index.total_lines), k=1000)
samples = [index.read_json(i) for i in sample_ids]
```

**Tail (last N lines):**

```python
index = JsonlIndex("logs.jsonl")
for line in index.iter_from(index.total_lines - 100):
    print(line)
```

## FAQ

**How big is the index file?**

About 15 bytes per line. A 10 million line file produces roughly a 150MB index.

**What if the file gets modified (not just appended)?**

The library compares file size and mtime. If something changed, it rebuilds. You can also call `rebuild()` explicitly.

**Is it thread-safe?**

Reads are fine from multiple threads. Don't call `update()` or `rebuild()` concurrently.

**Why not linecache?**

`linecache` loads the entire file into memory. This uses byte offsets so memory usage stays constant regardless of file size.

## License

MIT

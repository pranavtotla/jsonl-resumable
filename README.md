# jsonl-resumable

**Skip millions of lines in milliseconds.**

[![PyPI version](https://badge.fury.io/py/jsonl-resumable.svg)](https://pypi.org/project/jsonl-resumable/)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

---

## Why?

You have a 10GB JSONL file. Your script crashes at line 25 million. Now what?

```python
# Without jsonl-resumable: wait 10 minutes to skip processed lines
for i, line in enumerate(open("huge.jsonl")):
    if i < 25_000_000:
        continue  # 😴
    process(line)
```

```python
# With jsonl-resumable: resume instantly
from jsonl_resumable import JsonlIndex

index = JsonlIndex("huge.jsonl")
for event in index.iter_json_from(25_000_000):  # ⚡ <1ms
    process(event)
```

---

## Install

```bash
pip install jsonl-resumable
```

---

## Quick Start

```python
from jsonl_resumable import JsonlIndex

# First run: builds index (~20s for 1GB file)
# Next runs: loads from disk instantly
index = JsonlIndex("events.jsonl")

# Jump to any line in O(1)
event = index.read_json(1_000_000)

# Resume from any point
for event in index.iter_json_from(last_processed):
    process(event)

# File grew? Update index incrementally
index.update()  # Only indexes new lines
```

That's it. Three methods cover 90% of use cases.

---

## Who is this for?

| You're building... | Example |
|-------------------|---------|
| LLM data pipelines | Processing OpenAI fine-tuning datasets |
| ETL jobs | Resumable data transformations |
| Log analyzers | Jumping to specific timestamps |
| ML training | Random sampling from large datasets |

**Common thread:** Large JSONL files where restarting from scratch is expensive.

---

## API

### Core Methods

```python
index = JsonlIndex("data.jsonl")

# Read single line
index.read_json(1000)        # → dict/list (parsed)
index.read_line(1000)        # → str (raw)
index[1000]                  # → str (shorthand)

# Iterate from line N
index.iter_json_from(5000)   # → Iterator[dict|list]
index.iter_from(5000)        # → Iterator[str]

# After appending to file
index.update()               # → int (new lines indexed)

# Metadata
index.total_lines            # → int
index.file_size              # → int (bytes)
```

### Options

```python
JsonlIndex(
    "data.jsonl",
    checkpoint_interval=100,  # Memory vs speed tradeoff
    index_path="custom.idx",  # Where to save index
    auto_save=True,           # Persist after build/update
)
```

### Maintenance

```python
index.rebuild()   # Force full re-index
index.save()      # Manual persist
```

---

## Incremental Updates

When your JSONL file grows (append-only), don't rebuild the entire index:

```python
index = JsonlIndex("events.jsonl")
print(index.total_lines)  # 1000

# ... your app appends 50 new events ...

new_count = index.update()
print(f"Indexed {new_count} new lines")  # "Indexed 50 new lines"
print(index.total_lines)  # 1050
```

`update()` seeks to where the old index ended and only processes new bytes.

---

## How It Works

1. **Build**: Scan file once, record byte offset of each line
2. **Persist**: Save offsets to `{filename}.idx` (JSON format)
3. **Seek**: Use `file.seek(offset)` to jump directly to any line
4. **Detect changes**: Compare file size + mtime, rebuild if needed

---

## Real-World Patterns

### Crash-Resilient Processing

```python
from pathlib import Path
from jsonl_resumable import JsonlIndex

checkpoint = Path("progress.txt")
index = JsonlIndex("events.jsonl")

# Resume from last checkpoint
start = int(checkpoint.read_text()) if checkpoint.exists() else 0

for i, event in enumerate(index.iter_json_from(start), start=start):
    process(event)
    if i % 1000 == 0:
        checkpoint.write_text(str(i))
```

### Random Sampling

```python
import random
from jsonl_resumable import JsonlIndex

index = JsonlIndex("training_data.jsonl")
sample_ids = random.sample(range(index.total_lines), k=1000)
samples = [index.read_json(i) for i in sample_ids]
```

### Tail (Last N Lines)

```python
index = JsonlIndex("logs.jsonl")
for line in index.iter_from(index.total_lines - 100):
    print(line)
```

### Parallel Chunk Processing

```python
from concurrent.futures import ProcessPoolExecutor
from jsonl_resumable import JsonlIndex

def process_range(args):
    path, start, end = args
    index = JsonlIndex(path)
    return [transform(e) for e in index.iter_json_from(start)
            if index._lines[start:end]]

index = JsonlIndex("huge.jsonl")
n_workers = 4
chunk = index.total_lines // n_workers

with ProcessPoolExecutor(n_workers) as ex:
    results = ex.map(process_range, [
        ("huge.jsonl", i * chunk, (i+1) * chunk)
        for i in range(n_workers)
    ])
```

---

## FAQ

**Q: What's JSONL?**
JSON Lines — each line is a valid JSON object. Used by OpenAI, Hugging Face, and most ML pipelines.

**Q: How big is the index file?**
Roughly 15 bytes per line. A 10M line file → ~150MB index.

**Q: What if the file is modified (not just appended)?**
Call `rebuild()`. Or just create a new `JsonlIndex` — it auto-detects changes via file size/mtime.

**Q: Thread-safe?**
Read operations are safe. Don't call `update()` or `rebuild()` from multiple threads.

**Q: Why not just use `linecache`?**
`linecache` loads the entire file into memory. This library uses byte offsets — constant memory regardless of file size.

---

## License

MIT

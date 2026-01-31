"""Index persistence (save/load to disk)."""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .models import IndexMeta, LineInfo

# Index file format version
FORMAT_VERSION = "1.0"


def save_index(
    index_path: Path,
    meta: "IndexMeta",
    lines: list["LineInfo"],
) -> None:
    """Save index to disk in JSON format.

    Uses JSON for security (no arbitrary code execution) and portability.

    Args:
        index_path: Where to save the index
        meta: Index metadata
        lines: List of line info objects
    """
    data = {
        "format_version": FORMAT_VERSION,
        "meta": {
            "file_path": meta.file_path,
            "file_size": meta.file_size,
            "file_mtime": meta.file_mtime,
            "total_lines": meta.total_lines,
            "checkpoint_interval": meta.checkpoint_interval,
            "checkpoints": meta.checkpoints,
            "indexed_at": meta.indexed_at,
            "version": meta.version,
        },
        # Store lines compactly as [offset, length] arrays
        # (line_number is implicit from array index)
        "lines": [[line.offset, line.length] for line in lines],
    }

    with open(index_path, "w", encoding="utf-8") as f:
        json.dump(data, f, separators=(",", ":"))  # Compact JSON


def load_index(index_path: Path) -> tuple["IndexMeta", list["LineInfo"]] | None:
    """Load index from disk.

    Args:
        index_path: Path to the index file

    Returns:
        Tuple of (IndexMeta, list[LineInfo]) or None if load fails
    """
    # Import here to avoid circular imports
    from .models import IndexMeta, LineInfo

    try:
        with open(index_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        # Version check
        if data.get("format_version") != FORMAT_VERSION:
            return None

        meta_data = data["meta"]
        meta = IndexMeta(
            file_path=meta_data["file_path"],
            file_size=meta_data["file_size"],
            file_mtime=meta_data["file_mtime"],
            total_lines=meta_data["total_lines"],
            checkpoint_interval=meta_data["checkpoint_interval"],
            checkpoints={int(k): v for k, v in meta_data["checkpoints"].items()},
            indexed_at=meta_data["indexed_at"],
            version=meta_data.get("version", "1.0"),
        )

        lines = [
            LineInfo(line_number=i, offset=line[0], length=line[1])
            for i, line in enumerate(data["lines"])
        ]

        return meta, lines

    except (json.JSONDecodeError, KeyError, TypeError, FileNotFoundError):
        return None

"""Progress persistence (save/load job progress to disk)."""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .models import JobProgress

# Progress file format version
FORMAT_VERSION = "1.0"


def save_progress(
    progress_path: Path,
    jobs: dict[str, "JobProgress"],
) -> None:
    """Save all job progress to disk in JSON format.

    Args:
        progress_path: Where to save the progress file
        jobs: Dictionary mapping job_id to JobProgress
    """
    data = {
        "format_version": FORMAT_VERSION,
        "jobs": {
            job_id: {
                "position": job.position,
                "file_size": job.file_size,
                "file_mtime": job.file_mtime,
                "status": job.status,
                "created_at": job.created_at,
                "last_checkpoint_at": job.last_checkpoint_at,
                "completed_at": job.completed_at,
            }
            for job_id, job in jobs.items()
        },
    }

    with open(progress_path, "w", encoding="utf-8") as f:
        json.dump(data, f, separators=(",", ":"))


def load_progress(progress_path: Path) -> dict[str, "JobProgress"] | None:
    """Load job progress from disk.

    Args:
        progress_path: Path to the progress file

    Returns:
        Dictionary mapping job_id to JobProgress, or None if load fails
    """
    from .models import JobProgress

    try:
        with open(progress_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        if data.get("format_version") != FORMAT_VERSION:
            return None

        jobs: dict[str, JobProgress] = {}
        for job_id, job_data in data.get("jobs", {}).items():
            jobs[job_id] = JobProgress(
                job_id=job_id,
                position=job_data["position"],
                file_size=job_data["file_size"],
                file_mtime=job_data["file_mtime"],
                status=job_data["status"],
                created_at=job_data["created_at"],
                last_checkpoint_at=job_data["last_checkpoint_at"],
                completed_at=job_data.get("completed_at"),
            )

        return jobs

    except (json.JSONDecodeError, KeyError, TypeError, FileNotFoundError):
        return None


def update_job_progress(progress_path: Path, job: "JobProgress") -> None:
    """Update a single job's progress atomically.

    Performs a read-modify-write operation to update one job while
    preserving all other jobs in the progress file.

    Args:
        progress_path: Path to the progress file
        job: The job progress to update
    """
    jobs = load_progress(progress_path) or {}
    jobs[job.job_id] = job
    save_progress(progress_path, jobs)


def delete_job_progress(progress_path: Path, job_id: str) -> bool:
    """Delete a job's progress from the file.

    Args:
        progress_path: Path to the progress file
        job_id: The job ID to delete

    Returns:
        True if the job was found and deleted, False otherwise
    """
    jobs = load_progress(progress_path)
    if jobs is None or job_id not in jobs:
        return False

    del jobs[job_id]
    save_progress(progress_path, jobs)
    return True

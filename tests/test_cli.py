"""Tests for CLI interface."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

from jsonl_resumable.cli import main


@pytest.fixture
def sample_jsonl(tmp_path: Path) -> Path:
    """Create a sample JSONL file with 50 lines."""
    file_path = tmp_path / "test.jsonl"
    with open(file_path, "w") as f:
        for i in range(50):
            f.write(json.dumps({"id": i, "name": f"item_{i}"}) + "\n")
    return file_path


@pytest.fixture
def empty_jsonl(tmp_path: Path) -> Path:
    """Create an empty JSONL file."""
    file_path = tmp_path / "empty.jsonl"
    file_path.touch()
    return file_path


def run_cli(args: list[str]) -> tuple[int, str, str]:
    """Run CLI via main() and capture output."""
    import io
    from contextlib import redirect_stderr, redirect_stdout

    stdout = io.StringIO()
    stderr = io.StringIO()

    try:
        with redirect_stdout(stdout), redirect_stderr(stderr):
            main(args)
        exit_code = 0
    except SystemExit as e:
        exit_code = e.code if isinstance(e.code, int) else 0

    return exit_code, stdout.getvalue(), stderr.getvalue()


class TestInfoCommand:
    """Tests for 'info' subcommand."""

    def test_info_basic(self, sample_jsonl: Path):
        """info shows file information."""
        exit_code, stdout, stderr = run_cli(["info", str(sample_jsonl)])

        assert exit_code == 0
        assert "Lines: 50" in stdout
        assert str(sample_jsonl.resolve()) in stdout

    def test_info_json_output(self, sample_jsonl: Path):
        """info --json outputs JSON."""
        exit_code, stdout, stderr = run_cli(["info", "--json", str(sample_jsonl)])

        assert exit_code == 0
        data = json.loads(stdout)
        assert data["lines"] == 50
        assert "size_bytes" in data

    def test_info_file_not_found(self, tmp_path: Path):
        """info returns error for missing file."""
        exit_code, stdout, stderr = run_cli(["info", str(tmp_path / "missing.jsonl")])

        assert exit_code == 1
        assert "Error" in stderr

    def test_info_empty_file(self, empty_jsonl: Path):
        """info handles empty file."""
        exit_code, stdout, stderr = run_cli(["info", str(empty_jsonl)])

        assert exit_code == 0
        assert "Lines: 0" in stdout


class TestReadCommand:
    """Tests for 'read' subcommand."""

    def test_read_single_line(self, sample_jsonl: Path):
        """read fetches a single line."""
        exit_code, stdout, stderr = run_cli(["read", str(sample_jsonl), "0"])

        assert exit_code == 0
        data = json.loads(stdout.strip())
        assert data["id"] == 0

    def test_read_multiple_lines(self, sample_jsonl: Path):
        """read fetches multiple lines."""
        exit_code, stdout, stderr = run_cli(["read", str(sample_jsonl), "0", "25", "49"])

        assert exit_code == 0
        lines = stdout.strip().split("\n")
        assert len(lines) == 3

        data = [json.loads(line) for line in lines]
        assert data[0]["id"] == 0
        assert data[1]["id"] == 25
        assert data[2]["id"] == 49

    def test_read_pretty(self, sample_jsonl: Path):
        """read --pretty outputs indented JSON."""
        exit_code, stdout, stderr = run_cli(["read", "--pretty", str(sample_jsonl), "0"])

        assert exit_code == 0
        assert "  " in stdout  # Indentation present
        data = json.loads(stdout)
        assert data["id"] == 0

    def test_read_out_of_range(self, sample_jsonl: Path):
        """read returns error for out-of-range line."""
        exit_code, stdout, stderr = run_cli(["read", str(sample_jsonl), "100"])

        assert exit_code == 1
        assert "out of range" in stderr

    def test_read_partial_error(self, sample_jsonl: Path):
        """read shows valid lines even with some errors."""
        exit_code, stdout, stderr = run_cli(["read", str(sample_jsonl), "0", "100", "1"])

        assert exit_code == 1  # Error occurred
        assert "out of range" in stderr

        # But valid lines should still be printed
        lines = [line for line in stdout.strip().split("\n") if line]
        assert len(lines) == 2

    def test_read_file_not_found(self, tmp_path: Path):
        """read returns error for missing file."""
        exit_code, stdout, stderr = run_cli(["read", str(tmp_path / "missing.jsonl"), "0"])

        assert exit_code == 1
        assert "Error" in stderr


class TestSampleCommand:
    """Tests for 'sample' subcommand."""

    def test_sample_basic(self, sample_jsonl: Path):
        """sample returns n records."""
        exit_code, stdout, stderr = run_cli(["sample", str(sample_jsonl), "5"])

        assert exit_code == 0
        lines = stdout.strip().split("\n")
        assert len(lines) == 5

        for line in lines:
            data = json.loads(line)
            assert "id" in data
            assert 0 <= data["id"] < 50

    def test_sample_with_seed(self, sample_jsonl: Path):
        """sample --seed is reproducible."""
        exit_code1, stdout1, _ = run_cli(["sample", str(sample_jsonl), "10", "--seed", "42"])
        exit_code2, stdout2, _ = run_cli(["sample", str(sample_jsonl), "10", "--seed", "42"])

        assert exit_code1 == 0
        assert exit_code2 == 0
        assert stdout1 == stdout2

    def test_sample_different_seeds(self, sample_jsonl: Path):
        """sample with different seeds produces different results."""
        _, stdout1, _ = run_cli(["sample", str(sample_jsonl), "10", "--seed", "42"])
        _, stdout2, _ = run_cli(["sample", str(sample_jsonl), "10", "--seed", "123"])

        assert stdout1 != stdout2

    def test_sample_pretty(self, sample_jsonl: Path):
        """sample --pretty outputs JSON array."""
        exit_code, stdout, stderr = run_cli(
            ["sample", "--pretty", str(sample_jsonl), "3", "--seed", "42"]
        )

        assert exit_code == 0
        data = json.loads(stdout)
        assert isinstance(data, list)
        assert len(data) == 3

    def test_sample_more_than_available(self, sample_jsonl: Path):
        """sample n > total returns all lines."""
        exit_code, stdout, stderr = run_cli(["sample", str(sample_jsonl), "100"])

        assert exit_code == 0
        lines = stdout.strip().split("\n")
        assert len(lines) == 50

    def test_sample_empty_file(self, empty_jsonl: Path):
        """sample from empty file returns nothing."""
        exit_code, stdout, stderr = run_cli(["sample", str(empty_jsonl), "10"])

        assert exit_code == 0
        assert stdout.strip() == ""

    def test_sample_empty_file_pretty(self, empty_jsonl: Path):
        """sample --pretty from empty file returns []."""
        exit_code, stdout, stderr = run_cli(["sample", "--pretty", str(empty_jsonl), "10"])

        assert exit_code == 0
        assert stdout.strip() == "[]"

    def test_sample_file_not_found(self, tmp_path: Path):
        """sample returns error for missing file."""
        exit_code, stdout, stderr = run_cli(["sample", str(tmp_path / "missing.jsonl"), "5"])

        assert exit_code == 1
        assert "Error" in stderr

    def test_sample_zero(self, sample_jsonl: Path):
        """sample 0 returns nothing."""
        exit_code, stdout, stderr = run_cli(["sample", str(sample_jsonl), "0"])

        assert exit_code == 0
        assert stdout.strip() == ""


class TestModuleExecution:
    """Tests for python -m execution."""

    def test_module_execution(self, sample_jsonl: Path):
        """Can run as python -m jsonl_resumable."""
        result = subprocess.run(
            [sys.executable, "-m", "jsonl_resumable", "info", "--json", str(sample_jsonl)],
            capture_output=True,
            text=True,
            cwd=str(sample_jsonl.parent),
        )

        assert result.returncode == 0
        data = json.loads(result.stdout)
        assert data["lines"] == 50


class TestHelp:
    """Tests for help output."""

    def test_main_help(self):
        """Main --help works."""
        exit_code, stdout, stderr = run_cli(["--help"])

        # argparse raises SystemExit(0) for --help
        assert exit_code == 0
        assert "jsonl-index" in stdout or "usage" in stdout.lower()

    def test_subcommand_help(self):
        """Subcommand --help works."""
        for cmd in ["info", "read", "sample"]:
            exit_code, stdout, stderr = run_cli([cmd, "--help"])
            assert exit_code == 0

    def test_no_args_shows_error(self):
        """Running with no args shows error."""
        exit_code, stdout, stderr = run_cli([])

        assert exit_code != 0  # argparse should fail

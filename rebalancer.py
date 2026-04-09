#!/usr/bin/env python3
"""unraid-rebalancer — Rebalance data across Unraid disk array drives."""

from __future__ import annotations

import argparse
import csv
import fcntl
import json
import os
import re
import shlex
import shutil
import signal
import sqlite3
import subprocess
import sys
import tempfile
import time as time_mod
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, time as dt_time
from pathlib import Path


# =============================================================================
# Constants
# =============================================================================

__version__ = "0.1.0"

STATE_DIR = Path.home() / ".unraid-rebalancer"
DEFAULT_MAX_USED = 80
PLAN_FILE = "plan.csv"
DRIVES_FILE = "drives.json"
TRANSFERS_LOG = "transfers.log"
CONFIG_FILE = "config.json"
YEAR_PATTERN_SHARES = ["Movies"]
LOCK_FILE = "rebalancer.lock"
REQUIRED_TOOLS = ["rsync", "lsof", "du", "df", "rm", "mkdir", "ls", "test"]
PLAN_DB_FILE = "plan.db"

BANNER = r'''
88   88 88b 88 88""Yb    db    88 8888b.
88   88 88Yb88 88__dP   dPYb   88  8I  Yb
Y8   8P 88 Y88 88"Yb   dP__Yb  88  8I  dY
`YbodP' 88  Y8 88  Yb dP""""Yb 88 8888Y"
88""Yb 888888 88""Yb    db    88        db    88b 88  dP""b8 888888 88""Yb
88__dP 88__   88__dP   dPYb   88       dPYb   88Yb88 dP   `" 88__   88__dP
88"Yb  88""   88""Yb  dP__Yb  88  .o  dP__Yb  88 Y88 Yb      88""   88"Yb
88  Yb 888888 88oodP dP""""Yb 88ood8 dP""""Yb 88  Y8  YboodP 888888 88  Yb
'''.strip()

DEFAULT_CONFIG = {
    "max_used": 80,
    "strategy": "fullest-first",
    "excludes": ["Backups", "Development", "appdata"],
    "active_hours": None,
    "min_free_space": "50G",
    "bwlimit": None,
    "copy_timeout": 86400,
    "verify_timeout": 28800,
    "lsof_timeout": 120,
    "remote": None,
}


# =============================================================================
# Config
# =============================================================================

def load_config(state_dir: Path) -> dict:
    """Load config.json. Returns DEFAULT_CONFIG values for missing keys.

    Coerces max_used to int, falling back to default on invalid values.
    """
    path = state_dir / CONFIG_FILE
    config = dict(DEFAULT_CONFIG)
    if path.exists():
        try:
            with open(path) as f:
                user = json.load(f)
            config.update(user)
        except (json.JSONDecodeError, TypeError) as e:
            print(f"Warning: invalid config.json ({e}), using defaults.")
    # Coerce max_used to int (JSON could store it as string)
    try:
        config["max_used"] = int(config["max_used"])
    except (ValueError, TypeError):
        print(f"Warning: invalid max_used in config ({config['max_used']!r}), using default.")
        config["max_used"] = DEFAULT_CONFIG["max_used"]
    return config


def save_default_config(state_dir: Path) -> Path:
    """Write DEFAULT_CONFIG to config.json. Returns the path."""
    state_dir.mkdir(parents=True, exist_ok=True)
    path = state_dir / CONFIG_FILE
    with open(path, "w") as f:
        json.dump(DEFAULT_CONFIG, f, indent=2)
    return path


_SIZE_RE = re.compile(r"^(\d+(?:\.\d+)?)\s*([KMGTP]?B?)$", re.IGNORECASE)

_SIZE_UNITS = {
    "": 1, "B": 1,
    "K": 1024, "KB": 1024,
    "M": 1024**2, "MB": 1024**2,
    "G": 1024**3, "GB": 1024**3,
    "T": 1024**4, "TB": 1024**4,
    "P": 1024**5, "PB": 1024**5,
}


def parse_size(value: str) -> int:
    """Parse a human-readable size string to bytes.

    Accepts: 100, 100B, 100K, 100KB, 1.5G, 1T, etc.
    """
    value = value.strip()
    if not value:
        return 0
    # Try plain integer first
    try:
        return int(value)
    except ValueError:
        pass
    match = _SIZE_RE.match(value)
    if not match:
        raise ValueError(f"Invalid size format: {value} (use e.g., 100G, 1T, 500M)")
    num = float(match.group(1))
    suffix = match.group(2).upper()
    if suffix not in _SIZE_UNITS:
        raise ValueError(f"Unknown size unit: {suffix}")
    return int(num * _SIZE_UNITS[suffix])


# =============================================================================
# Data Classes
# =============================================================================

@dataclass
class DiskInfo:
    path: str
    total_bytes: int
    used_bytes: int
    free_bytes: int
    used_pct: int


@dataclass
class MovableUnit:
    path: str
    share: str
    name: str
    size_bytes: int
    disk: str


@dataclass
class PlanEntry:
    path: str
    size_bytes: int
    source_disk: str
    target_disk: str
    status: str = "pending"


@dataclass(eq=False)
class TransferResult:
    """Result of a transfer_unit() call with status and optional diagnostic detail.

    Supports string comparison (result == "cleaned") for backward compatibility.
    Not hashable — use result.status as dict key if needed.
    """
    status: str
    detail: str = ""
    copy_seconds: float | None = None
    verify_seconds: float | None = None
    delete_seconds: float | None = None

    def __eq__(self, other):
        if isinstance(other, str):
            return self.status == other
        if isinstance(other, TransferResult):
            return self.status == other.status and self.detail == other.detail
        return NotImplemented

    __hash__ = None  # unhashable: custom __eq__ without consistent __hash__

    def __str__(self):
        return self.status


def _truncate_stderr(text: str | None, max_len: int = 500) -> str:
    """Truncate stderr for logging. Sanitize tabs/newlines for TSV safety."""
    if not text:
        return ""
    sanitized = text.replace("\t", " ").replace("\r", "").replace("\n", " ")
    if len(sanitized) > max_len:
        return sanitized[:max_len] + "..."
    return sanitized


# =============================================================================
# CSV I/O
# =============================================================================

PLAN_CSV_FIELDS = ["path", "size_bytes", "source_disk", "target_disk", "status"]


def write_plan_csv(entries: list[PlanEntry], path: Path) -> None:
    """Write plan entries to CSV atomically (temp file + rename)."""
    parent = path.parent
    parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=parent, suffix=".csv.tmp")
    try:
        with os.fdopen(fd, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=PLAN_CSV_FIELDS)
            writer.writeheader()
            for entry in entries:
                writer.writerow({
                    "path": entry.path,
                    "size_bytes": entry.size_bytes,
                    "source_disk": entry.source_disk,
                    "target_disk": entry.target_disk,
                    "status": entry.status,
                })
        os.replace(tmp, path)
    except Exception:
        if os.path.exists(tmp):
            os.unlink(tmp)
        raise


def read_plan_csv(path: Path) -> list[PlanEntry]:
    """Read plan entries from CSV. Returns empty list if file missing or corrupted."""
    if not path.exists():
        return []
    entries = []
    try:
        with open(path, newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                entries.append(PlanEntry(
                    path=row["path"],
                    size_bytes=int(row["size_bytes"]),
                    source_disk=row["source_disk"],
                    target_disk=row["target_disk"],
                    status=row["status"],
                ))
    except (KeyError, ValueError, csv.Error) as e:
        print(f"Warning: corrupted plan CSV ({e}). Use --force-rescan to rebuild.")
        return []
    return entries


# =============================================================================
# JSON I/O
# =============================================================================

def write_drives_json(disks: list[DiskInfo], path: Path) -> None:
    """Write disk info to JSON atomically."""
    parent = path.parent
    parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=parent, suffix=".json.tmp")
    try:
        with os.fdopen(fd, "w") as f:
            data = [
                {
                    "path": d.path,
                    "total_bytes": d.total_bytes,
                    "used_bytes": d.used_bytes,
                    "free_bytes": d.free_bytes,
                    "used_pct": d.used_pct,
                }
                for d in disks
            ]
            json.dump(data, f, indent=2)
        os.replace(tmp, path)
    except Exception:
        if os.path.exists(tmp):
            os.unlink(tmp)
        raise


def read_drives_json(path: Path) -> list[DiskInfo]:
    """Read disk info from JSON. Returns empty list if file missing or corrupted."""
    if not path.exists():
        return []
    try:
        with open(path) as f:
            data = json.load(f)
        return [
            DiskInfo(
                path=d["path"],
                total_bytes=d["total_bytes"],
                used_bytes=d["used_bytes"],
                free_bytes=d["free_bytes"],
                used_pct=d["used_pct"],
            )
            for d in data
        ]
    except (json.JSONDecodeError, KeyError, TypeError) as e:
        print(f"Warning: corrupted drives JSON ({e}). Will rescan on next run.")
        return []


# =============================================================================
# SQLite Plan Database
# =============================================================================

class PlanDB:
    """SQLite-backed plan state. Replaces CSV I/O for plan entries."""

    def __init__(self, db_path: Path):
        self.db_path = db_path
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(db_path))
        self.conn.row_factory = sqlite3.Row
        self._ensure_schema()

    def _ensure_schema(self) -> None:
        result = self.conn.execute("PRAGMA journal_mode=WAL").fetchone()
        if result and result[0] != "wal":
            print(f"Warning: WAL mode not enabled (got {result[0]})")
        self.conn.execute("PRAGMA busy_timeout=5000")
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS plan (
                path        TEXT PRIMARY KEY,
                size_bytes  INTEGER NOT NULL,
                source_disk TEXT NOT NULL,
                target_disk TEXT NOT NULL,
                status      TEXT NOT NULL DEFAULT 'pending'
            )
        """)
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_plan_status ON plan(status)"
        )
        for table in ("throughput", "copy_throughput", "verify_throughput"):
            self.conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {table} (
                    id              INTEGER PRIMARY KEY AUTOINCREMENT,
                    size_bytes      INTEGER NOT NULL,
                    elapsed_seconds REAL    NOT NULL,
                    timestamp       TEXT    NOT NULL
                )
            """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS meta (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
        """)
        self.conn.commit()

    def write_plan(self, entries: list[PlanEntry]) -> None:
        """Replace entire plan with new entries in a single transaction.

        Uses INSERT OR REPLACE to handle duplicate paths gracefully
        (last entry wins if paths collide).
        """
        with self.conn:
            self.conn.execute("DELETE FROM plan")
            self.conn.executemany(
                "INSERT OR REPLACE INTO plan (path, size_bytes, source_disk, target_disk, status) "
                "VALUES (?, ?, ?, ?, ?)",
                [(e.path, e.size_bytes, e.source_disk, e.target_disk, e.status)
                 for e in entries],
            )

    def get_all(self, status_filter: str | None = None) -> list[PlanEntry]:
        """Return all entries, optionally filtered by status."""
        if status_filter:
            rows = self.conn.execute(
                "SELECT path, size_bytes, source_disk, target_disk, status "
                "FROM plan WHERE status = ? ORDER BY rowid",
                (status_filter,),
            ).fetchall()
        else:
            rows = self.conn.execute(
                "SELECT path, size_bytes, source_disk, target_disk, status "
                "FROM plan ORDER BY rowid",
            ).fetchall()
        return [PlanEntry(r["path"], r["size_bytes"], r["source_disk"],
                          r["target_disk"], r["status"]) for r in rows]

    def get_pending(self) -> list[PlanEntry]:
        """Return entries with status='pending'."""
        return self.get_all(status_filter="pending")

    def update_status(self, path: str, new_status: str) -> bool:
        """Update a single entry's status by path (O(1) via PRIMARY KEY).

        Returns True if the entry was found and updated, False otherwise.
        """
        with self.conn:
            cursor = self.conn.execute(
                "UPDATE plan SET status = ? WHERE path = ?",
                (new_status, path),
            )
            return cursor.rowcount > 0

    def retry_errors(self) -> int:
        """Reset all error and skipped entries to pending. Returns count reset."""
        with self.conn:
            cursor = self.conn.execute(
                "UPDATE plan SET status = 'pending' "
                "WHERE status LIKE 'error%' OR status LIKE 'skipped%'"
            )
            return cursor.rowcount

    def recover_in_progress(self) -> int:
        """Reset all in_progress entries to pending. Returns count recovered."""
        with self.conn:
            cursor = self.conn.execute(
                "UPDATE plan SET status = 'pending' WHERE status = 'in_progress'"
            )
            return cursor.rowcount

    def summary(self) -> dict[str, int]:
        """Return {status: count} dict."""
        rows = self.conn.execute(
            "SELECT status, COUNT(*) as cnt FROM plan GROUP BY status"
        ).fetchall()
        return {r["status"]: r["cnt"] for r in rows}

    def total_bytes(self) -> int:
        """Sum of all entry sizes."""
        row = self.conn.execute(
            "SELECT COALESCE(SUM(size_bytes), 0) FROM plan"
        ).fetchone()
        return row[0]

    def remaining_bytes(self) -> int:
        """Sum of pending and in_progress entry sizes (all unfinished work)."""
        row = self.conn.execute(
            "SELECT COALESCE(SUM(size_bytes), 0) FROM plan "
            "WHERE status IN ('pending', 'in_progress')"
        ).fetchone()
        return row[0]

    def set_meta(self, key: str, value: str) -> None:
        """Set a metadata key-value pair (upsert)."""
        with self.conn:
            self.conn.execute(
                "INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)",
                (key, value),
            )

    def get_meta(self, key: str) -> str | None:
        """Get a metadata value by key. Returns None if not found."""
        row = self.conn.execute(
            "SELECT value FROM meta WHERE key = ?", (key,)
        ).fetchone()
        return row[0] if row else None

    def delete_meta(self, key: str) -> None:
        """Delete a metadata key."""
        with self.conn:
            self.conn.execute("DELETE FROM meta WHERE key = ?", (key,))

    # --- Throughput tracking (private helpers + public per-table methods) ---

    def _record_to_table(self, table: str, size_bytes: int, elapsed_seconds: float) -> None:
        """Record a throughput sample to the named table. Keeps 20 most recent (FIFO)."""
        if elapsed_seconds <= 0:
            return
        with self.conn:
            self.conn.execute(
                f"INSERT INTO {table} (size_bytes, elapsed_seconds, timestamp) "
                "VALUES (?, ?, ?)",
                (size_bytes, elapsed_seconds, datetime.now().isoformat()),
            )
            self.conn.execute(
                f"DELETE FROM {table} WHERE id NOT IN "
                f"(SELECT id FROM {table} ORDER BY id DESC LIMIT 20)"
            )

    def _avg_from_table(self, table: str) -> float | None:
        """Return size-weighted average throughput (bytes/sec) from the named table."""
        row = self.conn.execute(
            f"SELECT SUM(size_bytes), SUM(elapsed_seconds) FROM {table}"
        ).fetchone()
        total_bytes, total_seconds = row[0], row[1]
        if not total_bytes or not total_seconds:
            return None
        return total_bytes / total_seconds

    def _last_from_table(self, table: str) -> float | None:
        """Return throughput (bytes/sec) of the most recent sample from the named table."""
        row = self.conn.execute(
            f"SELECT size_bytes, elapsed_seconds FROM {table} ORDER BY id DESC LIMIT 1"
        ).fetchone()
        if not row or row["elapsed_seconds"] <= 0:
            return None
        return row["size_bytes"] / row["elapsed_seconds"]

    # Total throughput (backward compat)
    def record_throughput(self, size_bytes: int, elapsed_seconds: float) -> None:
        self._record_to_table("throughput", size_bytes, elapsed_seconds)

    def avg_throughput(self) -> float | None:
        return self._avg_from_table("throughput")

    def last_throughput(self) -> float | None:
        return self._last_from_table("throughput")

    def throughput_sample_count(self) -> int:
        row = self.conn.execute("SELECT COUNT(*) FROM throughput").fetchone()
        return row[0]

    # Copy phase throughput
    def record_copy_throughput(self, size_bytes: int, elapsed_seconds: float) -> None:
        self._record_to_table("copy_throughput", size_bytes, elapsed_seconds)

    def avg_copy_throughput(self) -> float | None:
        return self._avg_from_table("copy_throughput")

    def last_copy_throughput(self) -> float | None:
        return self._last_from_table("copy_throughput")

    # Verify phase throughput
    def record_verify_throughput(self, size_bytes: int, elapsed_seconds: float) -> None:
        self._record_to_table("verify_throughput", size_bytes, elapsed_seconds)

    def avg_verify_throughput(self) -> float | None:
        return self._avg_from_table("verify_throughput")

    def has_plan(self) -> bool:
        """Return True if the plan table has any entries."""
        row = self.conn.execute("SELECT COUNT(*) FROM plan").fetchone()
        return row[0] > 0

    def checkpoint(self) -> None:
        """Run WAL checkpoint to reclaim space during long runs."""
        self.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")

    def close(self) -> None:
        self.conn.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


def _migrate_csv_to_db(state_dir: Path) -> None:
    """One-time migration from plan.csv to plan.db."""
    csv_path = state_dir / PLAN_FILE
    db_path = state_dir / PLAN_DB_FILE
    if not csv_path.exists() or (db_path.exists() and db_path.stat().st_size > 0):
        return
    entries = read_plan_csv(csv_path)
    try:
        with PlanDB(db_path) as db:
            if entries:
                db.write_plan(entries)
    except Exception as e:
        print(f"Warning: migration failed ({e}). Will retry next run.")
        if db_path.exists():
            db_path.unlink()
        return
    csv_path.rename(csv_path.with_suffix(".csv.bak"))
    print(f"Migrated plan.csv to plan.db (backup: plan.csv.bak)")


# =============================================================================
# Command Execution
# =============================================================================

def run_cmd(
    cmd: list[str],
    *,
    remote: str | None = None,
    timeout: int = 300,
    passthrough: bool = False,
) -> subprocess.CompletedProcess:
    """Run a command locally or via SSH. Single gateway for all subprocess calls.

    Uses Popen with explicit kill on timeout to prevent orphaned child processes
    (e.g., a timed-out rsync continuing to consume bandwidth and disk I/O).

    When passthrough=True, stdout inherits the parent terminal (for live
    progress output) while stderr is captured for error diagnostics.
    """
    if remote:
        # Quote each argument to prevent shell injection on the remote side
        safe_cmd = " ".join(shlex.quote(c) for c in cmd)
        cmd = [
            "ssh",
            "-o", "ConnectTimeout=10",
            "-o", "BatchMode=yes",
            "-o", "ServerAliveInterval=30",
            "-o", "ServerAliveCountMax=3",
            remote, safe_cmd,
        ]
    if passthrough:
        proc = subprocess.Popen(cmd, stderr=subprocess.PIPE, text=True)
    else:
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        stdout, stderr = proc.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()
        raise
    return subprocess.CompletedProcess(cmd, proc.returncode, stdout or "", stderr or "")


def validate_remote_connection(remote: str) -> bool:
    """Test SSH connectivity to remote host."""
    try:
        result = run_cmd(["echo", "ok"], remote=remote, timeout=15)
        return result.returncode == 0
    except Exception:
        return False


def _check_required_tools(remote: str | None = None) -> list[str]:
    """Check that all required external tools are available. Returns missing tool names."""
    missing = []
    if remote:
        # Batch check: single SSH call instead of N round-trips
        tool_list = " ".join(REQUIRED_TOOLS)
        try:
            result = run_cmd(
                ["sh", "-c",
                 f'for t in {tool_list}; do command -v "$t" >/dev/null 2>&1 || echo "MISSING:$t"; done'],
                remote=remote, timeout=30,
            )
            for line in result.stdout.strip().splitlines():
                line = line.strip()
                if line.startswith("MISSING:"):
                    missing.append(line[8:])
            return missing
        except Exception:
            # Fallback: individual checks if batch fails
            for tool in REQUIRED_TOOLS:
                result = run_cmd(["command", "-v", tool], remote=remote, timeout=10)
                if result.returncode != 0:
                    missing.append(tool)
    else:
        for tool in REQUIRED_TOOLS:
            if shutil.which(tool) is None:
                missing.append(tool)
    return missing


# =============================================================================
# Disk Discovery
# =============================================================================

_DISK_PATH_RE = re.compile(r"^/mnt/disk\d+$")
_YEAR_RE = re.compile(r"^(19|20)\d{2}$")


def is_year_folder(name: str) -> bool:
    """Check if a directory name is a valid movie year folder (1900-2099)."""
    return bool(_YEAR_RE.match(name))


def _disk_sort_key(disk: DiskInfo) -> tuple[str, int]:
    """Sort key for disk paths: /mnt/disk2 before /mnt/disk10."""
    match = re.search(r"(\d+)$", disk.path)
    num = int(match.group(1)) if match else 0
    return (disk.path.rstrip("0123456789"), num)


def parse_df_output(output: str) -> list[DiskInfo]:
    """Parse df output into DiskInfo list, filtering to /mnt/disk[N] only.

    Expects df output with 1K-blocks (default df format). Values are
    converted to bytes by multiplying by 1024.
    """
    disks = []
    for line in output.strip().splitlines()[1:]:  # skip header
        parts = line.split()
        if len(parts) < 6:
            continue
        mount = parts[5]
        if not _DISK_PATH_RE.match(mount):
            continue
        try:
            total_kb = int(parts[1])
            used_kb = int(parts[2])
            avail_kb = int(parts[3])
            used_pct = int(parts[4].rstrip("%"))
        except ValueError:
            continue  # skip malformed lines (e.g., unmounted disks)
        disks.append(DiskInfo(
            path=mount,
            total_bytes=total_kb * 1024,
            used_bytes=used_kb * 1024,
            free_bytes=avail_kb * 1024,
            used_pct=used_pct,
        ))
    disks.sort(key=_disk_sort_key)
    return disks


def parse_ls_output(output: str) -> list[str]:
    """Parse ls -1 output into list of names, stripping whitespace."""
    return [name.strip() for name in output.strip().splitlines() if name.strip()]


def parse_du_output(output: str) -> int:
    """Parse du -sb output, returning size in bytes. Returns 0 on error."""
    output = output.strip()
    if not output:
        return 0
    try:
        return int(output.split("\t")[0])
    except (ValueError, IndexError):
        return 0


def discover_disks(remote: str | None = None) -> list[DiskInfo]:
    """Discover all array disks by running df (default 1K-blocks output).

    When running locally, uses glob to expand /mnt/disk* since subprocess
    doesn't do shell expansion. Remote mode uses SSH which expands globs.
    """
    if remote:
        result = run_cmd(["df", "-Pk", "/mnt/disk*"], remote=remote)
    else:
        import glob as glob_mod
        disk_paths = sorted(glob_mod.glob("/mnt/disk*"))
        if not disk_paths:
            return []
        result = run_cmd(["df", "-Pk"] + disk_paths)
    return parse_df_output(result.stdout)


def parse_du_batch_output(output: str) -> dict[str, int]:
    """Parse multi-line du -sb output into {path: size_bytes} dict."""
    result = {}
    for line in output.strip().splitlines():
        if not line or "\t" not in line:
            continue
        try:
            size_str, path = line.split("\t", 1)
            result[path] = int(size_str)
        except (ValueError, IndexError):
            continue
    return result


def scan_movable_units(
    disk: DiskInfo,
    excludes: list[str],
    remote: str | None = None,
) -> list[MovableUnit]:
    """Scan a disk for movable units (folders that can be relocated)."""
    # List shares on this disk
    result = run_cmd(["ls", "-1", f"{disk.path}/"], remote=remote)
    if result.returncode != 0:
        print(f"  Warning: failed to list {disk.path}/ (exit {result.returncode})")
        return []
    shares = parse_ls_output(result.stdout)

    units = []
    for share in shares:
        if share in excludes:
            continue
        share_path = f"{disk.path}/{share}"
        result = run_cmd(["ls", "-1", f"{share_path}/"], remote=remote)
        if result.returncode != 0:
            continue
        children = parse_ls_output(result.stdout)

        # Filter children for year-pattern shares
        valid_children = []
        for child in children:
            if share in YEAR_PATTERN_SHARES and not is_year_folder(child):
                continue
            valid_children.append(child)

        if not valid_children:
            continue

        # Batch du: get sizes in chunks to avoid ARG_MAX overflow
        child_paths = [f"{share_path}/{child}" for child in valid_children]
        sizes: dict[str, int] = {}
        DU_BATCH_SIZE = 500
        for i in range(0, len(child_paths), DU_BATCH_SIZE):
            batch = child_paths[i:i + DU_BATCH_SIZE]
            du_result = run_cmd(["du", "-sb"] + batch, remote=remote, timeout=600)
            sizes.update(parse_du_batch_output(du_result.stdout))

        for child in valid_children:
            child_path = f"{share_path}/{child}"
            size = sizes.get(child_path, 0)
            if size <= 0:
                continue  # skip zero-byte or errored paths
            units.append(MovableUnit(
                path=child_path,
                share=share,
                name=child,
                size_bytes=size,
                disk=disk.path,
            ))

    return units


# =============================================================================
# Plan Generation
# =============================================================================

def classify_disks(
    disks: list[DiskInfo], max_used: int
) -> tuple[list[DiskInfo], list[DiskInfo]]:
    """Split disks into overloaded (> max_used%) and underloaded (<= max_used%).

    Overloaded sorted by used_pct descending (fullest first).
    Underloaded sorted by used_pct ascending (emptiest first).
    """
    over = sorted(
        [d for d in disks if d.used_pct > max_used],
        key=lambda d: d.used_pct,
        reverse=True,
    )
    under = sorted(
        [d for d in disks if d.used_pct <= max_used],
        key=lambda d: d.used_pct,
    )
    return over, under


def _find_best_target(
    unit_size: int,
    source_disk: str,
    targets: list[DiskInfo],
    projected_usage: dict[str, int],
    max_used: int,
    min_free: int,
    allow_fallback: bool = False,
) -> DiskInfo | None:
    """Find the best target disk for a unit, respecting constraints.

    First tries to find a target that stays within max_used. If allow_fallback
    is True and no target is under threshold (all disks overloaded), falls back
    to the target with lowest projected usage that still improves balance.
    """
    # Build a lookup for total_bytes by disk path
    total_by_path = {t.path: t.total_bytes for t in targets}

    best = None
    best_proj_pct = 101  # sentinel

    for t in targets:
        if t.path == source_disk:
            continue
        if t.total_bytes <= 0:
            continue  # skip zero-capacity disks (unmounted, corrupted df)
        proj_used = projected_usage[t.path] + unit_size
        proj_free = t.total_bytes - proj_used
        # Ceiling division to prevent over-filling (80.1% → 81, not 80)
        proj_pct = -(-proj_used * 100 // t.total_bytes)
        if proj_pct > max_used:
            continue
        if proj_free < min_free:
            continue
        if proj_pct < best_proj_pct:
            best = t
            best_proj_pct = proj_pct

    if best is not None or not allow_fallback:
        return best

    # Fallback: all targets over threshold. Pick the one with lowest
    # projected usage that would still be lower-or-equal to source after move.
    source_total = total_by_path.get(source_disk)
    if source_total is None or source_total <= 0:
        return None  # source disk not in targets or zero-capacity

    fallback = None
    fallback_proj_pct = 101
    for t in targets:
        if t.path == source_disk:
            continue
        if t.total_bytes <= 0:
            continue  # skip zero-capacity disks
        proj_used = projected_usage[t.path] + unit_size
        proj_free = t.total_bytes - proj_used
        proj_pct = -(-proj_used * 100 // t.total_bytes)
        if proj_free < min_free:
            continue
        source_proj_pct = -(-((projected_usage[source_disk] - unit_size) * 100) // source_total)
        if proj_pct > source_proj_pct:
            continue
        if proj_pct < fallback_proj_pct:
            fallback = t
            fallback_proj_pct = proj_pct

    return fallback


def generate_plan(
    units: list[MovableUnit],
    overloaded: list[DiskInfo],
    underloaded: list[DiskInfo],
    strategy: str,
    max_used: int,
    min_free: int,
) -> list[PlanEntry]:
    """Generate a transfer plan to rebalance disks.

    Strategies:
    - fullest-first: Process fullest overloaded disk first, move largest units
    - largest-first: Move largest units first across all overloaded disks
    - smallest-first: Move smallest units first across all overloaded disks
    """
    if not units:
        return []

    overloaded_paths = {d.path for d in overloaded}

    # Only consider units on overloaded disks
    movable = [u for u in units if u.disk in overloaded_paths]
    if not movable:
        return []

    # Build target list: underloaded first, then overloaded as fallback
    # (allows moving from 95% to 85% even if both are above threshold)
    all_targets = list(underloaded) + list(overloaded)

    # Track projected usage per disk
    projected_usage: dict[str, int] = {}
    for d in overloaded + underloaded:
        projected_usage[d.path] = d.used_bytes

    # Sort units according to strategy
    if strategy == "fullest-first":
        # Group by source disk (fullest first), then largest units first within each disk
        disk_order = {d.path: i for i, d in enumerate(overloaded)}
        movable.sort(key=lambda u: (disk_order.get(u.disk, 999), -u.size_bytes))
    elif strategy == "largest-first":
        movable.sort(key=lambda u: -u.size_bytes)
    elif strategy == "smallest-first":
        movable.sort(key=lambda u: u.size_bytes)

    plan: list[PlanEntry] = []
    assigned_paths: set[str] = set()

    # Build total_bytes lookup for source threshold check
    disk_total = {d.path: d.total_bytes for d in overloaded + underloaded}

    for unit in movable:
        if unit.path in assigned_paths:
            continue

        # Skip if source disk has already been drained below threshold
        source_total = disk_total.get(unit.disk, 0)
        if source_total > 0:
            source_proj_pct = -(-projected_usage[unit.disk] * 100 // source_total)
            if source_proj_pct <= max_used:
                continue

        target = _find_best_target(
            unit.size_bytes, unit.disk, all_targets, projected_usage,
            max_used, min_free, allow_fallback=not underloaded,
        )
        if target is None:
            continue

        plan.append(PlanEntry(
            path=unit.path,
            size_bytes=unit.size_bytes,
            source_disk=unit.disk,
            target_disk=target.path,
        ))
        assigned_paths.add(unit.path)
        projected_usage[target.path] += unit.size_bytes
        projected_usage[unit.disk] -= unit.size_bytes

    return plan


# =============================================================================
# Execution Engine
# =============================================================================

def check_in_use(path: str, remote: str | None = None, timeout: int = 120) -> bool:
    """Check if any files in path are currently open (via lsof).

    Uses -n to skip DNS lookups (faster). The -b flag is intentionally
    omitted — it causes stat() failures on Unraid's lsof 4.99.5,
    producing empty results and silently defeating the safety check.
    Default timeout is 120s to handle large directories with +D (recursive).
    Returns True (assume in use) on any error for safety.
    """
    try:
        result = run_cmd(["lsof", "-n", "+D", path], remote=remote, timeout=timeout)
        if result.returncode == 0 and result.stdout.strip():
            return True   # files are open
        if result.returncode != 0 and result.stderr.strip():
            return True   # lsof errored — safe default
        return False      # lsof ran clean, found nothing
    except subprocess.TimeoutExpired:
        print(f"  WARNING: lsof timed out after {timeout}s checking {path} — "
              "skipping as in-use for safety. Use --lsof-timeout to increase.")
        return True
    except Exception:
        return True  # safer default: assume in use on error


_SAFE_PATH_RE = re.compile(r"^/mnt/disk\d+/.+/.+")
_DIR_TS_ONLY = re.compile(r"\.d\.\.t\.{6}")


def _validate_safe_path(path: str) -> bool:
    """Validate a path is safely under /mnt/disk[N]/share/item with no traversal.

    Requires at minimum: /mnt/diskN/share/something (3+ components after /mnt/).
    Also rejects symlinks — a symlink at /mnt/disk1/share/item could point
    anywhere, and rm -rf would follow it to the real target.
    """
    normalized = os.path.normpath(path)
    if ".." in normalized:
        return False
    return bool(_SAFE_PATH_RE.match(normalized))


def _check_not_symlink(path: str, remote: str | None = None) -> bool:
    """Return True if path is safe (not a symlink). Returns False if it IS a symlink.

    On error, returns False (block delete) — this is the last safety gate
    before rm -rf. Data is already on both disks at this point, so blocking
    the delete is safe and retryable.
    """
    try:
        result = run_cmd(["test", "-L", path], remote=remote, timeout=10)
        # test -L returns 0 if path IS a symlink
        return result.returncode != 0
    except Exception:
        return False  # fail closed — last gate before rm -rf


def _build_target_path(entry: PlanEntry) -> str:
    """Build the target path by replacing source disk with target disk."""
    if not entry.path.startswith(entry.source_disk + "/"):
        raise ValueError(
            f"Path {entry.path} does not start with source disk {entry.source_disk}"
        )
    relative = entry.path[len(entry.source_disk):]  # e.g., /TV_Shows/Show
    return entry.target_disk + relative


def _check_path_exists(path: str, remote: str | None = None) -> bool:
    """Check if a path exists on the filesystem."""
    result = run_cmd(["test", "-e", path], remote=remote, timeout=10)
    return result.returncode == 0


def transfer_unit(
    entry: PlanEntry,
    remote: str | None = None,
    min_free: int = 0,
    bwlimit: str | None = None,
    copy_timeout: int = 86400,
    verify_timeout: int = 28800,
    progress: bool = False,
    lsof_timeout: int = 120,
    phase_status: bool = False,
) -> TransferResult:
    """Execute three-phase transfer: copy -> verify -> delete source.

    Returns TransferResult with status and optional stderr detail:
    - 'cleaned': success
    - 'skipped_full': target disk has insufficient free space
    - 'error_path': invalid/unsafe path, symlink, or mkdir failed
    - 'error_copy': rsync copy failed (detail has stderr)
    - 'error_verify': checksum mismatch (detail has stderr; source NOT deleted)
    - 'error_delete': rm -rf failed (detail has stderr; data on both disks)
    - 'skipped_in_use': files in use before delete (data on both disks, safe to retry)
    - 'error_timeout': command timed out
    """
    # Safety: validate paths with canonicalization to prevent traversal
    if not _validate_safe_path(entry.path):
        return TransferResult("error_path")

    try:
        target_path = _build_target_path(entry)
    except ValueError:
        return TransferResult("error_path")

    if not _validate_safe_path(target_path):
        return TransferResult("error_path")

    # Verify source still exists (plan could be stale)
    try:
        if not _check_path_exists(entry.path, remote=remote):
            return TransferResult("error_path")
    except subprocess.TimeoutExpired:
        return TransferResult("error_timeout")

    target_parent = os.path.dirname(target_path)

    # Pre-transfer disk space recheck — plan may be stale
    try:
        df_result = run_cmd(["df", "-Pk", entry.target_disk], remote=remote, timeout=30)
        if df_result.returncode == 0:
            target_disks = parse_df_output(df_result.stdout)
            matched = next((d for d in target_disks if d.path == entry.target_disk), None)
            if matched:
                if matched.free_bytes < entry.size_bytes + min_free:
                    return TransferResult("skipped_full")
            # If df succeeded but disk not in output, proceed (disk may have different name)
        # If df returns non-zero, proceed — rsync will fail cleanly if disk is gone
    except Exception as e:
        # Cannot verify target disk space — fail safe rather than risk ENOSPC
        return TransferResult("skipped_full", _truncate_stderr(str(e)))

    try:
        # Check if target already exists
        target_exists = False
        try:
            target_exists = _check_path_exists(target_path, remote=remote)
        except subprocess.TimeoutExpired:
            return TransferResult("error_timeout")

        if target_exists:
            # Target exists — could be a partial copy from a previous interrupted run.
            # Run rsync to complete/update the copy (idempotent), then verify as normal.
            pass  # fall through to rsync which will sync remaining differences

        # Ensure target parent directory exists
        mk_result = run_cmd(["mkdir", "-p", target_parent], remote=remote)
        if mk_result.returncode != 0:
            return TransferResult("error_path")

        # Phase 1: rsync copy (idempotent — safe to re-run on partial target)
        if phase_status:
            print(f"    {_now_hms()} Copying...")
        t_copy = time_mod.monotonic()
        rsync_cmd = ["rsync", "-aHP"]
        if bwlimit:
            rsync_cmd.append(f"--bwlimit={bwlimit}")
        if progress:
            rsync_cmd.append("--info=progress2")
        rsync_cmd.extend([f"{entry.path}/", f"{target_path}/"])
        copy_result = run_cmd(
            rsync_cmd,
            remote=remote,
            timeout=copy_timeout,
            passthrough=progress,
        )
        copy_secs = time_mod.monotonic() - t_copy
        if copy_result.returncode != 0:
            return TransferResult("error_copy", _truncate_stderr(copy_result.stderr),
                                  copy_seconds=copy_secs)

        # Phase 2: checksum verification using --itemize-changes for reliable parsing
        if phase_status:
            print(f"    {_now_hms()} Verifying...")
        t_verify = time_mod.monotonic()
        verify = run_cmd(
            ["rsync", "-anc", "--itemize-changes", f"{entry.path}/", f"{target_path}/"],
            remote=remote,
            timeout=verify_timeout,
        )
        verify_secs = time_mod.monotonic() - t_verify
        # With --itemize-changes, changed files produce lines like ">f..t......"
        # Filter out directory timestamp-only changes (.d..t......), which are
        # normal after copy. Do NOT filter other directory diffs (permissions,
        # owner, group) as those indicate real problems.
        verify_lines = [
            line for line in verify.stdout.strip().splitlines()
            if line.strip() and not _DIR_TS_ONLY.match(line)
        ]
        if verify.returncode != 0 or verify_lines:
            return TransferResult("error_verify", _truncate_stderr(verify.stderr),
                                  copy_seconds=copy_secs, verify_seconds=verify_secs)

        # Phase 3: final safety check — re-verify files aren't in use before delete
        if check_in_use(entry.path, remote=remote, timeout=lsof_timeout):
            return TransferResult("skipped_in_use",
                                  copy_seconds=copy_secs, verify_seconds=verify_secs)

        # Phase 3b: symlink safety — reject if source is a symlink to prevent
        # rm -rf from following it to an unrelated location
        if not _check_not_symlink(entry.path, remote=remote):
            return TransferResult("error_path",
                                  copy_seconds=copy_secs, verify_seconds=verify_secs)

        # Phase 4: delete source (verify it succeeds)
        if phase_status:
            print(f"    {_now_hms()} Deleting source...")
        t_delete = time_mod.monotonic()
        rm_result = run_cmd(["rm", "-rf", entry.path], remote=remote)
        delete_secs = time_mod.monotonic() - t_delete
        if rm_result.returncode != 0:
            return TransferResult("error_delete", _truncate_stderr(rm_result.stderr),
                                  copy_seconds=copy_secs, verify_seconds=verify_secs,
                                  delete_seconds=delete_secs)
        return TransferResult("cleaned",
                              copy_seconds=copy_secs, verify_seconds=verify_secs,
                              delete_seconds=delete_secs)

    except subprocess.TimeoutExpired:
        return TransferResult("error_timeout")


def log_transfer(log_path, entry: PlanEntry, status: str, detail: str = "") -> None:
    """Append a transfer record to the log file. Non-fatal on failure.

    Writes 6 TSV columns by default. When detail is non-empty, appends a
    7th column (backward compatible with existing log parsers).
    """
    try:
        timestamp = datetime.now().isoformat()
        line = f"{timestamp}\t{status}\t{entry.size_bytes}\t{entry.source_disk}\t{entry.target_disk}\t{entry.path}"
        if detail:
            line += f"\t{detail}"
        line += "\n"
        with open(log_path, "a") as f:
            f.write(line)
    except OSError as e:
        print(f"Warning: could not write to log ({e})")


# =============================================================================
# Signal Handling
# =============================================================================

_shutdown_requested = False
_last_signal_time = 0.0


def shutdown_requested() -> bool:
    return _shutdown_requested


def reset_shutdown_flags() -> None:
    """Reset shutdown flags (for testing)."""
    global _shutdown_requested, _last_signal_time
    _shutdown_requested = False
    _last_signal_time = 0.0


def _signal_handler(signum, frame):
    global _shutdown_requested, _last_signal_time
    now = time_mod.time()
    if _shutdown_requested and (now - _last_signal_time) < 3.0:
        sys.exit(1)
    _shutdown_requested = True
    _last_signal_time = now


def setup_signal_handlers() -> None:
    """Install SIGINT and SIGTERM handlers for graceful/hard shutdown."""
    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)


# =============================================================================
# Active Hours
# =============================================================================

_TIME_RANGE_RE = re.compile(r"^(\d{2}):(\d{2})-(\d{2}):(\d{2})$")


def parse_time_range(spec: str) -> tuple[dt_time, dt_time]:
    """Parse 'HH:MM-HH:MM' into (start, end) time objects."""
    match = _TIME_RANGE_RE.match(spec)
    if not match:
        raise ValueError(f"Invalid time range format (expected HH:MM-HH:MM): {spec}")
    try:
        start = dt_time(int(match.group(1)), int(match.group(2)))
        end = dt_time(int(match.group(3)), int(match.group(4)))
    except ValueError as e:
        raise ValueError(f"Invalid time in range: {spec}") from e
    if start == end:
        raise ValueError(f"Start and end times must differ: {spec}")
    return start, end


def is_within_active_hours(spec: str | None) -> bool:
    """Check if current time is within the active hours window."""
    if spec is None:
        return True
    start, end = parse_time_range(spec)
    now = datetime.now().time()
    if start <= end:
        # Same-day range: 09:00-17:00
        return start <= now < end
    else:
        # Overnight range: 22:00-06:00
        return now >= start or now < end


# =============================================================================
# Terminal Display
# =============================================================================

class ANSI:
    """ANSI escape code constants. Disabled when NO_COLOR is set."""
    @staticmethod
    def _enabled() -> bool:
        return "NO_COLOR" not in os.environ

    @classmethod
    def red(cls, text: str) -> str:
        return f"\033[31m{text}\033[0m" if cls._enabled() else text

    @classmethod
    def green(cls, text: str) -> str:
        return f"\033[32m{text}\033[0m" if cls._enabled() else text

    @classmethod
    def yellow(cls, text: str) -> str:
        return f"\033[33m{text}\033[0m" if cls._enabled() else text

    @classmethod
    def bold(cls, text: str) -> str:
        return f"\033[1m{text}\033[0m" if cls._enabled() else text


def format_bytes(n: int) -> str:
    """Format byte count to human-readable string."""
    if n == 0:
        return "0 B"
    value = float(n)
    for unit in ("B", "KB", "MB", "GB", "TB", "PB"):
        if abs(value) < 1024:
            if unit == "B":
                return f"{int(value)} {unit}"
            return f"{value:.1f} {unit}"
        value /= 1024
    return f"{value:.1f} EB"


def format_eta(seconds: float) -> str:
    """Format seconds into a human-readable ETA string."""
    if seconds < 60:
        return "<1m"
    if seconds < 3600:
        return f"~{int(seconds // 60)}m"
    if seconds < 86400:
        h, remainder = divmod(int(seconds), 3600)
        m = remainder // 60
        return f"~{h}h {m}m"
    d, remainder = divmod(int(seconds), 86400)
    h = remainder // 3600
    return f"~{d}d {h}h"


def _now_hms() -> str:
    """Return current time as [HH:MM:SS] prefix."""
    return datetime.now().strftime("[%H:%M:%S]")


def format_disk_table(disks: list[DiskInfo], max_used: int = DEFAULT_MAX_USED) -> str:
    """Format disk usage as a colored table."""
    lines = [ANSI.bold("Disk Summary:"), ""]
    header = f"{'Disk':<16} {'Total':>8} {'Used':>8} {'Free':>8} {'Use%':>7}"
    lines.append(ANSI.bold(header))
    lines.append("-" * 52)
    for d in disks:
        name = d.path.split("/")[-1]
        # Pad the raw string before wrapping in ANSI color to avoid
        # escape codes inflating the visible width calculation
        pct_padded = f"{d.used_pct}%".rjust(5)
        if d.used_pct > max_used:
            pct_display = ANSI.red(pct_padded)
        elif d.used_pct > max_used - 10:
            pct_display = ANSI.yellow(pct_padded)
        else:
            pct_display = ANSI.green(pct_padded)
        lines.append(
            f"{name:<16} {format_bytes(d.total_bytes):>8} "
            f"{format_bytes(d.used_bytes):>8} {format_bytes(d.free_bytes):>8} "
            f"  {pct_display}"
        )
    return "\n".join(lines)


def _title_case_status(status: str) -> str:
    """Convert snake_case status to Title Case label."""
    return status.replace("_", " ").title()


def _format_status_breakdown(
    counts: dict[str, int],
    total_entries: int,
    active_suffix: str | None = None,
) -> list[str]:
    """Format status breakdown lines with percentages."""
    if total_entries == 0:
        return []
    always_show = {"pending", "in_progress", "cleaned"}
    status_order = (
        "pending",
        "in_progress",
        "cleaned",
        "skipped",
        "skipped_full",
        "skipped_in_use",
        "error_path",
        "error_copy",
        "error_verify",
        "error_delete",
        "error_timeout",
    )
    lines = []
    for status in status_order:
        count = counts.get(status, 0)
        if count == 0 and status not in always_show:
            continue
        label = _title_case_status(status)
        if count > 0:
            pct = count / total_entries * 100
            suffix = ""
            if status == "in_progress" and active_suffix:
                suffix = f"  [{active_suffix}]"
            lines.append(f"  {label:<17} {count:>5}  ({pct:5.1f}%){suffix}")
        else:
            lines.append(f"  {label:<17} {count:>5}")
    return lines


def format_plan_summary(entries: list[PlanEntry]) -> str:
    """Format plan statistics summary.

    No ETA is shown because this is called at scan time before any transfers,
    so there is no throughput history. See format_plan_summary_db() for the
    DB-based variant that includes ETA when throughput data is available.
    """
    if not entries:
        return "No plan entries."
    total_entries = len(entries)
    counts = Counter(e.status for e in entries)
    total_bytes = sum(e.size_bytes for e in entries)
    pending_bytes = sum(e.size_bytes for e in entries if e.status in ("pending", "in_progress"))

    lines = [ANSI.bold("Plan Summary:"), ""]
    lines.append(f"  Total entries:    {total_entries}")
    lines.append(f"  Total size:       {format_bytes(total_bytes)}")
    if pending_bytes > 0:
        lines.append(f"  Remaining:        {format_bytes(pending_bytes)}")
    lines.append("")
    lines.extend(_format_status_breakdown(counts, total_entries))
    return "\n".join(lines)


# =============================================================================
# CLI
# =============================================================================

def build_parser(config: dict | None = None) -> argparse.ArgumentParser:
    """Build the argument parser. Config provides defaults for CLI flags."""
    cfg = config or DEFAULT_CONFIG
    parser = argparse.ArgumentParser(
        prog="unraid-rebalancer",
        description="Rebalance data across Unraid disk array drives",
    )
    parser.add_argument(
        "--version", action="version", version=f"%(prog)s {__version__}",
    )
    parser.add_argument(
        "--max-used", type=int, default=cfg.get("max_used", 80),
        help=f"Target max usage percentage per disk (default: {cfg.get('max_used', 80)})",
    )
    parser.add_argument(
        "--strategy", choices=["fullest-first", "largest-first", "smallest-first"],
        default=cfg.get("strategy", "fullest-first"),
        help=f"Rebalancing strategy (default: {cfg.get('strategy', 'fullest-first')})",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Show plan without executing transfers",
    )
    parser.add_argument(
        "--force-rescan", action="store_true",
        help="Discard current plan and rebuild from fresh scan",
    )
    parser.add_argument(
        "--status", action="store_true",
        help="Show current job status and exit",
    )
    parser.add_argument(
        "--remote", type=str, default=cfg.get("remote"),
        help="Execute via SSH (e.g., root@unraid.lan)",
    )
    parser.add_argument(
        "--active-hours", type=str, default=cfg.get("active_hours"),
        help="Only transfer during this window (e.g., 22:00-06:00)",
    )
    parser.add_argument(
        "--min-free-space", type=str, default=cfg.get("min_free_space", "50G"),
        help="Minimum free space per target disk (e.g., 100G, 1T, 500M)",
    )
    parser.add_argument(
        "--bwlimit", type=str, default=cfg.get("bwlimit"),
        help="Bandwidth limit for rsync in KB/s (e.g., 50000 for ~50MB/s)",
    )
    parser.add_argument(
        "--copy-timeout", type=int, default=cfg.get("copy_timeout", 86400),
        help="Timeout in seconds for rsync copy phase (default: 86400 = 24h)",
    )
    parser.add_argument(
        "--verify-timeout", type=int, default=cfg.get("verify_timeout", 28800),
        help="Timeout in seconds for rsync verify phase (default: 28800 = 8h)",
    )
    parser.add_argument(
        "--lsof-timeout", type=int, default=cfg.get("lsof_timeout", 120),
        help="Timeout in seconds for lsof in-use checks (default: 120)",
    )
    parser.add_argument(
        "--exclude", action="append", default=[],
        help="Additional share names to exclude (repeatable)",
    )
    parser.add_argument(
        "--include", action="append", default=[],
        help="Remove share from exclude list (overrides --exclude, repeatable)",
    )
    parser.add_argument(
        "--verbose", action="store_true",
        help="Detailed output",
    )
    parser.add_argument(
        "--progress", action="store_true",
        help="Show rsync progress during transfers",
    )
    parser.add_argument(
        "--limit", type=int, default=0,
        help="Max number of transfers to process (0 = unlimited)",
    )
    parser.add_argument(
        "--show-plan", nargs="?", const="all", default=None,
        metavar="STATUS",
        help="Print plan entries to stdout (optionally filter by status)",
    )
    parser.add_argument(
        "--export-csv", action="store_true",
        help="Export plan as CSV to stdout",
    )
    parser.add_argument(
        "--retry-errors", action="store_true",
        help="Reset error entries to pending for retry",
    )
    parser.add_argument(
        "--init-config", action="store_true",
        help="Generate default config.json and exit",
    )
    parser.add_argument(
        "-y", "--yes", action="store_true",
        help="Skip confirmation prompts",
    )
    parser.add_argument(
        "--state-dir", type=str, default=None,
        help="Override state directory (default: ~/.unraid-rebalancer/). "
             "Also settable via UNRAID_REBALANCER_STATE_DIR env var.",
    )
    return parser


# =============================================================================
# Main
# =============================================================================

def acquire_lock(state_dir: Path):
    """Acquire an exclusive lock file. Returns file handle or None."""
    lock_path = state_dir / LOCK_FILE
    try:
        lock_fd = open(lock_path, "w")
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        lock_fd.write(str(os.getpid()))
        lock_fd.flush()
        return lock_fd
    except (OSError, IOError):
        return None


def release_lock(lock_fd) -> None:
    """Release the lock file."""
    if lock_fd:
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_UN)
            lock_fd.close()
        except (OSError, IOError):
            pass


def format_plan_table(entries: list[PlanEntry]) -> str:
    """Format plan entries as TSV table for stdout (pipeable)."""
    lines = ["STATUS\tSIZE_BYTES\tSOURCE_DISK\tTARGET_DISK\tPATH"]
    for e in entries:
        lines.append(f"{e.status}\t{e.size_bytes}\t{e.source_disk}\t{e.target_disk}\t{e.path}")
    return "\n".join(lines)


def format_plan_summary_db(db: PlanDB) -> str:
    """Format plan statistics from database."""
    counts = db.summary()
    if not counts:
        return "No plan entries."
    total_entries = sum(counts.values())
    total_bytes = db.total_bytes()
    pending_bytes = db.remaining_bytes()
    active_count = db.get_meta("session_transfer_limit")

    lines = [ANSI.bold("Plan Summary:"), ""]
    lines.append(f"  Total entries:    {total_entries}")
    lines.append(f"  Total size:       {format_bytes(total_bytes)}")
    if pending_bytes > 0:
        lines.append(f"  Remaining:        {format_bytes(pending_bytes)}")
        rate = db.avg_copy_throughput() or db.avg_throughput()
        if rate is not None and rate > 0:
            lines.append(f"  Estimated time:   {format_eta(pending_bytes / rate)}")
    lines.append("")

    active_suffix = None
    if active_count and counts.get("in_progress", 0) > 0:
        active_suffix = f"limit: {active_count}"
    lines.extend(_format_status_breakdown(counts, total_entries, active_suffix))
    return "\n".join(lines)


def _resolve_state_dir(argv: list[str] | None) -> Path:
    """Resolve state directory from --state-dir flag, env var, or default.

    Must run before config load since config.json lives in state_dir.
    Uses simple argv scanning to avoid circular dependency with
    config-based parser defaults.
    """
    # Check argv for --state-dir (before argparse runs)
    args_list = argv if argv is not None else sys.argv[1:]
    for i, arg in enumerate(args_list):
        if arg == "--state-dir" and i + 1 < len(args_list):
            return Path(args_list[i + 1]).resolve()
        if arg.startswith("--state-dir="):
            return Path(arg.split("=", 1)[1]).resolve()

    # Fall back to env var
    env_val = os.environ.get("UNRAID_REBALANCER_STATE_DIR")
    if env_val:
        return Path(env_val).resolve()

    return STATE_DIR


def main(argv: list[str] | None = None) -> int:
    """Main entry point. Returns exit code."""
    state_dir = _resolve_state_dir(argv)
    state_dir.mkdir(parents=True, exist_ok=True)

    # Load config for parser defaults
    config = load_config(state_dir)
    parser = build_parser(config)
    args = parser.parse_args(argv)

    # --- Banner (skip for data-output and quick-check modes) ---
    if not (args.show_plan is not None or args.export_csv or args.status):
        print(f"\n{BANNER}")
        print(f"\nv{__version__}\n")

    # --- Init config mode ---
    if args.init_config:
        path = save_default_config(state_dir)
        print(f"Config written to {path}")
        print("Edit this file to set your defaults, then run without --init-config.")
        return 0

    # --- Parse min_free_space ---
    try:
        args.min_free_space_bytes = parse_size(args.min_free_space)
    except ValueError as e:
        print(f"Error: --min-free-space: {e}")
        return 1

    # --- Input validation ---
    if not 1 <= args.max_used <= 99:
        print(f"Error: --max-used must be between 1 and 99 (got {args.max_used})")
        return 1
    if args.limit < 0:
        print(f"Error: --limit must be >= 0 (got {args.limit})")
        return 1
    if args.min_free_space_bytes < 0:
        print(f"Error: --min-free-space must be >= 0 (got {args.min_free_space})")
        return 1
    if args.copy_timeout <= 0:
        print(f"Error: --copy-timeout must be > 0 (got {args.copy_timeout})")
        return 1
    if args.verify_timeout <= 0:
        print(f"Error: --verify-timeout must be > 0 (got {args.verify_timeout})")
        return 1
    if args.lsof_timeout <= 0:
        print(f"Error: --lsof-timeout must be > 0 (got {args.lsof_timeout})")
        return 1
    if args.active_hours:
        try:
            parse_time_range(args.active_hours)
        except ValueError as e:
            print(f"Error: {e}")
            return 1
    db_path = state_dir / PLAN_DB_FILE
    drives_path = state_dir / DRIVES_FILE
    log_path = state_dir / TRANSFERS_LOG

    # --- CSV→SQLite migration ---
    _migrate_csv_to_db(state_dir)

    # --- Status mode (no lock needed) ---
    if args.status:
        with PlanDB(db_path) as db:
            if not db.has_plan():
                print("No plan found.")
            else:
                drives = read_drives_json(drives_path)
                if drives:
                    print(format_disk_table(drives))
                    print()
                print(format_plan_summary_db(db))
        return 0

    # --- Show plan mode (no lock needed) ---
    if args.show_plan is not None:
        with PlanDB(db_path) as db:
            if not db.has_plan():
                print("No plan found.")
                return 0
            status_filter = None if args.show_plan == "all" else args.show_plan
            entries = db.get_all(status_filter=status_filter)
            print(format_plan_table(entries))
        return 0

    # --- Export CSV mode (no lock needed) ---
    if args.export_csv:
        with PlanDB(db_path) as db:
            if not db.has_plan():
                print("No plan found.")
                return 0
            entries = db.get_all()
            writer = csv.DictWriter(sys.stdout, fieldnames=PLAN_CSV_FIELDS)
            writer.writeheader()
            for e in entries:
                writer.writerow({
                    "path": e.path, "size_bytes": e.size_bytes,
                    "source_disk": e.source_disk, "target_disk": e.target_disk,
                    "status": e.status,
                })
        return 0

    # --- Lock (prevent concurrent runs) ---
    lock_fd = acquire_lock(state_dir)
    if lock_fd is None:
        print(f"Error: cannot acquire lock ({state_dir / LOCK_FILE}). "
              "Another instance may be running — check with: ps aux | grep rebalancer")
        return 1

    try:
        return _main_locked(args, config, state_dir, db_path, drives_path, log_path)
    finally:
        release_lock(lock_fd)


def _main_locked(args, config, state_dir, db_path, drives_path, log_path) -> int:
    """Main logic, called while holding the lock."""
    # --- Setup ---
    setup_signal_handlers()
    config_excludes = config.get("excludes", [])
    includes = set(args.include)
    excludes = [s for s in (config_excludes + args.exclude) if s not in includes]

    # --- Remote validation ---
    if args.remote:
        if not validate_remote_connection(args.remote):
            print(f"Error: cannot connect to {args.remote}")
            return 1

    # --- Tool availability check ---
    missing = _check_required_tools(remote=args.remote)
    if missing:
        print(f"Error: required tools not found: {', '.join(missing)}")
        return 1

    # --- Open database (closed in finally block) ---
    db = PlanDB(db_path)
    try:
        return _run_with_db(args, db, excludes, drives_path, log_path)
    finally:
        db.close()


def _run_with_db(args, db, excludes, drives_path, log_path) -> int:
    """Core execution logic with an open PlanDB."""
    # --- Recovery ---
    count = db.recover_in_progress()
    if count:
        print(f"Recovered {count} interrupted transfer(s)")

    # --- Retry errors ---
    if args.retry_errors:
        retried = db.retry_errors()
        if retried:
            print(f"Reset {retried} error entries to pending")
        else:
            print("No error entries to retry")

    # --- Scan / Plan ---
    need_scan = not db.has_plan() or args.force_rescan
    if args.force_rescan and db.has_plan() and not args.yes:
        pending = db.get_pending()
        if pending:
            print(f"Warning: existing plan has {len(pending)} pending transfer(s).")
            print("Rescanning will discard the current plan and create a new one.")
            try:
                answer = input("Continue? [y/N] ").strip().lower()
            except (EOFError, KeyboardInterrupt):
                answer = ""
            if answer != "y":
                print("Aborted.")
                return 0
    if need_scan:
        print("Discovering disks...")
        disks = discover_disks(remote=args.remote)
        if not disks:
            print("Error: no disks found at /mnt/disk*. Are you running on Unraid?")
            return 1
        print(format_disk_table(disks, max_used=args.max_used))
        print()
        write_drives_json(disks, drives_path)

        print("Scanning movable units...")
        all_units = []
        for disk in disks:
            units = scan_movable_units(disk, excludes, remote=args.remote)
            all_units.extend(units)
            if args.verbose:
                print(f"  {disk.path}: {len(units)} units")
        print(f"Found {len(all_units)} movable units")
        print()

        over, under = classify_disks(disks, args.max_used)
        plan = generate_plan(
            all_units, over, under,
            strategy=args.strategy,
            max_used=args.max_used,
            min_free=args.min_free_space_bytes,
        )
        db.write_plan(plan)
        print(format_plan_summary(plan))

        if not over:
            print("\nAll disks are within target. Nothing to do.")
            return 0

        if not plan:
            print("\nNo moves possible (units too large or no target space).")
            return 0

    # --- Dry run ---
    if args.dry_run:
        entries = db.get_all()
        print("\n" + format_plan_summary(entries))
        print("\nDry run — no transfers executed.")
        return 0

    # --- Confirm before execution ---
    pending = db.get_pending()
    total = len(pending)
    if total > 0 and not args.yes:
        pending_bytes = sum(e.size_bytes for e in pending)
        print(f"\nReady to move {total} entries ({format_bytes(pending_bytes)}).")
        try:
            answer = input("Proceed? [y/N] ").strip().lower()
        except (EOFError, KeyboardInterrupt):
            answer = ""
        if answer != "y":
            print("Aborted. Use --dry-run to preview, or --yes to skip this prompt.")
            return 0

    # --- Execute ---
    completed = 0

    limit = args.limit if args.limit > 0 else total
    if args.limit > 0:
        db.set_meta("session_transfer_limit", str(limit))
    print(f"\nStarting transfers: {total} pending" +
          (f" (limit: {limit})" if args.limit > 0 else ""))
    try:
        for entry in pending:
            if shutdown_requested():
                print("\nShutdown requested. Exiting after current transfer.")
                break

            if completed >= limit:
                print(f"\nLimit reached ({limit} transfers).")
                break

            # Active hours check
            if not is_within_active_hours(args.active_hours):
                print("Outside active hours. Waiting...")
                while not is_within_active_hours(args.active_hours):
                    if shutdown_requested():
                        break
                    time_mod.sleep(60)
                if shutdown_requested():
                    break

            # Check if in use
            if check_in_use(entry.path, remote=args.remote, timeout=args.lsof_timeout):
                print(f"  SKIP (in use): {entry.path}")
                db.update_status(entry.path, "skipped")
                continue

            # Transfer
            db.update_status(entry.path, "in_progress")
            # Show share/item (e.g., "Movies/2023") and short disk names (e.g., "disk4")
            parts = entry.path.split("/")
            short_path = "/".join(parts[3:]) if len(parts) > 3 else os.path.basename(entry.path)
            src_disk = entry.source_disk.split("/")[-1]
            tgt_disk = entry.target_disk.split("/")[-1]
            copy_rate = db.avg_copy_throughput() or db.avg_throughput()
            last_rate = db.last_copy_throughput() or db.last_throughput()
            verify_rate = db.avg_verify_throughput()
            if copy_rate and copy_rate > 0:
                eta_parts = [f"copy {format_eta(entry.size_bytes / copy_rate)}"]
                if verify_rate and verify_rate > 0:
                    eta_parts.append(f"verify {format_eta(entry.size_bytes / verify_rate)}")
                rate_str = f" @ {format_bytes(int(last_rate))}/s" if last_rate else ""
                eta_str = f" \u2014 {', '.join(eta_parts)}{rate_str}"
            else:
                eta_str = " \u2014 no ETA"
            print(f"  {_now_hms()} [{completed + 1}/{total}] Moving {short_path} "
                  f"({format_bytes(entry.size_bytes)}) "
                  f"{src_disk} -> {tgt_disk}{eta_str}")

            result = transfer_unit(
                entry, remote=args.remote, min_free=args.min_free_space_bytes,
                bwlimit=args.bwlimit, copy_timeout=args.copy_timeout,
                verify_timeout=args.verify_timeout,
                lsof_timeout=args.lsof_timeout,
                progress=args.progress,
                phase_status=True,
            )
            db.update_status(entry.path, result.status)
            log_transfer(log_path, entry, result.status, detail=result.detail)

            if result == "skipped_full":
                print(f"    {_now_hms()} SKIP (target disk full)")
                continue
            elif result == "skipped_in_use":
                print(f"    {_now_hms()} SKIP (files in use before delete \u2014 data safe on both disks)")
                continue
            elif result == "cleaned":
                completed += 1
                # Record phase-specific throughput
                if result.copy_seconds:
                    db.record_copy_throughput(entry.size_bytes, result.copy_seconds)
                if result.verify_seconds:
                    db.record_verify_throughput(entry.size_bytes, result.verify_seconds)
                db.record_throughput(entry.size_bytes,
                                    (result.copy_seconds or 0) + (result.verify_seconds or 0) + (result.delete_seconds or 0))
                # Done line with phase breakdown
                phase_parts = []
                if result.copy_seconds is not None:
                    phase_parts.append(f"copy {format_eta(result.copy_seconds)}")
                if result.verify_seconds is not None:
                    phase_parts.append(f"verify {format_eta(result.verify_seconds)}")
                phase_str = f" \u2014 {', '.join(phase_parts)}" if phase_parts else ""
                print(f"    {_now_hms()} Done ({format_bytes(entry.size_bytes)}{phase_str})")
            else:
                print(f"    {_now_hms()} {ANSI.red(f'FAILED: {result.status}')}")
                if result.detail:
                    print(f"    stderr: {result.detail[:200]}")

            # Periodic WAL checkpoint to reclaim space during long runs
            if completed > 0 and completed % 50 == 0:
                db.checkpoint()
    finally:
        db.delete_meta("session_transfer_limit")

    # --- Summary ---
    print(f"\n{format_plan_summary_db(db)}")
    print(f"Completed {completed}/{total} transfers this session.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""
Unraid Rebalancer

Scans /mnt/disk* mounts, builds an intelligent redistribution plan to balance
fill levels across data drives, then executes the plan using rsync.

Default behavior is a dry run (no data is modified). Use --execute to move.

Key ideas
- Works at a configurable "allocation unit" granularity (default: one
  directory level below each share, e.g., /mnt/disk1/Movies/<MovieName>).
- Avoids the user share copy bug by doing disk→disk paths only.
- Preserves permissions/attrs/hardlinks with rsync -aHAX and can resume.
- Lets you target a fill percentage (default 80%) or auto-evening.
- Prints a clear plan before acting and can save/load plans as JSON.

Example
  # Plan only, dry-run copy commands (no changes)
  sudo ./unraid_rebalancer.py --target-percent 80

  # Actually execute the moves from the computed plan
  sudo ./unraid_rebalancer.py --target-percent 80 --execute

  # Exclude certain shares and only consider large units (>= 5 GiB)
  sudo ./unraid_rebalancer.py --exclude-shares appdata,System \
       --min-unit-size 5GiB --execute

Safety notes
- Run at the console or via SSH screen/tmux. Avoid running from the Unraid GUI
  terminal if you might close the browser.
- Never mix /mnt/user with /mnt/diskX in the same command. This script uses
  /mnt/disk* only by design.
- Stop heavy writers (e.g., big downloads) during redistribution.
- Always keep good backups. Use --execute only after reviewing the plan.
"""

from __future__ import annotations

import argparse
import dataclasses
import fnmatch
import json
import logging
import math
import os
import re
import shlex
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

# ---------- Utilities ----------

SIZE_UNITS = {
    "B": 1,
    "KB": 1000,
    "MB": 1000**2,
    "GB": 1000**3,
    "TB": 1000**4,
    "KiB": 1024,
    "MiB": 1024**2,
    "GiB": 1024**3,
    "TiB": 1024**4,
}

def parse_size(s: str) -> int:
    m = re.fullmatch(r"\s*(\d+(?:\.\d+)?)\s*([KMGT]?i?B)\s*", s, re.I)
    if not m:
        raise argparse.ArgumentTypeError(f"Invalid size: {s}")
    val = float(m.group(1))
    unit = m.group(2)
    # Normalize case to match dict keys (e.g., GiB)
    for k in SIZE_UNITS:
        if k.lower() == unit.lower():
            return int(val * SIZE_UNITS[k])
    raise argparse.ArgumentTypeError(f"Unknown unit in size: {s}")


def human_bytes(n: int) -> str:
    """Convert bytes to human-readable format using binary units."""
    if n == 0:
        return "0 B"
    
    units = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"]
    for i, unit in enumerate(units):
        if n < 1024 or i == len(units) - 1:
            if unit == "B":
                return f"{n} {unit}"
            else:
                return f"{n:.2f} {unit}"
        n /= 1024
    return f"{n:.2f} PiB"


def run(cmd: List[str], dry_run: bool = False) -> int:
    """Execute a command, optionally in dry-run mode."""
    cmd_str = " ".join(shlex.quote(c) for c in cmd)
    print("$", cmd_str)
    
    if dry_run:
        return 0
    
    try:
        return subprocess.call(cmd)
    except FileNotFoundError as e:
        logging.error(f"Command not found: {cmd[0]}")
        return 127
    except Exception as e:
        logging.error(f"Error executing command: {e}")
        return 1


def is_mounted(path: Path) -> bool:
    """Check if a path is mounted by attempting to get filesystem stats."""
    try:
        os.statvfs(path)
        return True
    except (FileNotFoundError, OSError):
        return False

# ---------- Data Structures ----------

@dataclasses.dataclass
class Disk:
    name: str  # e.g., disk1
    path: Path  # /mnt/disk1
    size_bytes: int
    used_bytes: int
    free_bytes: int

    @property
    def used_pct(self) -> float:
        return (self.used_bytes / self.size_bytes) * 100 if self.size_bytes else 0.0


@dataclasses.dataclass
class Unit:
    """An allocation unit to move as a whole (a directory or a single file)."""
    share: str           # top-level share name (e.g., Movies)
    rel_path: str        # path relative to share root (e.g., "Alien (1979)")
    size_bytes: int
    src_disk: str        # e.g., disk1

    def src_abs(self) -> Path:
        return Path(f"/mnt/{self.src_disk}") / self.share / self.rel_path

    def dest_abs(self, dest_disk: str) -> Path:
        return Path(f"/mnt/{dest_disk}") / self.share / self.rel_path


@dataclasses.dataclass
class Move:
    unit: Unit
    dest_disk: str


@dataclasses.dataclass
class Plan:
    moves: List[Move]
    summary: Dict[str, float]

    def to_json(self) -> str:
        obj = {
            "moves": [
                {
                    "share": m.unit.share,
                    "rel_path": m.unit.rel_path,
                    "size_bytes": m.unit.size_bytes,
                    "src_disk": m.unit.src_disk,
                    "dest_disk": m.dest_disk,
                }
                for m in self.moves
            ],
            "summary": self.summary,
        }
        return json.dumps(obj, indent=2)

    @staticmethod
    def from_json(s: str) -> "Plan":
        obj = json.loads(s)
        moves = [
            Move(
                Unit(
                    share=mo["share"],
                    rel_path=mo["rel_path"],
                    size_bytes=int(mo["size_bytes"]),
                    src_disk=mo["src_disk"],
                ),
                dest_disk=mo["dest_disk"],
            )
            for mo in obj["moves"]
        ]
        return Plan(moves=moves, summary=obj.get("summary", {}))

# ---------- Discovery & Scanning ----------

def discover_disks(include: Optional[List[str]] = None,
                   exclude: Optional[List[str]] = None) -> List[Disk]:
    roots = sorted(p for p in Path("/mnt").glob("disk*") if p.is_dir())
    disks: List[Disk] = []
    for p in roots:
        name = p.name  # disk1, disk2, ...
        if include and name not in include:
            continue
        if exclude and name in exclude:
            continue
        if not is_mounted(p):
            continue
        st = os.statvfs(p)
        size = st.f_frsize * st.f_blocks
        free = st.f_frsize * st.f_bavail
        used = size - free
        disks.append(Disk(name=name, path=p, size_bytes=size, used_bytes=used, free_bytes=free))
    return disks


def iter_units_on_disk(disk: Disk, unit_depth: int, 
                       include_shares: Optional[List[str]],
                       exclude_shares: Optional[List[str]],
                       min_unit_size: int,
                       exclude_globs: List[str]) -> Iterable[Unit]:
    # Scan top-level shares under this disk
    if not disk.path.exists():
        return
    for share_root in sorted(p for p in disk.path.iterdir() if p.is_dir()):
        share = share_root.name
        if include_shares and share not in include_shares:
            continue
        if exclude_shares and share in exclude_shares:
            continue
        # Build allocation units at requested depth
        # depth=1: each direct child of share root is a unit; files at root are individual units
        # depth=0: the entire share on this disk is one unit
        # depth>=2: go deeper
        if unit_depth == 0:
            size = du_path(share_root)
            if size >= min_unit_size:
                rel = ""  # entire share content
                yield Unit(share=share, rel_path=rel, size_bytes=size, src_disk=disk.name)
            continue

        # Descend to unit_depth below share_root
        def gen_candidates(root: Path, depth: int) -> Iterable[Path]:
            if depth == 0:
                yield root
            else:
                try:
                    for child in root.iterdir():
                        if child.is_dir():
                            yield from gen_candidates(child, depth - 1)
                        elif depth == 1 and child.is_file():
                            # files at target depth count as units too
                            yield child
                except PermissionError:
                    return
        for cand in gen_candidates(share_root, unit_depth):
            rel = str(cand.relative_to(share_root)) if cand != share_root else ""
            # apply globs relative to share
            rel_for_match = f"{share}/{rel}" if rel else f"{share}"
            if any(fnmatch.fnmatch(rel_for_match, g) for g in exclude_globs):
                continue
            size = du_path(cand)
            if size >= min_unit_size:
                yield Unit(share=share, rel_path=rel, size_bytes=size, src_disk=disk.name)


def du_path(path: Path) -> int:
    """Calculate total size of a path (file or directory) in bytes."""
    total = 0
    try:
        if path.is_file():
            return path.stat().st_size
        
        for root, dirs, files in os.walk(path, onerror=lambda e: None):
            for filename in files:
                try:
                    filepath = Path(root) / filename
                    total += filepath.stat().st_size
                except (FileNotFoundError, PermissionError):
                    continue
    except PermissionError:
        return 0
    return total

# ---------- Planning ----------

def build_plan(disks: List[Disk], units: List[Unit], target_percent: Optional[float],
               headroom_percent: float) -> Plan:
    # Compute targets
    # If target_percent provided, aim each disk to be <= target_percent and also
    # try to raise low disks to (100 - headroom_percent)
    # Otherwise, compute equalizing average used across disks with headroom.
    total_size = sum(d.size_bytes for d in disks)
    total_used = sum(d.used_bytes for d in disks)

    if target_percent is not None:
        target_used_per_disk = [min(d.size_bytes * (target_percent / 100.0), d.size_bytes) for d in disks]
    else:
        avg_used = total_used / len(disks) if disks else 0
        # leave some breathing room
        target_used_per_disk = [min(avg_used, d.size_bytes * (1 - headroom_percent / 100.0)) for d in disks]

    # Classify disks
    donors: Dict[str, float] = {}  # disk -> bytes to shed
    recipients: Dict[str, float] = {}  # disk -> bytes it can take (up to target)
    for d, tgt in zip(disks, target_used_per_disk):
        if d.used_bytes > tgt:
            donors[d.name] = d.used_bytes - tgt
        elif d.used_bytes < tgt:
            recipients[d.name] = tgt - d.used_bytes

    # Sort units from donors by size (largest first for fewer moves)
    donor_units = [u for u in units if u.src_disk in donors]
    donor_units.sort(key=lambda u: u.size_bytes, reverse=True)

    # Sort recipients by most capacity needed first
    recipient_list = sorted(recipients.items(), key=lambda kv: kv[1], reverse=True)

    moves: List[Move] = []

    # Create disk lookup for efficiency
    disk_lookup = {d.name: d for d in disks}
    
    # Greedy assignment: place each unit on the recipient that needs it most and fits
    for unit in donor_units:
        # Refresh recipient order each time
        recipient_list.sort(key=lambda kv: kv[1], reverse=True)
        placed = False
        for rdisk, need_bytes in recipient_list:
            if need_bytes <= 0:  # Skip if recipient is full
                continue
                
            # Ensure destination has free space for the unit plus 1 GiB margin
            dest_disk = disk_lookup[rdisk]
            margin = 1 * 1024**3  # 1 GiB safety margin
            
            if unit.size_bytes + margin <= dest_disk.free_bytes + recipients[rdisk]:
                moves.append(Move(unit=unit, dest_disk=rdisk))
                # Update bookkeeping: donor sheds, recipient fills
                donors[unit.src_disk] -= unit.size_bytes
                recipients[rdisk] -= unit.size_bytes
                placed = True
                break
        
        # If not placed (e.g., unit too large for any recipient), skip
        if not placed:
            continue

    summary = {
        "total_moves": len(moves),
        "total_bytes": float(sum(m.unit.size_bytes for m in moves)),
    }
    return Plan(moves=moves, summary=summary)

# ---------- Execution ----------

def perform_plan(plan: Plan, execute: bool, rsync_extra: List[str], allow_merge: bool) -> int:
    failures = 0
    for idx, m in enumerate(plan.moves, 1):
        src = m.unit.src_abs()
        dst = m.unit.dest_abs(m.dest_disk)
        # Ensure parent exists on destination
        dst_parent = dst.parent
        dst_parent.mkdir(parents=True, exist_ok=True)

        # If destination exists and not allowed to merge, skip
        if dst.exists() and not allow_merge:
            print(f"[SKIP] Destination exists and --allow-merge not set: {dst}")
            continue

        # rsync path handling
        if src.is_dir():
            # Trailing slash to copy contents of dir into directory (rsync semantics)
            src_r = str(src) + "/"
            dst_r = str(dst)
        else:
            src_r = str(src)
            dst_r = str(dst)

        cmd = [
            "rsync",
            "-aHAX",
            "--info=progress2",
            "--partial",
            "--inplace",
            "--numeric-ids",
        ] + rsync_extra + [src_r, dst_r]

        print(f"\n[{idx}/{len(plan.moves)}] Moving {m.unit.share}/{m.unit.rel_path} "
              f"from {m.unit.src_disk} -> {m.dest_disk} ({human_bytes(m.unit.size_bytes)})")
        rc = run(cmd, dry_run=not execute)
        if rc != 0:
            print(f"[ERROR] rsync returned {rc}")
            failures += 1
            continue

        if execute:
            # After successful copy, remove source files
            if src.is_dir():
                # Remove files that have been copied; then clean up empty dirs
                rm_cmd = ["rsync", "-aHAX", "--remove-source-files", str(src) + "/", str(dst)]
                rc2 = run(rm_cmd, dry_run=False)
                if rc2 != 0:
                    print(f"[WARN] cleanup rsync returned {rc2}")
                    logging.warning(f"Failed to remove source files from {src}")
                
                # Remove empty directories
                try:
                    for root, dirs, files in os.walk(src, topdown=False):
                        if not dirs and not files:
                            try:
                                os.rmdir(root)
                            except OSError as e:
                                logging.debug(f"Could not remove directory {root}: {e}")
                except Exception as e:
                    logging.warning(f"Error during directory cleanup: {e}")
            else:
                try:
                    os.remove(src)
                except FileNotFoundError:
                    logging.debug(f"Source file {src} already removed")
                except Exception as e:
                    logging.error(f"Failed to remove source file {src}: {e}")
    return failures

# ---------- CLI ----------

def main():
    p = argparse.ArgumentParser(description="Rebalance Unraid data drives by moving directory/file units between /mnt/disk*.")
    p.add_argument("--include-disks", help="Comma list of disk names to include (e.g., disk1,disk2)")
    p.add_argument("--exclude-disks", help="Comma list of disk names to exclude")
    p.add_argument("--include-shares", help="Comma list of shares to include (default: all)")
    p.add_argument("--exclude-shares", help="Comma list of shares to exclude (e.g., appdata,System)")
    p.add_argument("--exclude-globs", default="", help="Comma list of globs relative to share root to skip (e.g., 'appdata/*,System/*')")
    p.add_argument("--unit-depth", type=int, default=1, help="Allocation unit depth under each share (0 = whole share on a disk, 1 = share's immediate children [default], 2 = grandchildren, etc.)")
    p.add_argument("--min-unit-size", type=parse_size, default=parse_size("1GiB"), help="Only move units >= this size (default 1GiB)")
    p.add_argument("--target-percent", type=float, default=80.0, help="Target maximum fill percent per disk (default 80). Use -1 to auto-even with headroom.")
    p.add_argument("--headroom-percent", type=float, default=5.0, help="Headroom percent when auto-evening (ignored if target-percent >= 0)")
    p.add_argument("--save-plan", help="Write plan JSON to this path")
    p.add_argument("--load-plan", help="Load plan from JSON and skip planning")
    p.add_argument("--execute", action="store_true", help="Execute moves (default is dry-run)")
    p.add_argument("--rsync-extra", default="", help="Extra args to pass to rsync (comma-separated, e.g., '--bwlimit=50M,--checksum')")
    p.add_argument("--allow-merge", action="store_true", help="Allow merging into existing destination directories if present")
    p.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")
    p.add_argument("--log-file", help="Write logs to this file (default: stderr only)")

    args = p.parse_args()
    
    # Configure logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    log_format = '%(asctime)s - %(levelname)s - %(message)s'
    
    if args.log_file:
        logging.basicConfig(level=log_level, format=log_format, 
                          handlers=[
                              logging.FileHandler(args.log_file),
                              logging.StreamHandler(sys.stderr)
                          ])
    else:
        logging.basicConfig(level=log_level, format=log_format, stream=sys.stderr)

    include_disks = args.include_disks.split(",") if args.include_disks else None
    exclude_disks = args.exclude_disks.split(",") if args.exclude_disks else None
    include_shares = args.include_shares.split(",") if args.include_shares else None
    exclude_shares = args.exclude_shares.split(",") if args.exclude_shares else None
    exclude_globs = [g.strip() for g in args.exclude_globs.split(",") if g.strip()]
    rsync_extra = [s for s in args.rsync_extra.split(",") if s]

    # Step 1: Discover disks and their usage
    disks = discover_disks(include_disks, exclude_disks)
    if not disks:
        print("No /mnt/disk* data disks found. Are you running on Unraid?")
        return 2

    print("Discovered disks:")
    for d in disks:
        print(f"  {d.name}: used={human_bytes(d.used_bytes)} ({d.used_pct:.1f}%), free={human_bytes(d.free_bytes)}")

    if args.load_plan:
        try:
            plan = Plan.from_json(Path(args.load_plan).read_text())
            print(f"Loaded plan with {len(plan.moves)} moves totaling {human_bytes(int(plan.summary.get('total_bytes', 0)))}")
            logging.info(f"Successfully loaded plan from {args.load_plan}")
        except Exception as e:
            logging.error(f"Failed to load plan from {args.load_plan}: {e}")
            return 1
    else:
        # Step 2: Scan units
        print("\nScanning allocation units (this can take a while)...")
        units: List[Unit] = []
        for d in disks:
            for u in iter_units_on_disk(
                disk=d,
                unit_depth=args.unit_depth,
                include_shares=include_shares,
                exclude_shares=exclude_shares,
                min_unit_size=args.min_unit_size,
                exclude_globs=exclude_globs,
            ):
                units.append(u)
        total_units = len(units)
        total_bytes = sum(u.size_bytes for u in units)
        print(f"Found {total_units} units totaling {human_bytes(total_bytes)}")

        # Step 3: Build plan
        target_percent = None if args.target_percent < 0 else args.target_percent
        plan = build_plan(disks, units, target_percent=target_percent, headroom_percent=args.headroom_percent)
        print(f"\nPlan: {len(plan.moves)} moves, {human_bytes(int(plan.summary['total_bytes']))} to re-distribute.")
        # Preview first few moves
        for i, m in enumerate(plan.moves[:20], 1):
            print(f"  {i:>3}. {m.unit.share}/{m.unit.rel_path} | {human_bytes(m.unit.size_bytes)} | {m.unit.src_disk} -> {m.dest_disk}")
        if len(plan.moves) > 20:
            print(f"  ... and {len(plan.moves)-20} more")

        if args.save_plan:
            try:
                Path(args.save_plan).write_text(plan.to_json())
                print(f"Saved plan to {args.save_plan}")
                logging.info(f"Plan saved to {args.save_plan}")
            except Exception as e:
                logging.error(f"Failed to save plan to {args.save_plan}: {e}")
                return 1

    # Step 4: Execute (or dry-run)
    mode = "EXECUTE" if args.execute else "DRY-RUN"
    print(f"\n=== {mode} {len(plan.moves)} planned move(s) ===")
    failures = perform_plan(plan, execute=args.execute, rsync_extra=rsync_extra, allow_merge=args.allow_merge)

    if failures:
        print(f"\nCompleted with {failures} failure(s). Review the log above.")
        return 1
    else:
        print("\nCompleted successfully.")
        return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\nInterrupted.")
        sys.exit(130)

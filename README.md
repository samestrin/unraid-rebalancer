```
88   88 88b 88 88""Yb    db    88 8888b.
88   88 88Yb88 88__dP   dPYb   88  8I  Yb
Y8   8P 88 Y88 88"Yb   dP__Yb  88  8I  dY
`YbodP' 88  Y8 88  Yb dP""""Yb 88 8888Y"
88""Yb 888888 88""Yb    db    88        db    88b 88  dP""b8 888888 88""Yb
88__dP 88__   88__dP   dPYb   88       dPYb   88Yb88 dP   `" 88__   88__dP
88"Yb  88""   88""Yb  dP__Yb  88  .o  dP__Yb  88 Y88 Yb      88""   88"Yb
88  Yb 888888 88oodP dP""""Yb 88ood8 dP""""Yb 88  Y8  YboodP 888888 88  Yb
```

Rebalance data across Unraid disk array drives. Moves folders from overloaded disks to underloaded ones using rsync with checksum verification.

## Features

- **Hybrid rebalancing**: drains disks above threshold (only as much as needed), fills the lowest-usage target first. When all disks exceed the threshold, still makes progress by moving from the fullest to the least-full.
- **Three-phase transfer**: rsync copy (`-aHP` with partial resume), checksum verify (`--itemize-changes`, directory-only attribute changes filtered out), delete source. Source is never deleted unless verification confirms an exact match. Crash-safe: resumes partial transfers automatically.
- **Configurable**: `/boot/config/plugins/rebalancer/config.json` sets persistent defaults (excludes, thresholds, schedule). CLI flags override config values. State directory overridable via `--state-dir` or `UNRAID_REBALANCER_STATE_DIR` env var.
- **SQLite state**: plan stored in WAL-mode SQLite for crash safety, O(1) status updates, and concurrent `--status` reads during execution.
- **Pause/resume**: Ctrl+C or `kill <pid>` (SIGTERM) gracefully finishes current transfer. Double Ctrl+C within 3 seconds force exits. Interrupted transfers recover automatically on restart.
- **Active hours**: `--active-hours 22:00-06:00` to only run during off-peak times (supports overnight ranges). Start and end times must differ. Note: a transfer in progress runs to completion even if the window ends.
- **Strategies**: `fullest-first` (default), `largest-first`, `smallest-first`
- **Remote mode**: run from your Mac, or Linux machine, transfers execute on Unraid via SSH (`BatchMode=yes`)
- **Safety**: lsof checks for open files, pre-transfer disk space recheck, path validation (requires `/mnt/diskN/share/item` depth), symlink rejection before delete (prevents `rm -rf` following symlinks), lock file prevents concurrent runs, confirmation prompts before execution, timed-out child processes are killed (no orphaned rsync)
- **Progress output**: `--progress` shows live rsync transfer progress during copy phase
- **Error diagnostics**: failed transfers log rsync stderr to `transfers.log` for headless debugging
- **Bandwidth control**: `--bwlimit` throttles rsync to prevent saturating disk I/O during Plex/Samba usage

## Requirements

- Python 3.10+ (stdlib only, no pip dependencies). Install via [NerdTools Community Applications plugin](https://forums.unraid.net/topic/35866-unraid-6-nerdtools/).
- rsync 3.1+ and lsof on the target machine (included with Unraid)

## Installation

1. Download `rebalancer.py` from [releases](https://github.com/samestrin/unraid-rebalancer/releases) or clone the repo

2. Copy to your Unraid server (persistent storage that survives reboots):
   ```bash
   ssh root@<your-unraid-host> "mkdir -p /boot/config/plugins/rebalancer"
   scp rebalancer.py root@<your-unraid-host>:/boot/config/plugins/rebalancer/
   ```

3. Create a symlink so it's in your PATH:
   ```bash
   ssh root@<your-unraid-host> "chmod +x /boot/config/plugins/rebalancer/rebalancer.py && ln -sf /boot/config/plugins/rebalancer/rebalancer.py /usr/local/bin/rebalancer.py"
   ```

4. Make the symlink persist across reboots by adding this line to `/boot/config/go`:
   ```bash
   ln -sf /boot/config/plugins/rebalancer/rebalancer.py /usr/local/bin/rebalancer.py
   ```

5. Generate default config:
   ```bash
   ssh root@<your-unraid-host> "python3 rebalancer.py --init-config"
   ```

## Configuration

On first run, generate a config file:

```bash
python3 rebalancer.py --init-config
```

This creates `/boot/config/plugins/rebalancer/config.json`:

```json
{
  "max_used": 80,
  "strategy": "fullest-first",
  "excludes": ["Backups", "Development", "appdata"],
  "active_hours": null,
  "min_free_space": "50G",
  "bwlimit": null,
  "copy_timeout": 86400,
  "verify_timeout": 28800,
  "lsof_timeout": 120,
  "remote": null
}
```

Edit this file to set your defaults. CLI flags always override config values.

| Config Key | CLI Flag | Description |
|------------|----------|-------------|
| `max_used` | `--max-used` | Target max usage percentage (1-99) |
| `strategy` | `--strategy` | Rebalancing strategy |
| `excludes` | `--exclude` / `--include` | Shares to skip (CLI adds/removes from this list) |
| `active_hours` | `--active-hours` | Time window for transfers |
| `min_free_space` | `--min-free-space` | Min free space per target (default: `"50G"`) |
| `bwlimit` | `--bwlimit` | Bandwidth limit for rsync in KB/s |
| `copy_timeout` | `--copy-timeout` | Timeout in seconds for rsync copy phase (default: 86400) |
| `verify_timeout` | `--verify-timeout` | Timeout in seconds for rsync verify phase (default: 28800) |
| `lsof_timeout` | `--lsof-timeout` | Timeout in seconds for lsof in-use checks (default: 120) |
| `remote` | `--remote` | SSH target for remote mode |

## Usage

```bash
# Dry run — see what would happen
python3 rebalancer.py --dry-run

# Run with defaults (prompts for confirmation)
python3 rebalancer.py

# Skip confirmation prompts (for scripted use)
python3 rebalancer.py --yes

# Custom threshold
python3 rebalancer.py --max-used 85

# Set minimum free space per target disk
python3 rebalancer.py --min-free-space 100G

# Test with a single transfer first
python3 rebalancer.py --limit 1 --yes

# Run during off-peak hours only
python3 rebalancer.py --active-hours 01:00-07:00 --yes

# Remote mode (from your Mac)
python3 rebalancer.py --remote root@unraid.lan --dry-run

# Check status of a running/paused job
python3 rebalancer.py --status

# View planned moves (pipeable)
python3 rebalancer.py --show-plan
python3 rebalancer.py --show-plan pending

# Export plan as CSV
python3 rebalancer.py --export-csv > plan_backup.csv

# Force rebuild plan after adding a new disk
python3 rebalancer.py --force-rescan

# Retry failed transfers
python3 rebalancer.py --retry-errors --yes

# Exclude additional shares
python3 rebalancer.py --exclude Manga --exclude Comics

# Override a config exclusion
python3 rebalancer.py --include Development

# Show live rsync progress during transfers
python3 rebalancer.py --progress --yes

# Use a custom state directory
python3 rebalancer.py --state-dir /mnt/cache/rebalancer-state --dry-run
```

## CLI Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--max-used` | 80 | Target max usage percentage per disk (1-99) |
| `--strategy` | fullest-first | `fullest-first`, `largest-first`, `smallest-first` |
| `--dry-run` | off | Show plan without executing |
| `--force-rescan` | off | Discard current plan and rebuild from fresh scan |
| `--status` | off | Show current job status and exit |
| `--show-plan` | off | Print plan entries as TSV (optional status filter) |
| `--export-csv` | off | Dump plan as CSV to stdout |
| `--remote` | none | Execute via SSH (e.g., `root@unraid.lan`) |
| `--active-hours` | none | Time window, HH:MM-HH:MM (e.g., `22:00-06:00`) |
| `--min-free-space` | 50G | Min free space per target (e.g., `100G`, `1T`, `500M`) |
| `--bwlimit` | none | Bandwidth limit for rsync in KB/s (e.g., `50000` for ~50MB/s) |
| `--copy-timeout` | 86400 | Timeout in seconds for rsync copy phase (24h) |
| `--verify-timeout` | 28800 | Timeout in seconds for rsync verify phase (8h) |
| `--lsof-timeout` | 120 | Timeout in seconds for lsof in-use checks |
| `--exclude` | none | Additional shares to skip (repeatable) |
| `--include` | none | Remove share from exclude list (overrides config/--exclude) |
| `--limit` | 0 | Max transfers per session (0 = unlimited) |
| `--retry-errors` | off | Reset error entries to pending for retry |
| `--init-config` | off | Generate default config.json and exit |
| `-y`, `--yes` | off | Skip confirmation prompts |
| `--verbose` | off | Detailed output |
| `--progress` | off | Show live rsync progress during transfers |
| `--state-dir` | `/boot/config/plugins/rebalancer/` | Override state directory (also: `UNRAID_REBALANCER_STATE_DIR` env var) |

## State Files

Stored in `/boot/config/plugins/rebalancer/`:

| File | Format | Purpose |
|------|--------|---------|
| `config.json` | JSON | Persistent defaults (excludes, thresholds, etc.) |
| `plan.db` | SQLite (WAL) | Transfer plan with per-entry status |
| `drives.json` | JSON | Disk usage snapshot from last scan |
| `transfers.log` | TSV | Append-only log of completed moves (optional 7th column: stderr detail on errors) |
| `rebalancer.lock` | lock | Prevents concurrent runs |

## Exclusions

Default exclusions are defined in `config.json` (initially `["Backups", "Development", "appdata"]`). Edit the file to change them.

- `--exclude Manga` adds Manga to the exclusion list for this run
- `--include Backups` removes Backups from exclusions for this run
- `--include` takes precedence over `--exclude`

## Move Units

- **Movies**: year folders matching 1900-2099 (e.g., `2024/`, `1999/`). Non-year directories like `Extras/` are ignored.
- **All other shares**: direct child folders (e.g., individual show/anime folders)

## Error Statuses

| Status | Meaning | Action |
|--------|---------|--------|
| `skipped_full` | Target disk has insufficient free space | Rerun later or use `--min-free-space 0` |
| `skipped_in_use` | Files in use before delete (data safe on both disks) | Rerun later; use `--retry-errors` to re-attempt |
| `error_path` | Invalid/unsafe path, symlink source, or mkdir failed | Check plan for bad entries |
| `error_copy` | rsync copy failed | Check `transfers.log` for stderr detail; check disk space, permissions, connectivity |
| `error_verify` | Checksum mismatch after copy | Data exists on both disks; check `transfers.log` for stderr detail |
| `error_delete` | Source deletion failed | Data on both disks; check `transfers.log` for stderr detail |
| `error_timeout` | Command timed out | Check connectivity, disk health; see `--copy-timeout` / `--verify-timeout` |

Use `--retry-errors` to reset all error entries to pending for another attempt.

## Crash Recovery

If the process crashes or loses power mid-transfer:
- **Partial copy on target**: rsync is idempotent — the next run detects the partial target and resumes from where it left off. No data loss.
- **After copy, before verify**: rsync re-runs (no-op since target is complete), then verify proceeds normally.
- **After verify, before delete**: rsync re-runs (no-op), verify passes again, source is deleted.
- **During delete**: partial source remains. The entry is marked `error_delete`. Use `--retry-errors` to reattempt.

## Important Notes

### Docker and VM Direct Disk Mappings

If a Docker container or VM maps a path directly to a specific disk (e.g., `/mnt/disk1/appdata/plex`), moving that folder to another disk will break the container. The `appdata` share is excluded by default for this reason. If you have other direct disk mappings, add them to your excludes.

### Share Layer (/mnt/user/)

Unraid's FUSE-based share aggregation layer (`/mnt/user/`) updates automatically when files move between disks. You do not need to restart any services after rebalancing. Samba shares served via `/mnt/user/` will see moved files immediately.

### Race Window at Delete Time

After verifying a copy, the tool checks `lsof` to confirm no processes have files open in the source directory, then deletes the source. There is a brief window between the lsof check and the `rm -rf` where a new process could open a file. In this case, the data is safe on the target disk, but the process that just opened the source file may lose access. This is inherent to any non-atomic file operation on Linux and the risk is extremely low in practice.

## Development

```bash
cd unraid-rebalancer
python3 -m pytest tests/ --cov --cov-report=term-missing
```

## License

MIT - see [LICENSE](LICENSE).

## Disclaimer

Use at your own risk. Always have current backups before disk operations.

## Acknowledgments

- Inspired by the Unraid community's need for better disk balancing tools
- Built with safety and reliability as primary concerns
- Thanks to all contributors and testers

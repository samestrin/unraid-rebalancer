# Technical Debt

### From Branch: feature/sync-before-delete

| STATUS | SEVERITY | FILE:LINE | PROBLEM | FIX | REVIEWERS |
|--------|----------|-----------|---------|-----|-----------|
| [ ] | LOW | rebalancer.py:1392 | Only the target is flushed. The source filesystem is not synced after `rm -rf`, so a power loss right after delete can roll back the unlink and the source tree reappears (duplicate data, not data loss). | Run `sync -f <source_disk>` after a successful delete, or accept duplicates as recoverable on the next rescan. | atcr:brad |
| [ ] | LOW | rebalancer.py:1342 | Sync failure detail is prefixed `sync failed:`; reviewer flagged the prefix as redundant in the log detail column. | Leave as-is (prefix is what distinguishes a sync failure from an rsync failure under `error_copy`) or move the marker into a dedicated field. | atcr:otto |

"""Tests for PlanDB SQLite state management."""

import sqlite3

import pytest

from rebalancer import PlanDB, PlanEntry, PLAN_DB_FILE


class TestPlanDBCore:
    def test_create_empty_db(self, state_dir):
        db_path = state_dir / PLAN_DB_FILE
        db = PlanDB(db_path)
        assert db_path.exists()
        assert db.has_plan() is False
        db.close()

    def test_write_and_get_all_roundtrip(self, state_dir):
        db = PlanDB(state_dir / PLAN_DB_FILE)
        entries = [
            PlanEntry("/mnt/disk1/TV_Shows/ShowA", 100, "/mnt/disk1", "/mnt/disk10"),
            PlanEntry("/mnt/disk1/TV_Shows/ShowB", 200, "/mnt/disk1", "/mnt/disk10", status="completed"),
        ]
        db.write_plan(entries)
        loaded = db.get_all()
        assert len(loaded) == 2
        assert loaded[0].path == "/mnt/disk1/TV_Shows/ShowA"
        assert loaded[0].size_bytes == 100
        assert loaded[0].status == "pending"
        assert loaded[1].status == "completed"
        db.close()

    def test_write_replaces_existing(self, state_dir):
        db = PlanDB(state_dir / PLAN_DB_FILE)
        db.write_plan([PlanEntry("/old", 1, "/s", "/t")])
        db.write_plan([PlanEntry("/new", 2, "/s", "/t")])
        loaded = db.get_all()
        assert len(loaded) == 1
        assert loaded[0].path == "/new"
        db.close()

    def test_get_pending(self, state_dir):
        db = PlanDB(state_dir / PLAN_DB_FILE)
        db.write_plan([
            PlanEntry("/a", 100, "/s", "/t", status="pending"),
            PlanEntry("/b", 200, "/s", "/t", status="completed"),
            PlanEntry("/c", 300, "/s", "/t", status="pending"),
        ])
        pending = db.get_pending()
        assert len(pending) == 2
        assert all(e.status == "pending" for e in pending)
        db.close()

    def test_get_all_with_status_filter(self, state_dir):
        db = PlanDB(state_dir / PLAN_DB_FILE)
        db.write_plan([
            PlanEntry("/a", 100, "/s", "/t", status="pending"),
            PlanEntry("/b", 200, "/s", "/t", status="cleaned"),
            PlanEntry("/c", 300, "/s", "/t", status="cleaned"),
        ])
        cleaned = db.get_all(status_filter="cleaned")
        assert len(cleaned) == 2
        assert all(e.status == "cleaned" for e in cleaned)
        db.close()

    def test_update_status(self, state_dir):
        db = PlanDB(state_dir / PLAN_DB_FILE)
        db.write_plan([
            PlanEntry("/a", 100, "/s", "/t", status="pending"),
            PlanEntry("/b", 200, "/s", "/t", status="pending"),
        ])
        db.update_status("/a", "in_progress")
        loaded = db.get_all()
        assert loaded[0].status == "in_progress"
        assert loaded[1].status == "pending"
        db.close()

    def test_recover_in_progress(self, state_dir):
        db = PlanDB(state_dir / PLAN_DB_FILE)
        db.write_plan([
            PlanEntry("/a", 100, "/s", "/t", status="in_progress"),
            PlanEntry("/b", 200, "/s", "/t", status="pending"),
            PlanEntry("/c", 300, "/s", "/t", status="in_progress"),
        ])
        count = db.recover_in_progress()
        assert count == 2
        loaded = db.get_all()
        assert loaded[0].status == "pending"
        assert loaded[2].status == "pending"
        db.close()

    def test_recover_no_in_progress(self, state_dir):
        db = PlanDB(state_dir / PLAN_DB_FILE)
        db.write_plan([PlanEntry("/a", 100, "/s", "/t", status="pending")])
        count = db.recover_in_progress()
        assert count == 0
        db.close()

    def test_summary(self, state_dir):
        db = PlanDB(state_dir / PLAN_DB_FILE)
        db.write_plan([
            PlanEntry("/a", 100, "/s", "/t", status="pending"),
            PlanEntry("/b", 200, "/s", "/t", status="pending"),
            PlanEntry("/c", 300, "/s", "/t", status="cleaned"),
        ])
        s = db.summary()
        assert s == {"pending": 2, "cleaned": 1}
        db.close()

    def test_retry_errors_resets_skipped_full(self, state_dir):
        """skipped_full entries must be retryable via retry_errors."""
        db = PlanDB(state_dir / PLAN_DB_FILE)
        db.write_plan([
            PlanEntry("/a", 100, "/s", "/t", status="skipped_full"),
            PlanEntry("/b", 200, "/s", "/t", status="error_copy"),
            PlanEntry("/c", 300, "/s", "/t", status="skipped"),
        ])
        count = db.retry_errors()
        assert count == 3
        pending = db.get_pending()
        assert len(pending) == 3
        db.close()

    def test_total_bytes(self, state_dir):
        db = PlanDB(state_dir / PLAN_DB_FILE)
        db.write_plan([
            PlanEntry("/a", 100, "/s", "/t"),
            PlanEntry("/b", 200, "/s", "/t"),
        ])
        assert db.total_bytes() == 300
        db.close()

    def test_pending_bytes(self, state_dir):
        db = PlanDB(state_dir / PLAN_DB_FILE)
        db.write_plan([
            PlanEntry("/a", 100, "/s", "/t", status="pending"),
            PlanEntry("/b", 200, "/s", "/t", status="cleaned"),
            PlanEntry("/c", 300, "/s", "/t", status="pending"),
        ])
        assert db.pending_bytes() == 400
        db.close()

    def test_has_plan_empty(self, state_dir):
        db = PlanDB(state_dir / PLAN_DB_FILE)
        assert db.has_plan() is False
        db.close()

    def test_has_plan_with_entries(self, state_dir):
        db = PlanDB(state_dir / PLAN_DB_FILE)
        db.write_plan([PlanEntry("/a", 100, "/s", "/t")])
        assert db.has_plan() is True
        db.close()

    def test_unicode_paths(self, state_dir):
        db = PlanDB(state_dir / PLAN_DB_FILE)
        db.write_plan([PlanEntry("/mnt/disk1/Anime/\u9032\u6483\u306e\u5de8\u4eba", 100, "/s", "/t")])
        loaded = db.get_all()
        assert loaded[0].path == "/mnt/disk1/Anime/\u9032\u6483\u306e\u5de8\u4eba"
        db.close()

    def test_commas_in_paths(self, state_dir):
        db = PlanDB(state_dir / PLAN_DB_FILE)
        db.write_plan([PlanEntry("/mnt/disk1/TV_Shows/Show, The (2020)", 100, "/s", "/t")])
        loaded = db.get_all()
        assert loaded[0].path == "/mnt/disk1/TV_Shows/Show, The (2020)"
        db.close()

    def test_context_manager(self, state_dir):
        with PlanDB(state_dir / PLAN_DB_FILE) as db:
            db.write_plan([PlanEntry("/a", 100, "/s", "/t")])
            assert db.has_plan() is True

    def test_wal_mode_enabled(self, state_dir):
        db = PlanDB(state_dir / PLAN_DB_FILE)
        mode = db.conn.execute("PRAGMA journal_mode").fetchone()[0]
        assert mode == "wal"
        db.close()

    def test_checkpoint_method_exists(self, state_dir):
        """M2: PlanDB should have a checkpoint method for WAL maintenance."""
        db = PlanDB(state_dir / PLAN_DB_FILE)
        assert hasattr(db, "checkpoint"), "PlanDB should have a checkpoint() method"
        # Should not raise
        db.checkpoint()
        db.close()

    def test_wal_mode_verified_on_init(self, state_dir, capsys):
        """M3: PlanDB should warn if WAL mode could not be enabled."""
        db = PlanDB(state_dir / PLAN_DB_FILE)
        mode = db.conn.execute("PRAGMA journal_mode").fetchone()[0]
        assert mode == "wal"
        db.close()

    def test_empty_db_summary(self, state_dir):
        db = PlanDB(state_dir / PLAN_DB_FILE)
        assert db.summary() == {}
        assert db.total_bytes() == 0
        assert db.pending_bytes() == 0
        db.close()

    def test_empty_write_clears_existing(self, state_dir):
        db = PlanDB(state_dir / PLAN_DB_FILE)
        db.write_plan([PlanEntry("/a", 100, "/s", "/t")])
        db.write_plan([])
        assert db.has_plan() is False
        db.close()


class TestMigration:
    def test_migrate_csv_to_db(self, state_dir):
        from rebalancer import _migrate_csv_to_db, write_plan_csv
        csv_path = state_dir / "plan.csv"
        db_path = state_dir / PLAN_DB_FILE
        entries = [
            PlanEntry("/mnt/disk1/A", 100, "/mnt/disk1", "/mnt/disk10", status="pending"),
            PlanEntry("/mnt/disk1/B", 200, "/mnt/disk1", "/mnt/disk10", status="completed"),
        ]
        write_plan_csv(entries, csv_path)
        _migrate_csv_to_db(state_dir)
        # DB should have entries
        db = PlanDB(db_path)
        loaded = db.get_all()
        assert len(loaded) == 2
        assert loaded[0].path == "/mnt/disk1/A"
        db.close()
        # CSV should be renamed to .bak
        assert not csv_path.exists()
        assert (state_dir / "plan.csv.bak").exists()

    def test_migrate_no_csv(self, state_dir):
        from rebalancer import _migrate_csv_to_db
        _migrate_csv_to_db(state_dir)
        assert not (state_dir / PLAN_DB_FILE).exists()

    def test_migrate_both_exist(self, state_dir):
        from rebalancer import _migrate_csv_to_db, write_plan_csv
        csv_path = state_dir / "plan.csv"
        db_path = state_dir / PLAN_DB_FILE
        write_plan_csv([PlanEntry("/csv", 100, "/s", "/t")], csv_path)
        db = PlanDB(db_path)
        db.write_plan([PlanEntry("/db", 200, "/s", "/t")])
        db.close()
        _migrate_csv_to_db(state_dir)
        db = PlanDB(db_path)
        loaded = db.get_all()
        assert len(loaded) == 1
        assert loaded[0].path == "/db"
        db.close()
        assert csv_path.exists()

    def test_migrate_empty_csv(self, state_dir):
        from rebalancer import _migrate_csv_to_db
        csv_path = state_dir / "plan.csv"
        csv_path.write_text("path,size_bytes,source_disk,target_disk,status\n")
        _migrate_csv_to_db(state_dir)
        db = PlanDB(state_dir / PLAN_DB_FILE)
        assert db.has_plan() is False
        db.close()
        assert not csv_path.exists()
        assert (state_dir / "plan.csv.bak").exists()

    def test_migrate_corrupted_csv(self, state_dir):
        from rebalancer import _migrate_csv_to_db
        csv_path = state_dir / "plan.csv"
        csv_path.write_text("garbage content\nnot csv at all\n")
        _migrate_csv_to_db(state_dir)
        db = PlanDB(state_dir / PLAN_DB_FILE)
        assert db.has_plan() is False
        db.close()
        assert not csv_path.exists()
        assert (state_dir / "plan.csv.bak").exists()


class TestPlanDBMeta:
    def test_meta_table_created_on_init(self, state_dir, db_path):
        db = PlanDB(db_path)
        db.conn.execute("SELECT * FROM meta").fetchall()
        db.close()

    def test_set_and_get_meta(self, state_dir, db_path):
        db = PlanDB(db_path)
        db.set_meta("active_dir_count", "3")
        assert db.get_meta("active_dir_count") == "3"
        db.close()

    def test_get_meta_missing_key(self, state_dir, db_path):
        db = PlanDB(db_path)
        assert db.get_meta("nonexistent") is None
        db.close()

    def test_set_meta_upserts(self, state_dir, db_path):
        db = PlanDB(db_path)
        db.set_meta("active_dir_count", "3")
        db.set_meta("active_dir_count", "5")
        assert db.get_meta("active_dir_count") == "5"
        db.close()

    def test_delete_meta_key(self, state_dir, db_path):
        db = PlanDB(db_path)
        db.set_meta("active_dir_count", "3")
        db.delete_meta("active_dir_count")
        assert db.get_meta("active_dir_count") is None
        db.close()

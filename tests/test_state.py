"""Tests for state management via PlanDB."""

import pytest

from rebalancer import PlanDB, PlanEntry, PLAN_DB_FILE


class TestRecoverInProgress:
    def test_resets_in_progress_to_pending(self, db_path):
        db = PlanDB(db_path)
        db.write_plan([
            PlanEntry("/mnt/disk1/A", 100, "/mnt/disk1", "/mnt/disk10", status="in_progress"),
            PlanEntry("/mnt/disk1/B", 200, "/mnt/disk1", "/mnt/disk10", status="pending"),
            PlanEntry("/mnt/disk1/C", 300, "/mnt/disk1", "/mnt/disk10", status="completed"),
        ])
        count = db.recover_in_progress()
        assert count == 1
        loaded = db.get_all()
        assert loaded[0].status == "pending"
        db.close()

    def test_no_in_progress_returns_zero(self, db_path):
        db = PlanDB(db_path)
        db.write_plan([
            PlanEntry("/mnt/disk1/A", 100, "/mnt/disk1", "/mnt/disk10", status="pending"),
            PlanEntry("/mnt/disk1/B", 200, "/mnt/disk1", "/mnt/disk10", status="completed"),
        ])
        count = db.recover_in_progress()
        assert count == 0
        db.close()

    def test_empty_db_returns_zero(self, db_path):
        db = PlanDB(db_path)
        count = db.recover_in_progress()
        assert count == 0
        db.close()

    def test_multiple_in_progress_all_recovered(self, db_path):
        db = PlanDB(db_path)
        db.write_plan([
            PlanEntry("/mnt/disk1/A", 100, "/mnt/disk1", "/mnt/disk10", status="in_progress"),
            PlanEntry("/mnt/disk1/B", 200, "/mnt/disk1", "/mnt/disk10", status="in_progress"),
        ])
        count = db.recover_in_progress()
        assert count == 2
        db.close()


class TestGetPendingEntries:
    def test_returns_only_pending(self, db_path):
        db = PlanDB(db_path)
        db.write_plan([
            PlanEntry("/mnt/disk1/A", 100, "/mnt/disk1", "/mnt/disk10", status="completed"),
            PlanEntry("/mnt/disk1/B", 200, "/mnt/disk1", "/mnt/disk10", status="pending"),
            PlanEntry("/mnt/disk1/C", 300, "/mnt/disk1", "/mnt/disk10", status="cleaned"),
            PlanEntry("/mnt/disk1/D", 400, "/mnt/disk1", "/mnt/disk10", status="pending"),
        ])
        pending = db.get_pending()
        assert len(pending) == 2
        assert pending[0].path == "/mnt/disk1/B"
        assert pending[1].path == "/mnt/disk1/D"
        db.close()

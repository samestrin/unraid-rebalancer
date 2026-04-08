"""Tests for throughput tracking and ETA display."""

import re

import pytest

from rebalancer import (
    PLAN_DB_FILE, PlanDB, PlanEntry, format_eta, format_plan_summary_db,
    _now_hms,
)


class TestThroughputDB:
    def test_table_created_on_init(self, state_dir):
        db = PlanDB(state_dir / PLAN_DB_FILE)
        tables = db.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='throughput'"
        ).fetchall()
        assert len(tables) == 1
        db.close()

    def test_record_stores_sample(self, state_dir):
        db = PlanDB(state_dir / PLAN_DB_FILE)
        db.record_throughput(1_000_000, 10.0)
        row = db.conn.execute("SELECT size_bytes, elapsed_seconds FROM throughput").fetchone()
        assert row["size_bytes"] == 1_000_000
        assert row["elapsed_seconds"] == 10.0
        db.close()

    def test_avg_no_samples_returns_none(self, state_dir):
        db = PlanDB(state_dir / PLAN_DB_FILE)
        assert db.avg_throughput() is None
        db.close()

    def test_avg_single_sample(self, state_dir):
        db = PlanDB(state_dir / PLAN_DB_FILE)
        db.record_throughput(1000, 10.0)
        assert db.avg_throughput() == pytest.approx(100.0)
        db.close()

    def test_avg_size_weighted(self, state_dir):
        """Size-weighted: sum(bytes) / sum(seconds), not average of rates."""
        db = PlanDB(state_dir / PLAN_DB_FILE)
        db.record_throughput(100, 1.0)    # 100 B/s
        db.record_throughput(900, 3.0)    # 300 B/s
        # Weighted: 1000 / 4.0 = 250 B/s (not simple avg of 200)
        assert db.avg_throughput() == pytest.approx(250.0)
        db.close()

    def test_fifo_keeps_20(self, state_dir):
        db = PlanDB(state_dir / PLAN_DB_FILE)
        for i in range(25):
            db.record_throughput(1000 * (i + 1), 10.0)
        assert db.throughput_sample_count() == 20
        db.close()

    def test_fifo_drops_oldest(self, state_dir):
        db = PlanDB(state_dir / PLAN_DB_FILE)
        for i in range(21):
            db.record_throughput(1000 * (i + 1), 10.0)
        # First sample (size=1000) should be gone
        row = db.conn.execute(
            "SELECT MIN(size_bytes) FROM throughput"
        ).fetchone()
        assert row[0] == 2000  # second sample is now the oldest
        db.close()

    def test_zero_elapsed_not_recorded(self, state_dir):
        db = PlanDB(state_dir / PLAN_DB_FILE)
        db.record_throughput(1000, 0.0)
        assert db.throughput_sample_count() == 0
        db.close()

    def test_negative_elapsed_not_recorded(self, state_dir):
        db = PlanDB(state_dir / PLAN_DB_FILE)
        db.record_throughput(1000, -1.0)
        assert db.throughput_sample_count() == 0
        db.close()

    def test_last_throughput_returns_most_recent(self, state_dir):
        db = PlanDB(state_dir / PLAN_DB_FILE)
        db.record_throughput(1000, 10.0)   # 100 B/s
        db.record_throughput(9000, 30.0)   # 300 B/s
        assert db.last_throughput() == pytest.approx(300.0)
        db.close()

    def test_last_throughput_no_samples(self, state_dir):
        db = PlanDB(state_dir / PLAN_DB_FILE)
        assert db.last_throughput() is None
        db.close()

    def test_persists_across_sessions(self, state_dir):
        db_path = state_dir / PLAN_DB_FILE
        db = PlanDB(db_path)
        db.record_throughput(5000, 10.0)
        db.close()

        db2 = PlanDB(db_path)
        assert db2.avg_throughput() == pytest.approx(500.0)
        assert db2.throughput_sample_count() == 1
        db2.close()


class TestCopyVerifyThroughputDB:
    def test_copy_table_created_on_init(self, state_dir):
        db = PlanDB(state_dir / PLAN_DB_FILE)
        tables = [r[0] for r in db.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '%throughput%'"
        ).fetchall()]
        assert "copy_throughput" in tables
        assert "verify_throughput" in tables
        db.close()

    def test_record_and_avg_copy(self, state_dir):
        db = PlanDB(state_dir / PLAN_DB_FILE)
        db.record_copy_throughput(1000, 10.0)
        assert db.avg_copy_throughput() == pytest.approx(100.0)
        db.close()

    def test_record_and_avg_verify(self, state_dir):
        db = PlanDB(state_dir / PLAN_DB_FILE)
        db.record_verify_throughput(2000, 5.0)
        assert db.avg_verify_throughput() == pytest.approx(400.0)
        db.close()

    def test_last_copy_throughput(self, state_dir):
        db = PlanDB(state_dir / PLAN_DB_FILE)
        db.record_copy_throughput(1000, 10.0)  # 100 B/s
        db.record_copy_throughput(3000, 10.0)  # 300 B/s
        assert db.last_copy_throughput() == pytest.approx(300.0)
        db.close()

    def test_copy_fifo_keeps_20(self, state_dir):
        db = PlanDB(state_dir / PLAN_DB_FILE)
        for i in range(25):
            db.record_copy_throughput(1000, 10.0)
        count = db.conn.execute("SELECT COUNT(*) FROM copy_throughput").fetchone()[0]
        assert count == 20
        db.close()

    def test_zero_elapsed_not_recorded_copy(self, state_dir):
        db = PlanDB(state_dir / PLAN_DB_FILE)
        db.record_copy_throughput(1000, 0.0)
        assert db.avg_copy_throughput() is None
        db.close()

    def test_no_samples_returns_none(self, state_dir):
        db = PlanDB(state_dir / PLAN_DB_FILE)
        assert db.avg_copy_throughput() is None
        assert db.avg_verify_throughput() is None
        assert db.last_copy_throughput() is None
        db.close()

    def test_copy_and_verify_independent(self, state_dir):
        """Copy and verify tables don't interfere with each other."""
        db = PlanDB(state_dir / PLAN_DB_FILE)
        db.record_copy_throughput(1000, 10.0)   # 100 B/s
        db.record_verify_throughput(2000, 5.0)  # 400 B/s
        assert db.avg_copy_throughput() == pytest.approx(100.0)
        assert db.avg_verify_throughput() == pytest.approx(400.0)
        db.close()


class TestFormatEta:
    def test_under_one_minute(self):
        assert format_eta(30) == "<1m"

    def test_minutes(self):
        assert format_eta(300) == "~5m"

    def test_hours_and_minutes(self):
        assert format_eta(5400) == "~1h 30m"

    def test_days_and_hours(self):
        assert format_eta(90000) == "~1d 1h"

    def test_zero_seconds(self):
        assert format_eta(0) == "<1m"

    def test_negative_seconds(self):
        assert format_eta(-5) == "<1m"

    def test_exactly_one_hour(self):
        assert format_eta(3600) == "~1h 0m"

    def test_large_value(self):
        assert format_eta(604800) == "~7d 0h"


class TestNowHms:
    def test_format_matches_pattern(self):
        result = _now_hms()
        assert re.match(r"\[\d{2}:\d{2}:\d{2}\]", result)


class TestTransferEtaIntegration:
    def test_session_eta_in_summary(self, state_dir):
        """Summary should show estimated time when throughput data exists."""
        db = PlanDB(state_dir / PLAN_DB_FILE)
        db.write_plan([
            PlanEntry("/mnt/disk1/TV/Show", 100_000_000_000, "/mnt/disk1", "/mnt/disk10"),
        ])
        db.record_throughput(50_000_000_000, 500.0)  # 100 MB/s
        output = format_plan_summary_db(db)
        assert "Estimated time:" in output
        db.close()

    def test_no_session_eta_without_samples(self, state_dir):
        """Summary should not show estimated time when no throughput data."""
        db = PlanDB(state_dir / PLAN_DB_FILE)
        db.write_plan([
            PlanEntry("/mnt/disk1/TV/Show", 100_000_000_000, "/mnt/disk1", "/mnt/disk10"),
        ])
        output = format_plan_summary_db(db)
        assert "Estimated time:" not in output
        db.close()

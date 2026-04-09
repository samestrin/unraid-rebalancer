"""Tests for plan generation — Phase 3 RED."""

import pytest

from rebalancer import (
    DiskInfo,
    MovableUnit,
    PlanEntry,
    classify_disks,
    generate_plan,
    STRATEGIES,
)


class TestClassifyDisks:
    def test_separates_over_and_under(self, sample_disks):
        over, under = classify_disks(sample_disks, max_used=80)
        over_paths = {d.path for d in over}
        under_paths = {d.path for d in under}
        # disk10 (51%) and disk11 (26%) should be under
        assert "/mnt/disk10" in under_paths
        assert "/mnt/disk11" in under_paths
        # All others (90-100%) should be over
        assert "/mnt/disk4" in over_paths
        assert "/mnt/disk6" in over_paths

    def test_overloaded_sorted_by_used_pct_desc(self, sample_disks):
        over, _ = classify_disks(sample_disks, max_used=80)
        pcts = [d.used_pct for d in over]
        assert pcts == sorted(pcts, reverse=True)

    def test_underloaded_sorted_by_used_pct_asc(self, sample_disks):
        _, under = classify_disks(sample_disks, max_used=80)
        pcts = [d.used_pct for d in under]
        assert pcts == sorted(pcts)

    def test_all_under_threshold(self):
        disks = [
            DiskInfo("/mnt/disk1", 1000, 500, 500, 50),
            DiskInfo("/mnt/disk2", 1000, 600, 400, 60),
        ]
        over, under = classify_disks(disks, max_used=80)
        assert over == []
        assert len(under) == 2

    def test_all_over_threshold(self):
        disks = [
            DiskInfo("/mnt/disk1", 1000, 900, 100, 90),
            DiskInfo("/mnt/disk2", 1000, 950, 50, 95),
        ]
        over, under = classify_disks(disks, max_used=80)
        assert len(over) == 2
        assert under == []

    def test_exact_threshold_is_not_overloaded(self):
        disks = [DiskInfo("/mnt/disk1", 1000, 800, 200, 80)]
        over, under = classify_disks(disks, max_used=80)
        assert over == []
        assert len(under) == 1


class TestGeneratePlan:
    def test_basic_plan_moves_from_full_to_empty(self):
        disks = [
            DiskInfo("/mnt/disk1", 1_000_000, 900_000, 100_000, 90),
            DiskInfo("/mnt/disk2", 1_000_000, 400_000, 600_000, 40),
        ]
        units = [
            MovableUnit("/mnt/disk1/TV_Shows/ShowA", "TV_Shows", "ShowA", 100_000, "/mnt/disk1"),
            MovableUnit("/mnt/disk1/TV_Shows/ShowB", "TV_Shows", "ShowB", 50_000, "/mnt/disk1"),
        ]
        over, under = classify_disks(disks, max_used=80)
        plan = generate_plan(units, over, under, strategy="fullest-first", max_used=80, min_free=0)
        assert len(plan) > 0
        for entry in plan:
            assert entry.source_disk == "/mnt/disk1"
            assert entry.target_disk == "/mnt/disk2"
            assert entry.status == "pending"

    def test_target_never_exceeds_max_used(self):
        """After assigning moves, no target disk should project above max_used."""
        disks = [
            DiskInfo("/mnt/disk1", 1_000_000, 950_000, 50_000, 95),
            DiskInfo("/mnt/disk2", 1_000_000, 750_000, 250_000, 75),
        ]
        units = [
            MovableUnit("/mnt/disk1/TV_Shows/ShowA", "TV_Shows", "ShowA", 200_000, "/mnt/disk1"),
            MovableUnit("/mnt/disk1/TV_Shows/ShowB", "TV_Shows", "ShowB", 100_000, "/mnt/disk1"),
        ]
        over, under = classify_disks(disks, max_used=80)
        plan = generate_plan(units, over, under, strategy="fullest-first", max_used=80, min_free=0)
        # disk2 can only absorb 50_000 before hitting 80% (800_000 - 750_000)
        # ShowA (200_000) would push it to 95% — must be skipped
        # ShowB (100_000) would push it to 85% — also skipped
        moved_bytes = sum(e.size_bytes for e in plan)
        projected = 750_000 + moved_bytes
        assert projected <= 800_000

    def test_skips_units_too_large_for_any_target(self):
        disks = [
            DiskInfo("/mnt/disk1", 1_000_000, 900_000, 100_000, 90),
            DiskInfo("/mnt/disk2", 1_000_000, 790_000, 210_000, 79),
        ]
        units = [
            MovableUnit("/mnt/disk1/Movies/2024", "Movies", "2024", 500_000, "/mnt/disk1"),
        ]
        over, under = classify_disks(disks, max_used=80)
        plan = generate_plan(units, over, under, strategy="fullest-first", max_used=80, min_free=0)
        # 500_000 > 10_000 available on disk2 before 80%
        assert len(plan) == 0

    def test_no_duplicate_entries(self):
        disks = [
            DiskInfo("/mnt/disk1", 1_000_000, 900_000, 100_000, 90),
            DiskInfo("/mnt/disk2", 1_000_000, 200_000, 800_000, 20),
        ]
        units = [
            MovableUnit("/mnt/disk1/TV_Shows/ShowA", "TV_Shows", "ShowA", 50_000, "/mnt/disk1"),
            MovableUnit("/mnt/disk1/TV_Shows/ShowB", "TV_Shows", "ShowB", 50_000, "/mnt/disk1"),
            MovableUnit("/mnt/disk1/TV_Shows/ShowC", "TV_Shows", "ShowC", 50_000, "/mnt/disk1"),
        ]
        over, under = classify_disks(disks, max_used=80)
        plan = generate_plan(units, over, under, strategy="fullest-first", max_used=80, min_free=0)
        paths = [e.path for e in plan]
        assert len(paths) == len(set(paths))

    def test_min_free_space_respected(self):
        disks = [
            DiskInfo("/mnt/disk1", 1_000_000, 900_000, 100_000, 90),
            DiskInfo("/mnt/disk2", 1_000_000, 500_000, 500_000, 50),
        ]
        units = [
            MovableUnit("/mnt/disk1/TV_Shows/Show", "TV_Shows", "Show", 100_000, "/mnt/disk1"),
        ]
        over, under = classify_disks(disks, max_used=80)
        # min_free = 450_000 means disk2 can only accept 50_000
        plan = generate_plan(units, over, under, strategy="fullest-first", max_used=80, min_free=450_000)
        # Unit is 100_000 but only 50_000 available with min_free
        assert len(plan) == 0

    def test_all_overloaded_still_makes_progress(self):
        """When all disks are over threshold, move from fullest to least-full."""
        disks = [
            DiskInfo("/mnt/disk1", 1_000_000, 950_000, 50_000, 95),
            DiskInfo("/mnt/disk2", 1_000_000, 850_000, 150_000, 85),
        ]
        units = [
            MovableUnit("/mnt/disk1/TV_Shows/ShowA", "TV_Shows", "ShowA", 50_000, "/mnt/disk1"),
        ]
        over, under = classify_disks(disks, max_used=80)
        # Both disks are over 80%, under is empty
        assert under == []
        plan = generate_plan(units, over, under, strategy="fullest-first", max_used=80, min_free=0)
        # Should still generate a plan: move from 95% disk to 85% disk
        assert len(plan) == 1
        assert plan[0].target_disk == "/mnt/disk2"

    def test_fullest_first_strategy(self):
        """fullest-first drains the fullest disk before moving to next."""
        disks = [
            DiskInfo("/mnt/disk1", 1_000_000, 900_000, 100_000, 90),
            DiskInfo("/mnt/disk2", 1_000_000, 850_000, 150_000, 85),
            DiskInfo("/mnt/disk3", 1_000_000, 200_000, 800_000, 20),
        ]
        units = [
            MovableUnit("/mnt/disk1/TV_Shows/A", "TV_Shows", "A", 50_000, "/mnt/disk1"),
            MovableUnit("/mnt/disk2/TV_Shows/B", "TV_Shows", "B", 50_000, "/mnt/disk2"),
        ]
        over, under = classify_disks(disks, max_used=80)
        plan = generate_plan(units, over, under, strategy="fullest-first", max_used=80, min_free=0)
        # disk1 (90%) should be drained before disk2 (85%)
        assert plan[0].source_disk == "/mnt/disk1"

    def test_largest_first_strategy(self):
        disks = [
            DiskInfo("/mnt/disk1", 1_000_000, 900_000, 100_000, 90),
            DiskInfo("/mnt/disk2", 1_000_000, 200_000, 800_000, 20),
        ]
        units = [
            MovableUnit("/mnt/disk1/TV_Shows/Small", "TV_Shows", "Small", 10_000, "/mnt/disk1"),
            MovableUnit("/mnt/disk1/TV_Shows/Large", "TV_Shows", "Large", 100_000, "/mnt/disk1"),
        ]
        over, under = classify_disks(disks, max_used=80)
        plan = generate_plan(units, over, under, strategy="largest-first", max_used=80, min_free=0)
        assert len(plan) >= 1
        assert plan[0].path == "/mnt/disk1/TV_Shows/Large"

    def test_smallest_first_strategy(self):
        disks = [
            DiskInfo("/mnt/disk1", 1_000_000, 900_000, 100_000, 90),
            DiskInfo("/mnt/disk2", 1_000_000, 200_000, 800_000, 20),
        ]
        units = [
            MovableUnit("/mnt/disk1/TV_Shows/Large", "TV_Shows", "Large", 100_000, "/mnt/disk1"),
            MovableUnit("/mnt/disk1/TV_Shows/Small", "TV_Shows", "Small", 10_000, "/mnt/disk1"),
        ]
        over, under = classify_disks(disks, max_used=80)
        plan = generate_plan(units, over, under, strategy="smallest-first", max_used=80, min_free=0)
        assert len(plan) >= 1
        assert plan[0].path == "/mnt/disk1/TV_Shows/Small"

    def test_empty_units_returns_empty_plan(self):
        disks = [
            DiskInfo("/mnt/disk1", 1_000_000, 900_000, 100_000, 90),
            DiskInfo("/mnt/disk2", 1_000_000, 200_000, 800_000, 20),
        ]
        over, under = classify_disks(disks, max_used=80)
        plan = generate_plan([], over, under, strategy="fullest-first", max_used=80, min_free=0)
        assert plan == []

    def test_only_moves_from_overloaded_disks(self):
        """Units on underloaded disks should not be moved."""
        disks = [
            DiskInfo("/mnt/disk1", 1_000_000, 900_000, 100_000, 90),
            DiskInfo("/mnt/disk2", 1_000_000, 200_000, 800_000, 20),
        ]
        units = [
            MovableUnit("/mnt/disk1/TV_Shows/A", "TV_Shows", "A", 50_000, "/mnt/disk1"),
            MovableUnit("/mnt/disk2/TV_Shows/B", "TV_Shows", "B", 50_000, "/mnt/disk2"),
        ]
        over, under = classify_disks(disks, max_used=80)
        plan = generate_plan(units, over, under, strategy="fullest-first", max_used=80, min_free=0)
        for entry in plan:
            assert entry.source_disk != "/mnt/disk2"

    def test_auto_strategy_picks_fewest_bytes(self):
        """Auto strategy should pick the plan that moves the fewest bytes."""
        disks = [
            DiskInfo("/mnt/disk1", 1_000_000, 900_000, 100_000, 90),
            DiskInfo("/mnt/disk2", 1_000_000, 200_000, 800_000, 20),
        ]
        units = [
            MovableUnit("/mnt/disk1/TV_Shows/Large", "TV_Shows", "Large", 100_000, "/mnt/disk1"),
            MovableUnit("/mnt/disk1/TV_Shows/Small", "TV_Shows", "Small", 10_000, "/mnt/disk1"),
        ]
        over, under = classify_disks(disks, max_used=80)
        plan = generate_plan(units, over, under, strategy="auto", max_used=80, min_free=0)
        assert len(plan) > 0
        # Auto should produce a valid plan
        for entry in plan:
            assert entry.status == "pending"

    def test_auto_strategy_respects_constraints(self):
        """Auto must still respect max_used and min_free constraints."""
        disks = [
            DiskInfo("/mnt/disk1", 1_000_000, 950_000, 50_000, 95),
            DiskInfo("/mnt/disk2", 1_000_000, 750_000, 250_000, 75),
        ]
        units = [
            MovableUnit("/mnt/disk1/TV_Shows/ShowA", "TV_Shows", "ShowA", 200_000, "/mnt/disk1"),
            MovableUnit("/mnt/disk1/TV_Shows/ShowB", "TV_Shows", "ShowB", 100_000, "/mnt/disk1"),
        ]
        over, under = classify_disks(disks, max_used=80)
        plan = generate_plan(units, over, under, strategy="auto", max_used=80, min_free=0)
        moved_bytes = sum(e.size_bytes for e in plan)
        projected = 750_000 + moved_bytes
        assert projected <= 800_000

    def test_auto_strategy_returns_same_as_best(self):
        """Auto should return the same plan as the best individual strategy."""
        disks = [
            DiskInfo("/mnt/disk1", 1_000_000, 900_000, 100_000, 90),
            DiskInfo("/mnt/disk2", 1_000_000, 850_000, 150_000, 85),
            DiskInfo("/mnt/disk3", 1_000_000, 200_000, 800_000, 20),
        ]
        units = [
            MovableUnit("/mnt/disk1/TV_Shows/A", "TV_Shows", "A", 50_000, "/mnt/disk1"),
            MovableUnit("/mnt/disk1/TV_Shows/B", "TV_Shows", "B", 30_000, "/mnt/disk1"),
            MovableUnit("/mnt/disk2/Anime/C", "Anime", "C", 40_000, "/mnt/disk2"),
        ]
        over, under = classify_disks(disks, max_used=80)

        # Generate plans for all concrete strategies
        plans = {}
        for s in STRATEGIES:
            plans[s] = generate_plan(units, over, under, strategy=s, max_used=80, min_free=0)

        auto_plan = generate_plan(units, over, under, strategy="auto", max_used=80, min_free=0)

        # Auto should match the strategy with fewest bytes
        best_bytes = min(sum(e.size_bytes for e in p) for p in plans.values() if p)
        auto_bytes = sum(e.size_bytes for e in auto_plan)
        assert auto_bytes == best_bytes

    def test_auto_with_empty_units(self):
        disks = [
            DiskInfo("/mnt/disk1", 1_000_000, 900_000, 100_000, 90),
            DiskInfo("/mnt/disk2", 1_000_000, 200_000, 800_000, 20),
        ]
        over, under = classify_disks(disks, max_used=80)
        plan = generate_plan([], over, under, strategy="auto", max_used=80, min_free=0)
        assert plan == []

    def test_strategies_constant_has_all_concrete(self):
        """STRATEGIES should contain exactly the three concrete strategies."""
        assert "fullest-first" in STRATEGIES
        assert "largest-first" in STRATEGIES
        assert "smallest-first" in STRATEGIES
        assert "auto" not in STRATEGIES

    def test_fills_lowest_usage_target_first(self):
        """When multiple targets exist, fill the lowest-usage one first."""
        disks = [
            DiskInfo("/mnt/disk1", 1_000_000, 900_000, 100_000, 90),
            DiskInfo("/mnt/disk2", 1_000_000, 500_000, 500_000, 50),
            DiskInfo("/mnt/disk3", 1_000_000, 300_000, 700_000, 30),
        ]
        units = [
            MovableUnit("/mnt/disk1/TV_Shows/A", "TV_Shows", "A", 50_000, "/mnt/disk1"),
        ]
        over, under = classify_disks(disks, max_used=80)
        plan = generate_plan(units, over, under, strategy="fullest-first", max_used=80, min_free=0)
        assert len(plan) == 1
        # Should target disk3 (30%) before disk2 (50%)
        assert plan[0].target_disk == "/mnt/disk3"

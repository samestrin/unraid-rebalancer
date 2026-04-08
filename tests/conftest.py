"""Shared test fixtures for unraid-rebalancer."""

from io import StringIO
from unittest.mock import MagicMock

import pytest

from rebalancer import DiskInfo, MovableUnit, PlanDB, PlanEntry, PLAN_DB_FILE


@pytest.fixture
def state_dir(tmp_path):
    """Temporary state directory acting as ~/.unraid-rebalancer/."""
    return tmp_path


@pytest.fixture
def db_path(state_dir):
    """Path for test plan database."""
    return state_dir / PLAN_DB_FILE


@pytest.fixture(autouse=True)
def mock_lock(monkeypatch):
    """Auto-mock the lock to prevent actual file locking in tests."""
    monkeypatch.setattr("rebalancer.acquire_lock", lambda _: MagicMock())
    monkeypatch.setattr("rebalancer.release_lock", lambda _: None)


@pytest.fixture
def sample_disks():
    """Realistic 11-disk Unraid array with imbalanced usage."""
    return [
        DiskInfo("/mnt/disk1", 16_000_000_000_000, 15_040_000_000_000, 960_000_000_000, 94),
        DiskInfo("/mnt/disk2", 16_000_000_000_000, 15_040_000_000_000, 960_000_000_000, 94),
        DiskInfo("/mnt/disk3", 16_000_000_000_000, 15_360_000_000_000, 640_000_000_000, 96),
        DiskInfo("/mnt/disk4", 16_000_000_000_000, 16_000_000_000_000, 0, 100),
        DiskInfo("/mnt/disk5", 16_000_000_000_000, 15_520_000_000_000, 480_000_000_000, 97),
        DiskInfo("/mnt/disk6", 16_000_000_000_000, 16_000_000_000_000, 0, 100),
        DiskInfo("/mnt/disk7", 16_000_000_000_000, 15_680_000_000_000, 320_000_000_000, 98),
        DiskInfo("/mnt/disk8", 16_000_000_000_000, 15_360_000_000_000, 640_000_000_000, 96),
        DiskInfo("/mnt/disk9", 16_000_000_000_000, 14_400_000_000_000, 1_600_000_000_000, 90),
        DiskInfo("/mnt/disk10", 16_000_000_000_000, 8_160_000_000_000, 7_840_000_000_000, 51),
        DiskInfo("/mnt/disk11", 16_000_000_000_000, 4_160_000_000_000, 11_840_000_000_000, 26),
    ]


@pytest.fixture
def sample_units():
    """Sample movable units across multiple disks and shares."""
    return [
        MovableUnit("/mnt/disk4/TV_Shows/Breaking Bad (2008)", "TV_Shows", "Breaking Bad (2008)", 200_000_000_000, "/mnt/disk4"),
        MovableUnit("/mnt/disk4/TV_Shows/The Wire (2002)", "TV_Shows", "The Wire (2002)", 150_000_000_000, "/mnt/disk4"),
        MovableUnit("/mnt/disk4/Movies/2024", "Movies", "2024", 500_000_000_000, "/mnt/disk4"),
        MovableUnit("/mnt/disk6/Anime/Naruto", "Anime", "Naruto", 300_000_000_000, "/mnt/disk6"),
        MovableUnit("/mnt/disk6/TV_Shows/Lost (2004)", "TV_Shows", "Lost (2004)", 180_000_000_000, "/mnt/disk6"),
        MovableUnit("/mnt/disk7/Donghua/Soul Land", "Donghua", "Soul Land", 80_000_000_000, "/mnt/disk7"),
        MovableUnit("/mnt/disk1/Education/Python Course", "Education", "Python Course", 50_000_000_000, "/mnt/disk1"),
        MovableUnit("/mnt/disk3/Anime/One Piece", "Anime", "One Piece", 400_000_000_000, "/mnt/disk3"),
    ]


@pytest.fixture
def sample_df_output():
    """Raw df output mimicking Unraid format."""
    return (
        "Filesystem      1K-blocks       Used  Available Use% Mounted on\n"
        "/dev/md1p1    15628677120 14880000000  748677120  96% /mnt/disk1\n"
        "/dev/md2p1    15628677120 14880000000  748677120  94% /mnt/disk2\n"
        "/dev/md10p1   15628677120  7968000000 7660677120  51% /mnt/disk10\n"
        "/dev/md11p1   15628677120  4064000000 11564677120  26% /mnt/disk11\n"
    )


@pytest.fixture
def sample_plan_csv():
    """Raw CSV plan content."""
    return (
        "path,size_bytes,source_disk,target_disk,status\n"
        "/mnt/disk4/TV_Shows/Breaking Bad (2008),200000000000,/mnt/disk4,/mnt/disk11,pending\n"
        "/mnt/disk4/Movies/2024,500000000000,/mnt/disk4,/mnt/disk11,completed\n"
        "/mnt/disk6/Anime/Naruto,300000000000,/mnt/disk6,/mnt/disk10,in_progress\n"
    )

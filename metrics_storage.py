#!/usr/bin/env python3
"""
SQLite-based metrics storage system for Unraid Rebalancer

Provides efficient, scalable storage for performance metrics with proper indexing,
transaction support, and data integrity for large-scale rebalancing operations.
"""

import logging
import sqlite3
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any, Union
import json
import os

# Database schema version for migrations
SCHEMA_VERSION = 1

# SQL schema definitions
SCHEMA_SQL = """
-- Schema version tracking
CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER PRIMARY KEY,
    applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Operations table - stores high-level operation information
CREATE TABLE IF NOT EXISTS operations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    operation_id TEXT UNIQUE NOT NULL,
    start_time REAL NOT NULL,
    end_time REAL,
    total_files INTEGER DEFAULT 0,
    completed_files INTEGER DEFAULT 0,
    failed_files INTEGER DEFAULT 0,
    total_bytes INTEGER DEFAULT 0,
    transferred_bytes INTEGER DEFAULT 0,
    average_transfer_rate_bps REAL DEFAULT 0.0,
    peak_transfer_rate_bps REAL DEFAULT 0.0,
    rsync_mode TEXT DEFAULT 'fast',
    success_rate REAL DEFAULT 0.0,
    duration_seconds REAL,
    overall_transfer_rate_mbps REAL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Transfers table - stores individual file/directory transfer metrics
CREATE TABLE IF NOT EXISTS transfers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    operation_id TEXT NOT NULL,
    unit_path TEXT NOT NULL,
    src_disk TEXT NOT NULL,
    dest_disk TEXT NOT NULL,
    size_bytes INTEGER NOT NULL,
    start_time REAL NOT NULL,
    end_time REAL,
    success BOOLEAN DEFAULT 0,
    error_message TEXT,
    transfer_rate_bps REAL,
    transfer_rate_mbps REAL,
    duration_seconds REAL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (operation_id) REFERENCES operations(operation_id) ON DELETE CASCADE
);

-- System metrics table - stores system resource usage over time
CREATE TABLE IF NOT EXISTS system_metrics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    operation_id TEXT NOT NULL,
    timestamp REAL NOT NULL,
    cpu_percent REAL NOT NULL,
    memory_percent REAL NOT NULL,
    disk_io_read_bps REAL NOT NULL,
    disk_io_write_bps REAL NOT NULL,
    network_sent_bps REAL DEFAULT 0.0,
    network_recv_bps REAL DEFAULT 0.0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (operation_id) REFERENCES operations(operation_id) ON DELETE CASCADE
);

-- Operation errors table - stores detailed error information
CREATE TABLE IF NOT EXISTS operation_errors (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    operation_id TEXT NOT NULL,
    error_message TEXT NOT NULL,
    error_type TEXT,
    timestamp REAL NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (operation_id) REFERENCES operations(operation_id) ON DELETE CASCADE
);

-- Performance indexes for efficient querying
CREATE INDEX IF NOT EXISTS idx_operations_start_time ON operations(start_time);
CREATE INDEX IF NOT EXISTS idx_operations_operation_id ON operations(operation_id);
CREATE INDEX IF NOT EXISTS idx_operations_rsync_mode ON operations(rsync_mode);
CREATE INDEX IF NOT EXISTS idx_operations_created_at ON operations(created_at);

CREATE INDEX IF NOT EXISTS idx_transfers_operation_id ON transfers(operation_id);
CREATE INDEX IF NOT EXISTS idx_transfers_start_time ON transfers(start_time);
CREATE INDEX IF NOT EXISTS idx_transfers_src_disk ON transfers(src_disk);
CREATE INDEX IF NOT EXISTS idx_transfers_dest_disk ON transfers(dest_disk);
CREATE INDEX IF NOT EXISTS idx_transfers_success ON transfers(success);
CREATE INDEX IF NOT EXISTS idx_transfers_size_bytes ON transfers(size_bytes);

CREATE INDEX IF NOT EXISTS idx_system_metrics_operation_id ON system_metrics(operation_id);
CREATE INDEX IF NOT EXISTS idx_system_metrics_timestamp ON system_metrics(timestamp);

CREATE INDEX IF NOT EXISTS idx_operation_errors_operation_id ON operation_errors(operation_id);
CREATE INDEX IF NOT EXISTS idx_operation_errors_timestamp ON operation_errors(timestamp);

-- Insert initial schema version
INSERT OR IGNORE INTO schema_version (version) VALUES (1);
"""


class MetricsDatabase:
    """SQLite-based metrics storage with connection pooling and transaction support."""
    
    def __init__(self, db_path: Union[str, Path], max_connections: int = 10):
        self.db_path = Path(db_path)
        self.max_connections = max_connections
        self._connections: List[sqlite3.Connection] = []
        self._connection_lock = threading.Lock()
        self._local = threading.local()
        
        # Prepared statements for frequent operations
        self._prepared_statements = {
            'insert_operation': """
                INSERT INTO operations (
                    operation_id, start_time, end_time, total_files, completed_files,
                    failed_files, total_bytes, transferred_bytes, average_transfer_rate_bps,
                    peak_transfer_rate_bps, rsync_mode, success_rate, duration_seconds,
                    overall_transfer_rate_mbps
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            'insert_transfer': """
                INSERT INTO transfers (
                    operation_id, unit_path, src_disk, dest_disk, size_bytes,
                    start_time, end_time, success, error_message, transfer_rate_bps,
                    transfer_rate_mbps, duration_seconds
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            'insert_system_metric': """
                INSERT INTO system_metrics (
                    operation_id, timestamp, cpu_percent, memory_percent,
                    disk_io_read_bps, disk_io_write_bps, network_sent_bps, network_recv_bps
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            'get_operation': "SELECT * FROM operations WHERE operation_id = ?",
            'get_transfers': "SELECT * FROM transfers WHERE operation_id = ? ORDER BY start_time",
            'get_system_metrics': "SELECT * FROM system_metrics WHERE operation_id = ? ORDER BY timestamp"
        }
        
        # Ensure database directory exists
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Initialize database schema
        self._initialize_database()
        
        # Configure database for performance
        self._configure_database()
    
    def _initialize_database(self):
        """Initialize database schema and apply migrations."""
        with self._get_connection() as conn:
            # Enable foreign keys
            conn.execute("PRAGMA foreign_keys = ON")
            
            # Create schema
            conn.executescript(SCHEMA_SQL)
            
            # Check if we need to run migrations
            current_version = self._get_schema_version(conn)
            if current_version < SCHEMA_VERSION:
                self._run_migrations(conn, current_version)
                
            conn.commit()
            logging.info(f"Database initialized at {self.db_path}")
    
    def _configure_database(self):
        """Configure database for optimal performance."""
        with self._get_connection() as conn:
            # Performance optimizations
            conn.execute("PRAGMA journal_mode = WAL")  # Better concurrency
            conn.execute("PRAGMA synchronous = NORMAL")  # Good balance of safety/speed
            conn.execute("PRAGMA cache_size = -64000")  # 64MB cache
            conn.execute("PRAGMA temp_store = MEMORY")  # Temporary tables in memory
            conn.execute("PRAGMA mmap_size = 268435456")  # 256MB memory mapping
            
            conn.commit()
    
    def _get_schema_version(self, conn: sqlite3.Connection) -> int:
        """Get current database schema version."""
        try:
            result = conn.execute("SELECT MAX(version) FROM schema_version").fetchone()
            return result[0] if result and result[0] is not None else 0
        except sqlite3.OperationalError:
            return 0
    
    def _run_migrations(self, conn: sqlite3.Connection, from_version: int):
        """Run database migrations from specified version."""
        # Future migrations would go here
        logging.info(f"Database migrations complete: {from_version} -> {SCHEMA_VERSION}")
        
        # Update schema version
        conn.execute(
            "INSERT INTO schema_version (version) VALUES (?)",
            (SCHEMA_VERSION,)
        )
    
    @contextmanager
    def _get_connection(self):
        """Get a database connection with automatic cleanup."""
        # Check if we have a connection for this thread
        if not hasattr(self._local, 'connection') or self._local.connection is None:
            with self._connection_lock:
                if len(self._connections) > 0:
                    conn = self._connections.pop()
                else:
                    conn = sqlite3.connect(
                        str(self.db_path),
                        timeout=30.0,
                        check_same_thread=False
                    )
                    conn.row_factory = sqlite3.Row  # Enable column access by name
                
                self._local.connection = conn
        
        try:
            yield self._local.connection
        except Exception as e:
            logging.error(f"Database error: {e}")
            # Rollback any pending transaction
            try:
                self._local.connection.rollback()
            except:
                pass
            raise
        finally:
            # Return connection to pool if healthy
            if self._local.connection:
                with self._connection_lock:
                    if len(self._connections) < self.max_connections:
                        self._connections.append(self._local.connection)
                    else:
                        self._local.connection.close()
                self._local.connection = None
    
    def store_operation(self, operation_data: Dict[str, Any]) -> int:
        """Store operation metrics and return the database row ID."""
        with self._get_connection() as conn:
            cursor = conn.execute("""
                INSERT INTO operations (
                    operation_id, start_time, end_time, total_files, completed_files,
                    failed_files, total_bytes, transferred_bytes, average_transfer_rate_bps,
                    peak_transfer_rate_bps, rsync_mode, success_rate, duration_seconds,
                    overall_transfer_rate_mbps
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                operation_data['operation_id'],
                operation_data['start_time'],
                operation_data.get('end_time'),
                operation_data.get('total_files', 0),
                operation_data.get('completed_files', 0),
                operation_data.get('failed_files', 0),
                operation_data.get('total_bytes', 0),
                operation_data.get('transferred_bytes', 0),
                operation_data.get('average_transfer_rate_bps', 0.0),
                operation_data.get('peak_transfer_rate_bps', 0.0),
                operation_data.get('rsync_mode', 'fast'),
                operation_data.get('success_rate', 0.0),
                operation_data.get('duration_seconds'),
                operation_data.get('overall_transfer_rate_mbps')
            ))
            conn.commit()
            return cursor.lastrowid
    
    def update_operation(self, operation_id: str, operation_data: Dict[str, Any]):
        """Update existing operation metrics."""
        # Build dynamic update query based on provided data
        set_clauses = []
        values = []
        
        for field in ['end_time', 'total_files', 'completed_files', 'failed_files',
                      'total_bytes', 'transferred_bytes', 'average_transfer_rate_bps',
                      'peak_transfer_rate_bps', 'success_rate', 'duration_seconds',
                      'overall_transfer_rate_mbps']:
            if field in operation_data:
                set_clauses.append(f"{field} = ?")
                values.append(operation_data[field])
        
        if not set_clauses:
            return
        
        # Always update the updated_at timestamp
        set_clauses.append("updated_at = CURRENT_TIMESTAMP")
        values.append(operation_id)
        
        query = f"UPDATE operations SET {', '.join(set_clauses)} WHERE operation_id = ?"
        
        with self._get_connection() as conn:
            conn.execute(query, values)
            conn.commit()
    
    def store_transfer(self, transfer_data: Dict[str, Any]) -> int:
        """Store transfer metrics and return the database row ID."""
        with self._get_connection() as conn:
            cursor = conn.execute("""
                INSERT INTO transfers (
                    operation_id, unit_path, src_disk, dest_disk, size_bytes,
                    start_time, end_time, success, error_message, transfer_rate_bps,
                    transfer_rate_mbps, duration_seconds
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                transfer_data['operation_id'],
                transfer_data['unit_path'],
                transfer_data['src_disk'],
                transfer_data['dest_disk'],
                transfer_data['size_bytes'],
                transfer_data['start_time'],
                transfer_data.get('end_time'),
                transfer_data.get('success', False),
                transfer_data.get('error_message'),
                transfer_data.get('transfer_rate_bps'),
                transfer_data.get('transfer_rate_mbps'),
                transfer_data.get('duration_seconds')
            ))
            conn.commit()
            return cursor.lastrowid
    
    def store_system_metric(self, metric_data: Dict[str, Any]) -> int:
        """Store system metrics and return the database row ID."""
        with self._get_connection() as conn:
            cursor = conn.execute("""
                INSERT INTO system_metrics (
                    operation_id, timestamp, cpu_percent, memory_percent,
                    disk_io_read_bps, disk_io_write_bps, network_sent_bps, network_recv_bps
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                metric_data['operation_id'],
                metric_data['timestamp'],
                metric_data['cpu_percent'],
                metric_data['memory_percent'],
                metric_data['disk_io_read_bps'],
                metric_data['disk_io_write_bps'],
                metric_data.get('network_sent_bps', 0.0),
                metric_data.get('network_recv_bps', 0.0)
            ))
            conn.commit()
            return cursor.lastrowid
    
    def store_error(self, operation_id: str, error_message: str, error_type: str = None, timestamp: float = None) -> int:
        """Store operation error and return the database row ID."""
        if timestamp is None:
            timestamp = time.time()
        
        with self._get_connection() as conn:
            cursor = conn.execute("""
                INSERT INTO operation_errors (operation_id, error_message, error_type, timestamp)
                VALUES (?, ?, ?, ?)
            """, (operation_id, error_message, error_type, timestamp))
            conn.commit()
            return cursor.lastrowid
    
    def get_operation(self, operation_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve operation data by operation ID."""
        with self._get_connection() as conn:
            row = conn.execute(
                "SELECT * FROM operations WHERE operation_id = ?",
                (operation_id,)
            ).fetchone()
            
            if not row:
                return None
            
            return dict(row)
    
    def get_operations(self, limit: int = 100, offset: int = 0, 
                      start_time: float = None, end_time: float = None) -> List[Dict[str, Any]]:
        """Retrieve multiple operations with optional filtering."""
        query = "SELECT * FROM operations"
        conditions = []
        params = []
        
        if start_time is not None:
            conditions.append("start_time >= ?")
            params.append(start_time)
        
        if end_time is not None:
            conditions.append("start_time <= ?")
            params.append(end_time)
        
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        
        query += " ORDER BY start_time DESC LIMIT ? OFFSET ?"
        params.extend([limit, offset])
        
        with self._get_connection() as conn:
            rows = conn.execute(query, params).fetchall()
            return [dict(row) for row in rows]
    
    def get_transfers(self, operation_id: str) -> List[Dict[str, Any]]:
        """Retrieve all transfers for a specific operation."""
        with self._get_connection() as conn:
            rows = conn.execute(
                "SELECT * FROM transfers WHERE operation_id = ? ORDER BY start_time",
                (operation_id,)
            ).fetchall()
            return [dict(row) for row in rows]
    
    def get_system_metrics(self, operation_id: str) -> List[Dict[str, Any]]:
        """Retrieve all system metrics for a specific operation."""
        with self._get_connection() as conn:
            rows = conn.execute(
                "SELECT * FROM system_metrics WHERE operation_id = ? ORDER BY timestamp",
                (operation_id,)
            ).fetchall()
            return [dict(row) for row in rows]
    
    def get_operation_errors(self, operation_id: str) -> List[Dict[str, Any]]:
        """Retrieve all errors for a specific operation."""
        with self._get_connection() as conn:
            rows = conn.execute(
                "SELECT * FROM operation_errors WHERE operation_id = ? ORDER BY timestamp",
                (operation_id,)
            ).fetchall()
            return [dict(row) for row in rows]

    def get_incomplete_transfers(self, operation_id: str) -> List[Dict[str, Any]]:
        """Retrieve incomplete transfers (end_time is NULL) for a specific operation."""
        with self._get_connection() as conn:
            rows = conn.execute(
                "SELECT * FROM transfers WHERE operation_id = ? AND end_time IS NULL ORDER BY start_time",
                (operation_id,)
            ).fetchall()
            return [dict(row) for row in rows]

    def update_transfer(self, operation_id: str, unit_path: str, transfer_data: Dict[str, Any]) -> None:
        """Update an existing transfer record."""
        with self._get_connection() as conn:
            # Build the SET clause dynamically based on provided data
            set_clauses = []
            params = []

            for key, value in transfer_data.items():
                if key in ['end_time', 'success', 'error_message', 'transfer_rate_bps', 'transfer_rate_mbps', 'duration_seconds']:
                    set_clauses.append(f"{key} = ?")
                    params.append(value)

            if set_clauses:
                query = f"UPDATE transfers SET {', '.join(set_clauses)} WHERE operation_id = ? AND unit_path = ?"
                params.extend([operation_id, unit_path])
                conn.execute(query, params)

    def delete_old_data(self, days: int = 30) -> int:
        """Delete data older than specified days. Returns number of operations deleted."""
        cutoff_timestamp = time.time() - (days * 24 * 60 * 60)
        
        with self._get_connection() as conn:
            # Get count before deletion
            count = conn.execute(
                "SELECT COUNT(*) FROM operations WHERE start_time < ?",
                (cutoff_timestamp,)
            ).fetchone()[0]
            
            # Delete old operations (cascades to related tables)
            conn.execute(
                "DELETE FROM operations WHERE start_time < ?",
                (cutoff_timestamp,)
            )
            
            conn.commit()
            logging.info(f"Deleted {count} operations older than {days} days")
            return count
    
    def apply_retention_policy(self, retention_config: Dict[str, int]) -> Dict[str, int]:
        """Apply configurable retention policies for different data types.
        
        Args:
            retention_config: Dict with keys like 'operations', 'system_metrics', 'errors'
                             and values as days to retain
        
        Returns:
            Dict with deletion counts for each data type
        """
        results = {}
        current_time = time.time()
        
        with self._get_connection() as conn:
            # Delete old operations (cascades to transfers)
            if 'operations' in retention_config:
                days = retention_config['operations']
                cutoff = current_time - (days * 24 * 60 * 60)
                
                count = conn.execute(
                    "SELECT COUNT(*) FROM operations WHERE start_time < ?",
                    (cutoff,)
                ).fetchone()[0]
                
                conn.execute(
                    "DELETE FROM operations WHERE start_time < ?",
                    (cutoff,)
                )
                results['operations'] = count
                logging.info(f"Deleted {count} operations older than {days} days")
            
            # Delete old system metrics (more granular retention)
            if 'system_metrics' in retention_config:
                days = retention_config['system_metrics']
                cutoff = current_time - (days * 24 * 60 * 60)
                
                count = conn.execute(
                    "SELECT COUNT(*) FROM system_metrics WHERE timestamp < ?",
                    (cutoff,)
                ).fetchone()[0]
                
                conn.execute(
                    "DELETE FROM system_metrics WHERE timestamp < ?",
                    (cutoff,)
                )
                results['system_metrics'] = count
                logging.info(f"Deleted {count} system metrics older than {days} days")
            
            # Delete old errors (separate retention policy)
            if 'errors' in retention_config:
                days = retention_config['errors']
                cutoff = current_time - (days * 24 * 60 * 60)
                
                count = conn.execute(
                    "SELECT COUNT(*) FROM operation_errors WHERE timestamp < ?",
                    (cutoff,)
                ).fetchone()[0]
                
                conn.execute(
                    "DELETE FROM operation_errors WHERE timestamp < ?",
                    (cutoff,)
                )
                results['errors'] = count
                logging.info(f"Deleted {count} error records older than {days} days")
            
            conn.commit()
        
        return results
    
    def compress_old_system_metrics(self, days_threshold: int = 7, sample_rate: int = 10) -> int:
        """Compress old system metrics by keeping only every Nth sample.
        
        Args:
            days_threshold: Compress metrics older than this many days
            sample_rate: Keep 1 out of every N samples (e.g., 10 = keep 10%)
        
        Returns:
            Number of metrics deleted during compression
        """
        cutoff_timestamp = time.time() - (days_threshold * 24 * 60 * 60)
        
        with self._get_connection() as conn:
            # Get old metrics grouped by operation_id
            old_metrics = conn.execute("""
                SELECT id, operation_id, timestamp, 
                       ROW_NUMBER() OVER (PARTITION BY operation_id ORDER BY timestamp) as row_num
                FROM system_metrics 
                WHERE timestamp < ?
                ORDER BY operation_id, timestamp
            """, (cutoff_timestamp,)).fetchall()
            
            # Delete all but every Nth sample
            ids_to_delete = []
            for metric in old_metrics:
                if metric['row_num'] % sample_rate != 0:
                    ids_to_delete.append(metric['id'])
            
            if ids_to_delete:
                # Delete in batches to avoid SQL limits
                batch_size = 1000
                deleted_count = 0
                
                for i in range(0, len(ids_to_delete), batch_size):
                    batch = ids_to_delete[i:i + batch_size]
                    placeholders = ','.join('?' * len(batch))
                    
                    conn.execute(
                        f"DELETE FROM system_metrics WHERE id IN ({placeholders})",
                        batch
                    )
                    deleted_count += len(batch)
                
                conn.commit()
                logging.info(f"Compressed system metrics: deleted {deleted_count} samples, kept {len(old_metrics) - deleted_count}")
                return deleted_count
            
            return 0
    
    def vacuum_database(self):
        """Optimize database by reclaiming space and updating statistics."""
        with self._get_connection() as conn:
            conn.execute("VACUUM")
            conn.execute("ANALYZE")
            conn.commit()
            logging.info("Database maintenance completed (VACUUM and ANALYZE)")
    
    def get_database_stats(self) -> Dict[str, Any]:
        """Get database statistics for monitoring."""
        with self._get_connection() as conn:
            stats = {}
            
            # Table counts
            for table in ['operations', 'transfers', 'system_metrics', 'operation_errors']:
                count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                stats[f"{table}_count"] = count
            
            # Database size
            stats['database_size_bytes'] = self.db_path.stat().st_size if self.db_path.exists() else 0
            
            # Date range
            date_range = conn.execute(
                "SELECT MIN(start_time), MAX(start_time) FROM operations"
            ).fetchone()
            
            if date_range and date_range[0]:
                stats['earliest_operation'] = date_range[0]
                stats['latest_operation'] = date_range[1]
                stats['data_span_days'] = (date_range[1] - date_range[0]) / (24 * 60 * 60)
            
            return stats
    
    # Advanced query capabilities
    def get_performance_trends(self, days: int = 30) -> Dict[str, Any]:
        """Get performance trend analysis over specified period."""
        cutoff_timestamp = time.time() - (days * 24 * 60 * 60)
        
        with self._get_connection() as conn:
            # Overall transfer rate trends
            transfer_trends = conn.execute("""
                SELECT 
                    DATE(start_time, 'unixepoch') as date,
                    AVG(overall_transfer_rate_mbps) as avg_rate,
                    MAX(overall_transfer_rate_mbps) as max_rate,
                    COUNT(*) as operation_count,
                    AVG(success_rate) as avg_success_rate
                FROM operations 
                WHERE start_time >= ? AND overall_transfer_rate_mbps IS NOT NULL
                GROUP BY DATE(start_time, 'unixepoch')
                ORDER BY date
            """, (cutoff_timestamp,)).fetchall()
            
            # Resource utilization trends
            resource_trends = conn.execute("""
                SELECT 
                    DATE(timestamp, 'unixepoch') as date,
                    AVG(cpu_percent) as avg_cpu,
                    AVG(memory_percent) as avg_memory,
                    AVG(disk_io_read_bps + disk_io_write_bps) as avg_disk_io
                FROM system_metrics 
                WHERE timestamp >= ?
                GROUP BY DATE(timestamp, 'unixepoch')
                ORDER BY date
            """, (cutoff_timestamp,)).fetchall()
            
            return {
                'transfer_trends': [dict(row) for row in transfer_trends],
                'resource_trends': [dict(row) for row in resource_trends],
                'analysis_period_days': days
            }
    
    def get_disk_performance_comparison(self, days: int = 30) -> Dict[str, Any]:
        """Compare performance across different source/destination disks."""
        cutoff_timestamp = time.time() - (days * 24 * 60 * 60)
        
        with self._get_connection() as conn:
            # Source disk performance
            src_performance = conn.execute("""
                SELECT 
                    src_disk,
                    COUNT(*) as transfer_count,
                    AVG(transfer_rate_mbps) as avg_rate,
                    MAX(transfer_rate_mbps) as max_rate,
                    AVG(size_bytes) as avg_size,
                    SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as success_rate
                FROM transfers 
                WHERE start_time >= ? AND transfer_rate_mbps IS NOT NULL
                GROUP BY src_disk
                ORDER BY avg_rate DESC
            """, (cutoff_timestamp,)).fetchall()
            
            # Destination disk performance
            dest_performance = conn.execute("""
                SELECT 
                    dest_disk,
                    COUNT(*) as transfer_count,
                    AVG(transfer_rate_mbps) as avg_rate,
                    MAX(transfer_rate_mbps) as max_rate,
                    AVG(size_bytes) as avg_size,
                    SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as success_rate
                FROM transfers 
                WHERE start_time >= ? AND transfer_rate_mbps IS NOT NULL
                GROUP BY dest_disk
                ORDER BY avg_rate DESC
            """, (cutoff_timestamp,)).fetchall()
            
            return {
                'source_disk_performance': [dict(row) for row in src_performance],
                'destination_disk_performance': [dict(row) for row in dest_performance],
                'analysis_period_days': days
            }
    
    def get_rsync_mode_comparison(self, days: int = 30) -> Dict[str, Any]:
        """Compare performance across different rsync modes."""
        cutoff_timestamp = time.time() - (days * 24 * 60 * 60)
        
        with self._get_connection() as conn:
            mode_comparison = conn.execute("""
                SELECT 
                    rsync_mode,
                    COUNT(*) as operation_count,
                    AVG(overall_transfer_rate_mbps) as avg_transfer_rate,
                    AVG(success_rate) as avg_success_rate,
                    AVG(duration_seconds) as avg_duration,
                    AVG(total_bytes) as avg_total_bytes,
                    -- System resource usage correlation
                    (SELECT AVG(cpu_percent) FROM system_metrics sm 
                     WHERE sm.operation_id = o.operation_id) as avg_cpu_usage,
                    (SELECT AVG(memory_percent) FROM system_metrics sm 
                     WHERE sm.operation_id = o.operation_id) as avg_memory_usage
                FROM operations o
                WHERE start_time >= ? AND overall_transfer_rate_mbps IS NOT NULL
                GROUP BY rsync_mode
                ORDER BY avg_transfer_rate DESC
            """, (cutoff_timestamp,)).fetchall()
            
            return {
                'mode_comparison': [dict(row) for row in mode_comparison],
                'analysis_period_days': days
            }
    
    def get_operation_correlations(self, operation_id: str) -> Dict[str, Any]:
        """Get detailed correlations between metrics for a specific operation."""
        with self._get_connection() as conn:
            # Get operation details
            operation = self.get_operation(operation_id)
            if not operation:
                return {}
            
            # Get transfer performance correlation with system resources
            correlation_data = conn.execute("""
                SELECT 
                    t.start_time,
                    t.transfer_rate_mbps,
                    t.size_bytes,
                    sm.cpu_percent,
                    sm.memory_percent,
                    sm.disk_io_read_bps + sm.disk_io_write_bps as total_disk_io
                FROM transfers t
                LEFT JOIN system_metrics sm ON 
                    sm.operation_id = t.operation_id AND 
                    ABS(sm.timestamp - t.start_time) < 10  -- Within 10 seconds
                WHERE t.operation_id = ? AND t.transfer_rate_mbps IS NOT NULL
                ORDER BY t.start_time
            """, (operation_id,)).fetchall()
            
            # Calculate basic correlations (simplified)
            correlations = {}
            if correlation_data:
                data_dicts = [dict(row) for row in correlation_data]
                
                # Extract numeric values for correlation calculation
                transfer_rates = [d['transfer_rate_mbps'] for d in data_dicts if d['transfer_rate_mbps']]
                cpu_values = [d['cpu_percent'] for d in data_dicts if d['cpu_percent']]
                
                if len(transfer_rates) > 1 and len(cpu_values) > 1:
                    # Simple correlation indicator (not true Pearson correlation)
                    correlations['transfer_rate_vs_cpu'] = {
                        'data_points': len(transfer_rates),
                        'avg_transfer_rate': sum(transfer_rates) / len(transfer_rates),
                        'avg_cpu_usage': sum(cpu_values) / len(cpu_values)
                    }
            
            return {
                'operation_id': operation_id,
                'correlation_data': [dict(row) for row in correlation_data],
                'correlations': correlations
            }
    
    def close(self):
        """Close all database connections."""
        with self._connection_lock:
            for conn in self._connections:
                conn.close()
            self._connections.clear()
        
        if hasattr(self._local, 'connection') and self._local.connection:
            self._local.connection.close()
            self._local.connection = None
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
    
    def backup_database(self, backup_path: Union[str, Path]) -> bool:
        """Create a backup copy of the database."""
        backup_path = Path(backup_path)
        
        try:
            if not self.db_path.exists():
                logging.warning(f"Database file {self.db_path} does not exist")
                return False
            
            # Ensure backup directory exists
            backup_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Use SQLite backup API for consistent backup
            with self._get_connection() as source_conn:
                backup_conn = sqlite3.connect(str(backup_path))
                source_conn.backup(backup_conn)
                backup_conn.close()
            
            logging.info(f"Database backed up to {backup_path}")
            return True
            
        except Exception as e:
            logging.error(f"Failed to backup database: {e}")
            return False
    
    def verify_database_integrity(self) -> Dict[str, Any]:
        """Verify database integrity and return status report."""
        integrity_report = {
            'integrity_check': False,
            'foreign_key_check': False,
            'schema_version_valid': False,
            'table_counts': {},
            'issues': []
        }
        
        try:
            with self._get_connection() as conn:
                # Integrity check
                try:
                    result = conn.execute("PRAGMA integrity_check").fetchone()
                    integrity_report['integrity_check'] = result[0] == 'ok'
                    if result[0] != 'ok':
                        integrity_report['issues'].append(f"Integrity check failed: {result[0]}")
                except Exception as e:
                    integrity_report['issues'].append(f"Integrity check error: {e}")
                
                # Foreign key check
                try:
                    fk_violations = conn.execute("PRAGMA foreign_key_check").fetchall()
                    integrity_report['foreign_key_check'] = len(fk_violations) == 0
                    if fk_violations:
                        integrity_report['issues'].append(f"Foreign key violations: {len(fk_violations)}")
                except Exception as e:
                    integrity_report['issues'].append(f"Foreign key check error: {e}")
                
                # Schema version check
                try:
                    current_version = self._get_schema_version(conn)
                    integrity_report['schema_version_valid'] = current_version == SCHEMA_VERSION
                    integrity_report['current_schema_version'] = current_version
                    integrity_report['expected_schema_version'] = SCHEMA_VERSION
                except Exception as e:
                    integrity_report['issues'].append(f"Schema version check error: {e}")
                
                # Table counts
                for table in ['operations', 'transfers', 'system_metrics', 'operation_errors']:
                    try:
                        count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                        integrity_report['table_counts'][table] = count
                    except Exception as e:
                        integrity_report['issues'].append(f"Failed to count {table}: {e}")
                
        except Exception as e:
            integrity_report['issues'].append(f"Database connection error: {e}")
        
        return integrity_report
    
    def repair_database(self) -> bool:
        """Attempt to repair database issues."""
        logging.info("Attempting database repair...")
        
        try:
            with self._get_connection() as conn:
                # Try to rebuild the database
                conn.execute("VACUUM")
                conn.execute("REINDEX")
                conn.execute("ANALYZE")
                conn.commit()
            
            # Verify repair was successful
            integrity_report = self.verify_database_integrity()
            
            if integrity_report['integrity_check'] and integrity_report['foreign_key_check']:
                logging.info("Database repair completed successfully")
                return True
            else:
                logging.error("Database repair failed - integrity issues remain")
                return False
                
        except Exception as e:
            logging.error(f"Database repair failed: {e}")
            return False


# Migration utilities for converting from JSON to SQLite
class JSONToSQLiteMigrator:
    """Utility class for migrating existing JSON metrics to SQLite."""
    
    def __init__(self, database: MetricsDatabase):
        self.database = database
    
    def migrate_json_file(self, json_file_path: Path) -> bool:
        """Migrate a single JSON metrics file to SQLite."""
        try:
            with open(json_file_path) as f:
                data = json.load(f)
            
            # Store operation data
            operation_data = {
                'operation_id': data['operation_id'],
                'start_time': data['start_time'],
                'end_time': data.get('end_time'),
                'total_files': data.get('total_files', 0),
                'completed_files': data.get('completed_files', 0),
                'failed_files': data.get('failed_files', 0),
                'total_bytes': data.get('total_bytes', 0),
                'transferred_bytes': data.get('transferred_bytes', 0),
                'average_transfer_rate_bps': data.get('average_transfer_rate_bps', 0.0),
                'peak_transfer_rate_bps': data.get('peak_transfer_rate_bps', 0.0),
                'rsync_mode': data.get('rsync_mode', 'fast'),
                'success_rate': data.get('success_rate', 0.0),
                'duration_seconds': data.get('duration_seconds'),
                'overall_transfer_rate_mbps': data.get('overall_transfer_rate_mbps')
            }
            
            self.database.store_operation(operation_data)
            
            # Store transfer data
            for transfer in data.get('transfers', []):
                transfer_data = {
                    'operation_id': data['operation_id'],
                    'unit_path': transfer['unit_path'],
                    'src_disk': transfer['src_disk'],
                    'dest_disk': transfer['dest_disk'],
                    'size_bytes': transfer['size_bytes'],
                    'start_time': transfer['start_time'],
                    'end_time': transfer.get('end_time'),
                    'success': transfer.get('success', False),
                    'error_message': transfer.get('error_message'),
                    'transfer_rate_bps': transfer.get('transfer_rate_bps'),
                    'transfer_rate_mbps': transfer.get('transfer_rate_mbps'),
                    'duration_seconds': transfer.get('duration_seconds')
                }
                self.database.store_transfer(transfer_data)
            
            # Store system metrics
            for metric in data.get('system_samples', []):
                metric_data = {
                    'operation_id': data['operation_id'],
                    'timestamp': metric['timestamp'],
                    'cpu_percent': metric['cpu_percent'],
                    'memory_percent': metric['memory_percent'],
                    'disk_io_read_bps': metric['disk_io_read_bps'],
                    'disk_io_write_bps': metric['disk_io_write_bps'],
                    'network_sent_bps': metric.get('network_sent_bps', 0.0),
                    'network_recv_bps': metric.get('network_recv_bps', 0.0)
                }
                self.database.store_system_metric(metric_data)
            
            # Store errors
            for error in data.get('errors', []):
                self.database.store_error(data['operation_id'], error)
            
            logging.info(f"Successfully migrated {json_file_path}")
            return True
            
        except Exception as e:
            logging.error(f"Failed to migrate {json_file_path}: {e}")
            return False
    
    def migrate_directory(self, json_dir: Path) -> Tuple[int, int]:
        """Migrate all JSON files in a directory. Returns (success_count, total_count)."""
        json_files = list(json_dir.glob("*.json"))
        success_count = 0
        
        for json_file in json_files:
            if self.migrate_json_file(json_file):
                success_count += 1
        
        logging.info(f"Migration complete: {success_count}/{len(json_files)} files migrated successfully")
        return success_count, len(json_files)
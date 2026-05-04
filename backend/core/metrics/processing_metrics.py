"""
Processing Metrics Tracker (Phase 5)
Tracks real-time processing metrics for the pipeline dashboard.
"""
from __future__ import annotations
import json
import logging
from pathlib import Path
from typing import Any, Dict, Optional
from datetime import datetime, timezone
from dataclasses import dataclass, asdict
import os
import threading

@dataclass
class SyncMetrics:
    """Metrics for a single sync operation."""
    branch_id: str
    date: str
    start_time: str
    end_time: Optional[str] = None
    status: str = "in_progress"  # in_progress | completed | failed

    # API Fetching
    total_api_visits: int = 0
    new_visits_fetched: int = 0
    updated_visits_fetched: int = 0
    api_pages_fetched: int = 0
    api_errors: int = 0

    # Image Processing
    images_found: int = 0
    images_downloaded: int = 0
    images_skipped: int = 0
    download_errors: int = 0

    # ML Processing
    embeddings_extracted: int = 0
    embeddings_failed: int = 0
    embeddings_skipped: int = 0
    quality_filtered: int = 0

    # Storage
    points_upserted: int = 0
    visit_manifests_saved: int = 0

    # Clustering
    clusters_created: int = 0
    conflicts_detected: int = 0
    duplicates_detected: int = 0

    # Performance
    duration_seconds: float = 0.0
    avg_visit_processing_time_ms: float = 0.0

    # Errors
    error_message: Optional[str] = None
    warning_count: int = 0


class ProcessingMetricsManager:
    """
    Manages processing metrics for pipeline monitoring.
    Stores metrics in data/metrics/{branchId}_{date}.json
    """

    def __init__(self, storage_root: str = "data"):
        self.storage_root = Path(storage_root) / "metrics"
        self.storage_root.mkdir(parents=True, exist_ok=True)
        self.logger = logging.getLogger(__name__)
        self._lock = threading.Lock()

        # In-memory cache for active syncs
        self._active_syncs: Dict[str, SyncMetrics] = {}

    def _get_metrics_path(self, branch_id: str, date: str) -> Path:
        """Returns the path to metrics file for a specific branch/date."""
        return self.storage_root / f"{branch_id}_{date}.json"

    def start_sync(self, branch_id: str, date: str) -> SyncMetrics:
        """Initialize metrics for a new sync operation."""
        key = f"{branch_id}_{date}"

        metrics = SyncMetrics(
            branch_id=branch_id,
            date=date,
            start_time=datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
            status="in_progress"
        )

        with self._lock:
            self._active_syncs[key] = metrics

        self._save_metrics(metrics)
        return metrics

    def update_sync(
        self,
        branch_id: str,
        date: str,
        **updates
    ) -> Optional[SyncMetrics]:
        """Update metrics for an ongoing sync."""
        key = f"{branch_id}_{date}"

        with self._lock:
            metrics = self._active_syncs.get(key)
            if metrics is None:
                self.logger.warning(f"No active sync found for {key}, creating new one")
                metrics = self.start_sync(branch_id, date)

            # Apply updates
            for field, value in updates.items():
                if hasattr(metrics, field):
                    setattr(metrics, field, value)

            # Update duration
            if metrics.start_time:
                try:
                    start = datetime.fromisoformat(metrics.start_time.replace('Z', '+00:00'))
                    now = datetime.now(timezone.utc)
                    metrics.duration_seconds = (now - start).total_seconds()
                except Exception:
                    pass

        self._save_metrics(metrics)
        return metrics

    def complete_sync(
        self,
        branch_id: str,
        date: str,
        status: str = "completed",
        error_message: Optional[str] = None
    ) -> Optional[SyncMetrics]:
        """Mark a sync as completed or failed."""
        key = f"{branch_id}_{date}"

        with self._lock:
            metrics = self._active_syncs.get(key)
            if metrics is None:
                self.logger.warning(f"No active sync found for {key}")
                return None

            metrics.status = status
            metrics.end_time = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
            metrics.error_message = error_message

            # Calculate final duration
            if metrics.start_time:
                try:
                    start = datetime.fromisoformat(metrics.start_time.replace('Z', '+00:00'))
                    end = datetime.fromisoformat(metrics.end_time.replace('Z', '+00:00'))
                    metrics.duration_seconds = (end - start).total_seconds()
                except Exception:
                    pass

            # Calculate average processing time
            if metrics.new_visits_fetched > 0 and metrics.duration_seconds > 0:
                metrics.avg_visit_processing_time_ms = (
                    metrics.duration_seconds / metrics.new_visits_fetched * 1000
                )

        self._save_metrics(metrics)

        # Remove from active cache after a delay (keep for dashboard queries)
        # We'll keep it in memory for 5 minutes after completion
        # For now, just keep it (memory usage is minimal)

        return metrics

    def get_sync_metrics(self, branch_id: str, date: str) -> Optional[SyncMetrics]:
        """Retrieve metrics for a specific sync."""
        key = f"{branch_id}_{date}"

        # Check active cache first
        with self._lock:
            if key in self._active_syncs:
                return self._active_syncs[key]

        # Load from disk
        metrics_path = self._get_metrics_path(branch_id, date)
        if not metrics_path.exists():
            return None

        try:
            with open(metrics_path, 'r', encoding='utf-8') as f:
                data = json.load(f)

            return SyncMetrics(**data)
        except Exception as e:
            self.logger.error(f"Failed to load metrics from {metrics_path}: {e}")
            return None

    def get_recent_syncs(self, limit: int = 10) -> list[SyncMetrics]:
        """Get the most recent sync metrics across all branches."""
        metrics_list = []

        try:
            for metrics_file in sorted(
                self.storage_root.glob("*.json"),
                key=lambda p: p.stat().st_mtime,
                reverse=True
            )[:limit]:
                if metrics_file.name.startswith(".tmp_"):
                    continue

                try:
                    with open(metrics_file, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    metrics_list.append(SyncMetrics(**data))
                except Exception as e:
                    self.logger.error(f"Failed to load metrics from {metrics_file}: {e}")
                    continue

        except Exception as e:
            self.logger.error(f"Failed to scan metrics directory: {e}")

        return metrics_list

    def _save_metrics(self, metrics: SyncMetrics) -> bool:
        """Save metrics to disk with atomic write."""
        metrics_path = self._get_metrics_path(metrics.branch_id, metrics.date)

        try:
            # Atomic write
            tmp_path = metrics_path.parent / f".tmp_{metrics.branch_id}_{metrics.date}.json"
            with open(tmp_path, 'w', encoding='utf-8') as f:
                json.dump(asdict(metrics), f, ensure_ascii=False, indent=2)
                f.flush()
                os.fsync(f.fileno())

            tmp_path.replace(metrics_path)
            return True
        except Exception as e:
            self.logger.error(f"Failed to save metrics to {metrics_path}: {e}")
            if tmp_path.exists():
                try:
                    tmp_path.unlink()
                except:
                    pass
            return False

    def get_dashboard_summary(self) -> Dict[str, Any]:
        """
        Get a summary of all processing metrics for the dashboard.
        Returns aggregated stats across all recent syncs.
        """
        recent_syncs = self.get_recent_syncs(limit=50)

        if not recent_syncs:
            return {
                "total_syncs": 0,
                "active_syncs": 0,
                "completed_syncs": 0,
                "failed_syncs": 0,
                "total_visits_processed": 0,
                "total_embeddings": 0,
                "total_clusters": 0,
                "avg_sync_duration_seconds": 0.0,
                "last_sync_time": None
            }

        active = sum(1 for s in recent_syncs if s.status == "in_progress")
        completed = sum(1 for s in recent_syncs if s.status == "completed")
        failed = sum(1 for s in recent_syncs if s.status == "failed")

        total_visits = sum(s.new_visits_fetched for s in recent_syncs)
        total_embeddings = sum(s.embeddings_extracted for s in recent_syncs)
        total_clusters = sum(s.clusters_created for s in recent_syncs)

        completed_syncs = [s for s in recent_syncs if s.status == "completed" and s.duration_seconds > 0]
        avg_duration = (
            sum(s.duration_seconds for s in completed_syncs) / len(completed_syncs)
            if completed_syncs else 0.0
        )

        last_sync = recent_syncs[0] if recent_syncs else None

        return {
            "total_syncs": len(recent_syncs),
            "active_syncs": active,
            "completed_syncs": completed,
            "failed_syncs": failed,
            "total_visits_processed": total_visits,
            "total_embeddings": total_embeddings,
            "total_clusters": total_clusters,
            "avg_sync_duration_seconds": round(avg_duration, 2),
            "last_sync_time": last_sync.end_time or last_sync.start_time if last_sync else None,
            "recent_syncs": [
                {
                    "branch_id": s.branch_id,
                    "date": s.date,
                    "status": s.status,
                    "duration_seconds": round(s.duration_seconds, 2),
                    "visits_processed": s.new_visits_fetched,
                    "end_time": s.end_time or s.start_time
                }
                for s in recent_syncs[:10]
            ]
        }

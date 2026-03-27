from __future__ import annotations

import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta
from types import SimpleNamespace
from typing import TYPE_CHECKING

from loguru import logger
from sqlalchemy import delete, func, select

from program.db.db import db_session
from program.orchestrator.models import (
    DebridCacheStatus,
    DebridResolutionCache,
    DebridResolutionTask,
    DebridTaskPriority,
    DebridTaskStatus,
    DebridTaskTrigger,
    ProviderHealthState,
)
from program.orchestrator.provider_registry import ProviderRegistry
from program.orchestrator.provider_workers import ProviderQueueWorkers
from program.orchestrator.provider_wrapper import (
    ProviderCacheResult,
    ProviderNoMatchingFilesError,
    ProviderResolveStatus,
    ProviderResolveWrapper,
)
from program.orchestrator.rate_limiter import ProviderRateLimiter
from program.settings import settings_manager

if TYPE_CHECKING:
    from program.program import Program
    from program.services.downloaders.shared import DownloaderBase


def _log_debrid(message: str) -> None:
    try:
        logger.log("DEBRID", message)
    except ValueError:
        logger.info(message)


@dataclass
class ResolveOnPlayResult:
    success: bool
    status_code: int
    message: str
    item_id: int
    resolved: bool
    provider: str | None
    infohash: str | None
    queued_tasks: int
    processed_tasks: int
    elapsed_ms: int


@dataclass
class DueTaskCandidate:
    task_id: int
    infohash: str
    priority: DebridTaskPriority
    available_at: datetime
    provider: str | None
    provider_torrent_id: str | None
    provider_torrent_status: str | None
    acquiring_started_at: datetime | None


class DebridManager:
    def __init__(self) -> None:
        self._registry = ProviderRegistry()
        self._rate_limiters: dict[str, ProviderRateLimiter] = {}
        self._provider_state_lock = threading.Lock()
        self._metrics_lock = threading.Lock()
        self._provider_workers = ProviderQueueWorkers()
        self._negative_ttl = timedelta(
            minutes=settings_manager.settings.downloaders.orchestrator.cache_negative_ttl_minutes
        )
        self._uncached_acquire_fallback = (
            settings_manager.settings.downloaders.orchestrator.uncached_acquire_fallback
        )
        self._pending_acquire_poll = timedelta(
            seconds=settings_manager.settings.downloaders.orchestrator.uncached_acquire_poll_seconds
        )
        self._pending_acquire_max_wait = timedelta(
            minutes=settings_manager.settings.downloaders.orchestrator.uncached_acquire_max_wait_minutes
        )
        self._terminal_task_retention = max(
            timedelta(days=7),
            self._pending_acquire_max_wait * 2,
            self._negative_ttl * 8,
        )
        self._cache_retention = max(
            timedelta(days=30),
            self._negative_ttl * 48,
        )
        self._queue_backoff = timedelta(minutes=1)
        self._negative_reprobe_after = min(
            self._negative_ttl,
            max(self._queue_backoff * 5, self._pending_acquire_poll * 5),
        )
        self._strategy = (
            settings_manager.settings.downloaders.orchestrator.provider_strategy
        )
        self._priority_order = (
            settings_manager.settings.downloaders.orchestrator.provider_priority
        )
        self._stranded_recovery_delay = max(
            self._negative_ttl,
            self._queue_backoff * 5,
        )
        self._processing_stale_after = timedelta(
            seconds=max(
                60,
                settings_manager.settings.downloaders.orchestrator.shared_queue_poll_seconds
                * 4,
            )
        )
        self._cache_hits = 0
        self._cache_negative_hits = 0
        self._cache_misses = 0
        self._queue_processed_total = 0
        self._queue_resolved_total = 0
        self._queue_failed_total = 0
        self._queue_requeued_total = 0
        self._last_queue_run_at: datetime | None = None
        self._configure_limiters()

    def _configure_limiters(self) -> None:
        config = settings_manager.settings.downloaders.orchestrator.rate_limits
        self._rate_limiters = {
            "realdebrid": ProviderRateLimiter(
                requests_per_minute=config.realdebrid_per_minute,
                threshold_ratio=config.threshold_ratio,
            ),
            "debridlink": ProviderRateLimiter(
                requests_per_minute=config.debridlink_per_minute,
                threshold_ratio=config.threshold_ratio,
            ),
            "alldebrid": ProviderRateLimiter(
                requests_per_minute=config.alldebrid_per_minute,
                threshold_ratio=config.threshold_ratio,
            ),
        }

    def sync_services(self, services: list["DownloaderBase"]) -> None:
        self._registry.sync_services(services)

    def select_providers(
        self,
        services: list["DownloaderBase"],
        infohash: str,
        *,
        ignore_negative_cache: bool = False,
    ) -> list["DownloaderBase"]:
        self.sync_services(services)
        available: list[DownloaderBase] = []
        cached_first: list[DownloaderBase] = []
        skipped_negative_cache = list[str]()
        skipped_rate_limit = list[str]()

        for service in services:
            managed = self._registry.get_or_create(service)
            if managed.cooldown_until and managed.cooldown_until > datetime.utcnow():
                continue
            limiter = self._rate_limiters.get(service.key)
            if limiter and not limiter.can_allow():
                skipped_rate_limit.append(service.key)
                logger.debug(f"Skipping {service.key} for {infohash}: rate limit threshold reached")
                continue

            cached = self.get_cached(infohash, service.key)
            if cached == DebridCacheStatus.CACHED:
                cached_first.append(service)
            elif cached == DebridCacheStatus.NOT_FOUND and not ignore_negative_cache:
                skipped_negative_cache.append(service.key)
                logger.debug(f"Skipping {service.key} for {infohash}: negative cache still valid")
            else:
                available.append(service)

        if self._strategy == "priority":
            priority_index = {
                provider: index for index, provider in enumerate(self._priority_order)
            }
            ordered_available = sorted(
                available,
                key=lambda service: priority_index.get(service.key, len(priority_index)),
            )
        else:
            ordered_available = sorted(
                available,
                key=lambda service: self._balanced_sort_key(service),
            )

        ordered = cached_first + ordered_available

        if ordered:
            logger.debug(
                "Provider selection for {} -> ordered={}, cached_hits={}, rate_limited={}, negative_cache={}".format(
                    infohash,
                    [service.key for service in ordered],
                    [service.key for service in cached_first],
                    skipped_rate_limit,
                    skipped_negative_cache,
                )
            )

        return ordered

    def _balanced_sort_key(self, service: "DownloaderBase") -> tuple[int, float, int, datetime, int, str]:
        managed = self._registry.get(service.key)
        limiter = self._rate_limiters.get(service.key)

        health_rank = {
            ProviderHealthState.HEALTHY: 0,
            ProviderHealthState.RATE_LIMITED: 1,
            ProviderHealthState.DOWN: 2,
        }

        default_time = datetime.min
        return (
            health_rank.get(
                managed.health if managed else ProviderHealthState.HEALTHY,
                99,
            ),
            limiter.usage_ratio() if limiter else 0.0,
            managed.total_attempts if managed else 0,
            managed.last_selected_at or default_time if managed else default_time,
            managed.consecutive_failures if managed else 0,
            service.key,
        )

    def _get_valid_negative_cache_retry_at(
        self,
        infohash: str,
        services: list["DownloaderBase"],
    ) -> datetime | None:
        provider_keys = [service.key for service in services]
        if not provider_keys:
            return None

        now = datetime.utcnow()
        try:
            with db_session() as session:
                cache_rows = (
                    session.execute(
                        select(DebridResolutionCache)
                        .where(DebridResolutionCache.infohash == infohash)
                        .where(DebridResolutionCache.provider.in_(provider_keys))
                        .where(DebridResolutionCache.status == DebridCacheStatus.NOT_FOUND)
                    )
                    .scalars()
                    .all()
                )
        except Exception as exc:
            logger.debug(
                f"Failed reading negative cache retry window for {infohash}: {exc}"
            )
            return None

        retry_at = None
        for cache_row in cache_rows:
            expires_at = cache_row.last_checked + self._negative_ttl
            if expires_at <= now:
                continue
            if retry_at is None or expires_at < retry_at:
                retry_at = expires_at
        return retry_at

    def _should_reprobe_negative_cache(
        self,
        *,
        created_at: datetime,
        provider_torrent_id: str | None,
    ) -> bool:
        if provider_torrent_id:
            return False

        if not self._uncached_acquire_fallback:
            return False

        return datetime.utcnow() - created_at >= self._negative_reprobe_after

    def record_provider_attempt(self, provider: str) -> bool:
        with self._provider_state_lock:
            managed = self._registry.get(provider)
            if managed is None:
                return False

            limiter = self._rate_limiters.get(provider)
            if limiter and not limiter.consume():
                return False

            managed.total_attempts += 1
            managed.last_selected_at = datetime.utcnow()
            return True

    def enqueue_resolution_tasks(
        self,
        item,
        *,
        trigger: DebridTaskTrigger = DebridTaskTrigger.PIPELINE,
        priority: DebridTaskPriority = DebridTaskPriority.NORMAL,
        max_attempts: int = 3,
        max_streams: int = 3,
    ) -> int:
        from program.media.state import States

        if item.last_state not in (States.Scraped, States.PartiallyCompleted):
            return 0

        now = datetime.utcnow()
        candidate_streams = self._sort_candidate_streams(item.streams)[:max_streams]

        if not candidate_streams:
            return 0

        try:
            with db_session() as session:
                existing_infohashes = self._get_existing_open_infohashes(
                    session, item.id
                )

                tasks = list[DebridResolutionTask]()
                for stream in candidate_streams:
                    if stream.infohash in existing_infohashes:
                        continue

                    tasks.append(
                        DebridResolutionTask(
                            item_id=item.id,
                            infohash=stream.infohash,
                            provider=None,
                            provider_torrent_id=None,
                            provider_torrent_status=None,
                            stream_title=stream.raw_title,
                            trigger=trigger,
                            priority=priority,
                            status=DebridTaskStatus.PENDING,
                            attempts=0,
                            max_attempts=max_attempts,
                            available_at=now,
                            locked_at=None,
                            completed_at=None,
                            last_error=None,
                            created_at=now,
                            updated_at=now,
                        )
                    )

                if not tasks:
                    return 0

                session.add_all(tasks)
                session.commit()
                return len(tasks)
        except Exception as exc:
            logger.error(
                f"Failed to enqueue debrid resolution tasks for item {item.id}: {exc}"
            )
            return 0

    def _sort_candidate_streams(self, streams) -> list:
        resolution_order = {
            "4k": 9,
            "2160p": 9,
            "1440p": 7,
            "1080p": 6,
            "720p": 5,
            "576p": 4,
            "480p": 3,
            "360p": 2,
            "unknown": 1,
        }

        def _stream_key(stream) -> tuple[int, int]:
            resolution = (
                stream.resolution.lower()
                if getattr(stream, "resolution", None)
                else "unknown"
            )
            return (
                resolution_order.get(resolution, resolution_order["unknown"]),
                getattr(stream, "rank", 0),
            )

        return sorted(streams, key=_stream_key, reverse=True)

    def _get_item_play_snapshot(self, item_id: int) -> dict[str, object]:
        from program.db import db_functions

        with db_session() as session:
            item = db_functions.get_item_by_id(item_id, session=session)
            if item is None:
                return {
                    "exists": False,
                    "resolved": False,
                    "provider": None,
                    "infohash": None,
                    "last_state": None,
                    "open_tasks": 0,
                    "last_error": None,
                }

            item = session.merge(item)
            media_entry = item.media_entry
            active_stream = getattr(item, "active_stream", None)
            resolved = bool(media_entry and media_entry.url)

            open_tasks = (
                session.execute(
                    select(DebridResolutionTask.id)
                    .where(DebridResolutionTask.item_id == item_id)
                    .where(
                        DebridResolutionTask.status.in_(
                            [DebridTaskStatus.PENDING, DebridTaskStatus.PROCESSING]
                        )
                    )
                )
                .scalars()
                .all()
            )

            latest_task = (
                session.execute(
                    select(DebridResolutionTask)
                    .where(DebridResolutionTask.item_id == item_id)
                    .order_by(
                        DebridResolutionTask.updated_at.desc(),
                        DebridResolutionTask.id.desc(),
                    )
                )
                .scalars()
                .first()
            )

            provider = media_entry.provider if resolved and media_entry else None
            if provider is None and latest_task is not None:
                provider = latest_task.provider

            infohash = (
                active_stream.infohash
                if active_stream is not None
                else (latest_task.infohash if latest_task is not None else None)
            )

            return {
                "exists": True,
                "resolved": resolved,
                "provider": provider,
                "infohash": infohash,
                "last_state": str(item.last_state),
                "open_tasks": len(open_tasks),
                "last_error": latest_task.last_error if latest_task is not None else None,
            }

    def resolve_on_play(
        self,
        program: "Program",
        item_id: int,
        *,
        timeout_seconds: int = 20,
        max_streams: int = 3,
    ) -> ResolveOnPlayResult:
        from program.db import db_functions
        from program.media.state import States

        started_at = datetime.utcnow()

        def _elapsed_ms() -> int:
            return int((datetime.utcnow() - started_at).total_seconds() * 1000)

        _log_debrid(
            f"Resolve-on-play requested for item={item_id}, timeout={timeout_seconds}s, max_streams={max_streams}"
        )

        if not program.services or not program.services.downloader.initialized:
            return ResolveOnPlayResult(
                success=False,
                status_code=503,
                message="Downloader service is not initialized",
                item_id=item_id,
                resolved=False,
                provider=None,
                infohash=None,
                queued_tasks=0,
                processed_tasks=0,
                elapsed_ms=_elapsed_ms(),
            )

        snapshot = self._get_item_play_snapshot(item_id)
        if not bool(snapshot["exists"]):
            return ResolveOnPlayResult(
                success=False,
                status_code=404,
                message="Item not found",
                item_id=item_id,
                resolved=False,
                provider=None,
                infohash=None,
                queued_tasks=0,
                processed_tasks=0,
                elapsed_ms=_elapsed_ms(),
            )

        if bool(snapshot["resolved"]):
            return ResolveOnPlayResult(
                success=True,
                status_code=200,
                message="Item already resolved",
                item_id=item_id,
                resolved=True,
                provider=snapshot["provider"],
                infohash=snapshot["infohash"],
                queued_tasks=0,
                processed_tasks=0,
                elapsed_ms=_elapsed_ms(),
            )

        queued_tasks = 0
        with db_session() as session:
            item = db_functions.get_item_by_id(item_id, session=session)
            if item is None:
                return ResolveOnPlayResult(
                    success=False,
                    status_code=404,
                    message="Item not found",
                    item_id=item_id,
                    resolved=False,
                    provider=None,
                    infohash=None,
                    queued_tasks=0,
                    processed_tasks=0,
                    elapsed_ms=_elapsed_ms(),
                )

            item = session.merge(item)
            if item.last_state not in (States.Scraped, States.PartiallyCompleted):
                return ResolveOnPlayResult(
                    success=False,
                    status_code=409,
                    message=f"Item state is {item.last_state}; no playable stream can be resolved yet",
                    item_id=item_id,
                    resolved=False,
                    provider=None,
                    infohash=None,
                    queued_tasks=0,
                    processed_tasks=0,
                    elapsed_ms=_elapsed_ms(),
                )

            queued_tasks = self.enqueue_resolution_tasks(
                item,
                trigger=DebridTaskTrigger.PLAY,
                priority=DebridTaskPriority.HIGH,
                max_streams=max_streams,
                max_attempts=3,
            )
            _log_debrid(f"Resolve-on-play queued {queued_tasks} task(s) for item={item_id}")

        processed_tasks = 0
        deadline = time.monotonic() + max(1, timeout_seconds)
        per_tick_limit = max(
            1,
            settings_manager.settings.downloaders.orchestrator.shared_queue_max_parallel_tasks,
        )

        while time.monotonic() < deadline:
            processed_tasks += self.process_pending_tasks(program, limit=per_tick_limit)
            snapshot = self._get_item_play_snapshot(item_id)

            if bool(snapshot["resolved"]):
                _log_debrid(
                    f"Resolve-on-play succeeded for item={item_id} with provider={snapshot['provider']} in {processed_tasks} processed task(s)"
                )
                return ResolveOnPlayResult(
                    success=True,
                    status_code=200,
                    message="Resolved stream for playback",
                    item_id=item_id,
                    resolved=True,
                    provider=snapshot["provider"],
                    infohash=snapshot["infohash"],
                    queued_tasks=queued_tasks,
                    processed_tasks=processed_tasks,
                    elapsed_ms=_elapsed_ms(),
                )

            # No queued/open work remaining and nothing processed in this cycle.
            if int(snapshot["open_tasks"]) == 0 and processed_tasks == 0:
                logger.warning(
                    f"Resolve-on-play failed for item={item_id}: no provider could resolve stream ({snapshot['last_error']})"
                )
                return ResolveOnPlayResult(
                    success=False,
                    status_code=409,
                    message=(
                        "No provider could resolve a playable stream"
                        if not snapshot["last_error"]
                        else f"No provider could resolve a playable stream ({snapshot['last_error']})"
                    ),
                    item_id=item_id,
                    resolved=False,
                    provider=snapshot["provider"],
                    infohash=snapshot["infohash"],
                    queued_tasks=queued_tasks,
                    processed_tasks=processed_tasks,
                    elapsed_ms=_elapsed_ms(),
                )

            time.sleep(0.25)

        snapshot = self._get_item_play_snapshot(item_id)
        logger.warning(
            f"Resolve-on-play timed out for item={item_id} after {timeout_seconds}s (open_tasks={snapshot['open_tasks']}, processed={processed_tasks})"
        )
        return ResolveOnPlayResult(
            success=False,
            status_code=408,
            message="Timed out waiting for on-play resolution",
            item_id=item_id,
            resolved=bool(snapshot["resolved"]),
            provider=snapshot["provider"],
            infohash=snapshot["infohash"],
            queued_tasks=queued_tasks,
            processed_tasks=processed_tasks,
            elapsed_ms=_elapsed_ms(),
        )

    def _get_existing_open_infohashes(self, session, item_id: int) -> set[str]:
        rows = (
            session.execute(
                select(DebridResolutionTask.infohash)
                .where(DebridResolutionTask.item_id == item_id)
                .where(
                    DebridResolutionTask.status.in_(
                        [DebridTaskStatus.PENDING, DebridTaskStatus.PROCESSING]
                    )
                )
            )
            .scalars()
            .all()
        )
        return set(rows)

    def process_pending_tasks(self, program: "Program", limit: int = 10) -> int:
        if not program.services or not program.services.downloader.initialized:
            return 0

        downloader = program.services.downloader
        self.sync_services(downloader.initialized_services)
        self._last_queue_run_at = datetime.utcnow()
        recovered = self._recover_stale_processing_tasks()
        if recovered:
            logger.warning(
                f"Recovered {recovered} stale orchestrator task(s) left in processing state"
            )
        recovered_items = self._recover_stranded_scraped_items(limit=max(1, limit))
        if recovered_items:
            logger.warning(
                f"Recovered {recovered_items} stranded scraped item(s) with no active queue tasks"
            )

        processed = 0
        try:
            with db_session() as session:
                due_tasks = self._get_due_tasks(session, limit=max(limit * 3, limit))

            task_lanes = self._build_provider_task_lanes(
                due_tasks,
                downloader.initialized_services,
                limit=limit,
            )

            if not task_lanes:
                return 0

            _log_debrid(
                f"Orchestrator queue tick: due_tasks={len(due_tasks)}, lanes={len(task_lanes)}, limit={limit}"
            )
            logger.debug(f"Orchestrator provider lanes: {task_lanes}")

            worker_result = self._provider_workers.run_provider_lanes(
                task_lanes,
                lambda provider, task_id: self._process_single_task(
                    program,
                    task_id,
                    provider_hint=provider,
                ),
                max_workers=min(max(1, limit), len(task_lanes)),
            )
            with self._metrics_lock:
                self._queue_processed_total += worker_result.attempted_tasks
            processed += worker_result.successful_tasks
            _log_debrid(
                "Orchestrator queue workers completed: "
                f"attempted={worker_result.attempted_tasks}, "
                f"successful={worker_result.successful_tasks}, "
                f"providers={worker_result.providers_used}"
            )
        except Exception as exc:
            logger.error(f"Failed processing debrid resolution queue: {exc}")

        return processed

    def _recover_stranded_scraped_items(self, *, limit: int) -> int:
        from program.db import db_functions
        from program.media.item import MediaItem
        from program.media.state import States

        now = datetime.utcnow()
        recovered = 0
        candidate_ids = list[int]()

        try:
            with db_session() as session:
                candidates = (
                    session.execute(
                        select(MediaItem)
                        .where(MediaItem.last_state == States.Scraped)
                        .where(MediaItem.type.in_(["movie", "season", "episode"]))
                        .order_by(
                            MediaItem.scraped_at.asc().nullsfirst(),
                            MediaItem.id.asc(),
                        )
                    )
                    .scalars()
                    .all()
                )

                for item in candidates:
                    item = session.merge(item)

                    if item.available_in_vfs or (item.media_entry and item.media_entry.url):
                        continue

                    if not item.streams:
                        continue

                    has_open_tasks = session.execute(
                        select(DebridResolutionTask.id)
                        .where(DebridResolutionTask.item_id == item.id)
                        .where(
                            DebridResolutionTask.status.in_(
                                [
                                    DebridTaskStatus.PENDING,
                                    DebridTaskStatus.PROCESSING,
                                ]
                            )
                        )
                        .limit(1)
                    ).scalar_one_or_none()

                    if has_open_tasks is not None:
                        continue

                    latest_task = (
                        session.execute(
                            select(DebridResolutionTask)
                            .where(DebridResolutionTask.item_id == item.id)
                            .order_by(
                                DebridResolutionTask.updated_at.desc(),
                                DebridResolutionTask.id.desc(),
                            )
                        )
                        .scalars()
                        .first()
                    )

                    if latest_task is not None:
                        if latest_task.status not in (
                            DebridTaskStatus.FAILED,
                            DebridTaskStatus.CANCELLED,
                        ):
                            continue

                        # If the item was scraped again after the last terminal queue task,
                        # recover immediately instead of waiting for stranded_recovery_delay.
                        scraped_after_latest_terminal = bool(
                            item.scraped_at
                            and latest_task.updated_at
                            and item.scraped_at > latest_task.updated_at
                        )

                        if (
                            not scraped_after_latest_terminal
                            and latest_task.updated_at > now - self._stranded_recovery_delay
                        ):
                            continue

                    candidate_ids.append(item.id)
                    if len(candidate_ids) >= limit:
                        break

            for item_id in candidate_ids:
                with db_session() as session:
                    item = db_functions.get_item_by_id(item_id, session=session)
                    if item is None:
                        continue

                    item = session.merge(item)
                    queued = self.enqueue_resolution_tasks(
                        item,
                        trigger=DebridTaskTrigger.RETRY,
                        priority=DebridTaskPriority.NORMAL,
                        max_attempts=3,
                        max_streams=3,
                    )
                    if queued:
                        recovered += 1
                        logger.info(
                            f"Recovered stranded queued resolution work for {item.log_string} ({item.id})"
                        )
        except Exception as exc:
            logger.error(f"Failed recovering stranded scraped items: {exc}")

        return recovered

    def _recover_stale_processing_tasks(self) -> int:
        cutoff = datetime.utcnow() - self._processing_stale_after

        try:
            with db_session() as session:
                stale_tasks = (
                    session.execute(
                        select(DebridResolutionTask)
                        .where(
                            DebridResolutionTask.status
                            == DebridTaskStatus.PROCESSING
                        )
                        .where(
                            (DebridResolutionTask.locked_at.is_(None))
                            | (DebridResolutionTask.locked_at <= cutoff)
                        )
                    )
                    .scalars()
                    .all()
                )

                if not stale_tasks:
                    return 0

                now = datetime.utcnow()
                for task in stale_tasks:
                    task.status = DebridTaskStatus.PENDING
                    task.available_at = now
                    task.locked_at = None
                    task.updated_at = now
                    task.last_error = (
                        "Recovered stale processing task after interrupted worker"
                    )
                    session.add(task)

                session.commit()
                return len(stale_tasks)
        except Exception as exc:
            logger.error(f"Failed recovering stale orchestrator tasks: {exc}")
            return 0

    def prune_history(self) -> dict[str, int]:
        deleted_tasks = 0
        deleted_cache = 0
        now = datetime.utcnow()
        terminal_cutoff = now - self._terminal_task_retention
        cache_cutoff = now - self._cache_retention

        try:
            with db_session() as session:
                task_result = session.execute(
                    delete(DebridResolutionTask)
                    .where(
                        DebridResolutionTask.status.in_(
                            [
                                DebridTaskStatus.COMPLETED,
                                DebridTaskStatus.FAILED,
                                DebridTaskStatus.CANCELLED,
                            ]
                        )
                    )
                    .where(DebridResolutionTask.updated_at < terminal_cutoff)
                )
                deleted_tasks = int(task_result.rowcount or 0)

                cache_result = session.execute(
                    delete(DebridResolutionCache).where(
                        DebridResolutionCache.last_checked < cache_cutoff
                    )
                )
                deleted_cache = int(cache_result.rowcount or 0)

                session.commit()
        except Exception as exc:
            logger.error(f"Failed pruning orchestrator history: {exc}")
            return {
                "deleted_tasks": 0,
                "deleted_cache": 0,
            }

        return {
            "deleted_tasks": deleted_tasks,
            "deleted_cache": deleted_cache,
        }

    def _get_due_tasks(self, session, *, limit: int) -> list[DueTaskCandidate]:
        now = datetime.utcnow()
        due_tasks = (
            session.execute(
                select(DebridResolutionTask)
                .where(DebridResolutionTask.status == DebridTaskStatus.PENDING)
                .where(DebridResolutionTask.available_at <= now)
            )
            .scalars()
            .all()
        )

        priority_order = {
            DebridTaskPriority.HIGH: 0,
            DebridTaskPriority.NORMAL: 1,
            DebridTaskPriority.LOW: 2,
        }

        due_tasks.sort(
            key=lambda task: (
                priority_order.get(task.priority, 99),
                task.available_at,
                task.id,
            )
        )
        return [
            DueTaskCandidate(
                task_id=task.id,
                infohash=task.infohash,
                priority=task.priority,
                available_at=task.available_at,
                provider=task.provider,
                provider_torrent_id=task.provider_torrent_id,
                provider_torrent_status=task.provider_torrent_status,
                acquiring_started_at=task.acquiring_started_at,
            )
            for task in due_tasks[:limit]
        ]

    def _preferred_provider_for_infohash(
        self,
        services: list["DownloaderBase"],
        infohash: str,
    ) -> str:
        ordered = self.select_providers(services, infohash)
        if not ordered:
            return "unassigned"
        return ordered[0].key

    def _preferred_provider_for_task(
        self,
        task: DueTaskCandidate,
        services: list["DownloaderBase"],
    ) -> str:
        if (
            task.provider
            and task.provider_torrent_id
            and any(service.key == task.provider for service in services)
        ):
            return task.provider
        return self._preferred_provider_for_infohash(services, task.infohash)

    def _select_parallel_task_batch(
        self,
        due_tasks: list[DueTaskCandidate],
        services: list["DownloaderBase"],
        *,
        limit: int,
    ) -> list[int]:
        grouped = defaultdict(list)
        for task in due_tasks:
            provider_key = self._preferred_provider_for_task(task, services)
            grouped[provider_key].append(task.task_id)

        selected = list[int]()
        while grouped and len(selected) < limit:
            provider_keys = list(grouped.keys())
            for provider_key in provider_keys:
                queue = grouped.get(provider_key, [])
                if not queue:
                    grouped.pop(provider_key, None)
                    continue

                selected.append(queue.pop(0))
                if not queue:
                    grouped.pop(provider_key, None)

                if len(selected) >= limit:
                    break

        return selected

    def _build_provider_task_lanes(
        self,
        due_tasks: list[DueTaskCandidate],
        services: list["DownloaderBase"],
        *,
        limit: int,
    ) -> dict[str, list[int]]:
        if not due_tasks:
            return {}

        selected_ids = self._select_parallel_task_batch(due_tasks, services, limit=limit)
        task_by_id = {task.task_id: task for task in due_tasks}

        lanes = defaultdict(list)
        for task_id in selected_ids:
            task = task_by_id.get(task_id)
            if task is None:
                continue
            provider_key = self._preferred_provider_for_task(task, services)
            lanes[provider_key].append(task_id)

        return dict(lanes)

    def _process_single_task(
        self,
        program: "Program",
        task_id: int,
        *,
        provider_hint: str | None = None,
    ) -> bool:
        from program.db import db_functions
        from program.media.item import Episode, Season
        from program.media.state import States
        from program.types import Event

        assert program.services
        downloader = program.services.downloader
        provider_wrapper = ProviderResolveWrapper(downloader)

        with db_session() as session:
            task = session.get(DebridResolutionTask, task_id)
            if task is None or task.status != DebridTaskStatus.PENDING:
                return False

            item = db_functions.get_item_by_id(task.item_id, session=session)
            if item is None:
                self._finalize_task(
                    session,
                    task,
                    status=DebridTaskStatus.CANCELLED,
                    error="Item no longer exists",
                )
                session.commit()
                return False

            item = session.merge(item)

            if item.last_state not in (States.Scraped, States.PartiallyCompleted):
                self._finalize_task(
                    session,
                    task,
                    status=DebridTaskStatus.CANCELLED,
                    error=f"Item state is {item.last_state}, queue no longer required",
                )
                session.commit()
                return False

            stream = next((s for s in item.streams if s.infohash == task.infohash), None)
            if stream is None:
                self._finalize_task(
                    session,
                    task,
                    status=DebridTaskStatus.CANCELLED,
                    error="Stream no longer exists on item",
                )
                session.commit()
                return False

            task.status = DebridTaskStatus.PROCESSING
            task.attempts += 1
            task.locked_at = datetime.utcnow()
            task.updated_at = datetime.utcnow()
            session.add(task)
            session.commit()

            item_id = item.id
            item_log_string = item.log_string
            item_type = item.type
            task_infohash = task.infohash
            stream_infohash = stream.infohash
            task_created_at = task.created_at
            task_provider_torrent_id = task.provider_torrent_id
            task_provider = task.provider
            task_acquiring_started_at = task.acquiring_started_at

        if not provider_hint and task_provider_torrent_id and task_provider:
            provider_hint = task_provider

        if provider_hint and task_provider_torrent_id:
            providers = self.select_providers(
                [
                    service
                    for service in downloader.initialized_services
                    if service.key == provider_hint
                ],
                task_infohash,
                ignore_negative_cache=True,
            )
        else:
            providers = self.select_providers(
                downloader.initialized_services,
                task_infohash,
            )

        candidate_services = (
            [
                service
                for service in downloader.initialized_services
                if service.key == provider_hint
            ]
            if provider_hint and task_provider_torrent_id
            else downloader.initialized_services
        )

        if provider_hint and not task_provider_torrent_id:
            hinted = [service for service in providers if service.key == provider_hint]
            others = [service for service in providers if service.key != provider_hint]
            providers = hinted + others
            logger.debug(
                f"Task {task_id} provider hint='{provider_hint}', ordered providers={[service.key for service in providers]}"
            )
        if not providers:
            negative_retry_at = self._get_valid_negative_cache_retry_at(
                task_infohash,
                candidate_services,
            )

            if negative_retry_at and self._should_reprobe_negative_cache(
                created_at=task_created_at,
                provider_torrent_id=task_provider_torrent_id,
            ):
                providers = self.select_providers(
                    candidate_services,
                    task_infohash,
                    ignore_negative_cache=True,
                )
                if provider_hint and not task_provider_torrent_id:
                    hinted = [
                        service for service in providers if service.key == provider_hint
                    ]
                    others = [
                        service for service in providers if service.key != provider_hint
                    ]
                    providers = hinted + others
                if providers:
                    logger.warning(
                        "Task {} bypassing stale negative cache for {} after waiting {}".format(
                            task_id,
                            task_infohash,
                            datetime.utcnow() - task_created_at,
                        )
                    )

            if not providers:
                delay = self._queue_backoff
                error = "No providers available for task"
                if negative_retry_at:
                    delay = max(self._queue_backoff, negative_retry_at - datetime.utcnow())
                    error = (
                        "All providers blocked by negative cache until "
                        f"{negative_retry_at.isoformat()}"
                    )

                self._requeue_task(
                    task_id,
                    error=error,
                    delay=delay,
                    consume_attempt=False,
                )
                return False

        last_error = "No provider could resolve stream"
        eligible_services = []
        for service in providers:
            if not self.record_provider_attempt(service.key):
                last_error = f"No remaining provider budget for {service.key}"
                continue
            eligible_services.append(service)

        if not eligible_services:
            self._requeue_task(
                task_id,
                error=last_error,
                delay=self._queue_backoff,
                consume_attempt=False,
            )
            return False

        probe_item = SimpleNamespace(id=item_id, type=item_type, log_string=item_log_string)
        probe_stream = SimpleNamespace(infohash=stream_infohash)
        selected_service = None
        selected_cache = None
        probe_error = ""
        probe_policy_blocked = False

        if task_provider_torrent_id and provider_hint:
            selected_service = eligible_services[0]
            logger.debug(
                "Task {} polling provider-side torrent {} on {}".format(
                    task_id,
                    task_provider_torrent_id,
                    selected_service.key,
                )
            )
            try:
                selected_cache = provider_wrapper.check_existing_torrent(
                    selected_service,
                    task_infohash,
                    item=probe_item,
                    stream=probe_stream,
                    torrent_id=task_provider_torrent_id,
                )
            except Exception as exc:
                _rate_limited, _cooldown, classification = self._classify_provider_exception(exc)
                cache_status = (
                    DebridCacheStatus.NOT_FOUND
                    if classification == "content_policy_blocked"
                    else DebridCacheStatus.ERROR
                )
                self.save_resolution(task_infohash, selected_service.key, cache_status)
                self.record_provider_exception(selected_service.key, exc)
                probe_error = self._format_exception(exc)
                probe_policy_blocked = classification == "content_policy_blocked"
                selected_service = None
                selected_cache = None
        else:
            logger.debug(
                f"Task {task_id} probing cache in parallel across providers={[service.key for service in eligible_services]}"
            )
            (
                selected_service,
                selected_cache,
                probe_error,
                probe_policy_blocked,
            ) = self._probe_provider_caches_parallel(
                provider_wrapper,
                eligible_services,
                task_infohash,
                probe_item,
                probe_stream,
            )

        if selected_service is None or selected_cache is None:
            logger.warning(
                f"Task {task_id} failed cache probe across providers: {probe_error or last_error}"
            )
            if probe_policy_blocked:
                return self._blacklist_blocked_stream_and_advance(
                    task_id=task_id,
                    item_id=item_id,
                    infohash=task_infohash,
                    error=(
                        "Provider policy blocked this hash during cache probe: "
                        f"{probe_error or last_error}"
                    ),
                )
            self._requeue_task(
                task_id,
                error=probe_error or last_error,
                delay=self._queue_backoff * 5,
            )
            return False

        if (
            task_provider_torrent_id
            and not selected_cache.is_cached
            and not selected_cache.is_acquiring
        ):
            self._cleanup_provider_torrent(selected_service, task_provider_torrent_id)
            with db_session() as session:
                task = session.get(DebridResolutionTask, task_id)
                if task is not None:
                    self._clear_pending_provider_state(task)
                    session.add(task)
                    session.commit()
            self._requeue_task(
                task_id,
                error=f"Pending provider torrent is no longer available on {selected_service.key}",
                delay=self._queue_backoff * 5,
            )
            return False

        if selected_cache.is_acquiring:
            provider_status = None
            if (
                selected_cache.container
                and selected_cache.container.torrent_info
                and selected_cache.container.torrent_info.status
            ):
                provider_status = selected_cache.container.torrent_info.status

            acquiring_started_at = task_acquiring_started_at or task_created_at
            if datetime.utcnow() - acquiring_started_at >= self._pending_acquire_max_wait:
                self._cleanup_provider_torrent(
                    selected_service,
                    selected_cache.container.torrent_id
                    if selected_cache.container
                    else task_provider_torrent_id,
                )
                with db_session() as session:
                    task = session.get(DebridResolutionTask, task_id)
                    if task is not None:
                        self._clear_pending_provider_state(task)
                        session.add(task)
                        session.commit()
                self._requeue_task(
                    task_id,
                    error=(
                        f"Provider acquisition timed out on {selected_service.key}"
                        f" (status={provider_status or 'unknown'})"
                    ),
                    delay=self._queue_backoff * 5,
                )
                return False

            self._park_acquiring_task(
                task_id,
                provider=selected_service.key,
                torrent_id=(
                    selected_cache.container.torrent_id
                    if selected_cache.container and selected_cache.container.torrent_id
                    else task_provider_torrent_id
                ),
                provider_status=provider_status,
                delay=self._pending_acquire_poll,
                error=(
                    f"Waiting for provider acquisition on {selected_service.key}"
                    f" (status={provider_status or 'unknown'})"
                ),
            )
            return False

        _log_debrid(
            f"Task {task_id} selected provider={selected_service.key} after parallel cache probe"
        )

        try:
            with db_session() as session:
                task = session.get(DebridResolutionTask, task_id)
                if task is None:
                    return False

                item = db_functions.get_item_by_id(task.item_id, session=session)
                if item is None:
                    self._finalize_task(
                        session,
                        task,
                        status=DebridTaskStatus.CANCELLED,
                        error="Item no longer exists",
                    )
                    session.commit()
                    return False

                item = session.merge(item)
                stream = next(
                    (s for s in item.streams if s.infohash == task.infohash), None
                )
                if stream is None:
                    self._finalize_task(
                        session,
                        task,
                        status=DebridTaskStatus.CANCELLED,
                        error="Stream no longer exists on item",
                    )
                    session.commit()
                    return False

                task.provider = selected_service.key
                self._clear_pending_provider_state(task)
                task.updated_at = datetime.utcnow()
                session.add(task)
                session.commit()

                resolve_result = provider_wrapper.resolve_cached(
                    selected_service,
                    task.infohash,
                    item=item,
                    stream=stream,
                    cache_result=selected_cache,
                )
                if resolve_result.status != ProviderResolveStatus.RESOLVED:
                    self.save_resolution(
                        stream.infohash,
                        selected_service.key,
                        DebridCacheStatus.NOT_FOUND,
                    )
                    self._requeue_task(
                        task_id,
                        error=f"Stream not cached on {selected_service.key}",
                        delay=self._queue_backoff,
                    )
                    return False

                self.save_resolution(
                    stream.infohash, selected_service.key, DebridCacheStatus.CACHED
                )
                self.mark_provider_healthy(selected_service.key)

                item.store_state()
                if isinstance(item, Episode):
                    item.parent.store_state()
                    item.parent.parent.store_state()
                elif isinstance(item, Season):
                    item.parent.store_state()

                self._cancel_sibling_tasks(session, item.id, exclude_task_id=task.id)
                self._finalize_task(
                    session,
                    task,
                    status=DebridTaskStatus.COMPLETED,
                    error=None,
                )
                session.commit()

                program.em.add_event(
                    Event(emitted_by="OrchestratorQueue", item_id=item.id)
                )
                logger.info(
                    f"Resolved queued stream {stream.infohash} for {item.log_string} using {selected_service.key}"
                )
                return True
        except ProviderNoMatchingFilesError as exc:
            error_summary = self._format_exception(exc)
            self.save_resolution(
                task_infohash, selected_service.key, DebridCacheStatus.ERROR
            )
            self.record_provider_exception(selected_service.key, exc)
            with db_session() as session:
                task = session.get(DebridResolutionTask, task_id)
                if task is not None:
                    self._finalize_task(
                        session,
                        task,
                        status=DebridTaskStatus.FAILED,
                        error=error_summary,
                    )
                    session.commit()
            logger.debug(
                f"Queued resolution failed for {task_infohash} on {selected_service.key}: {error_summary}"
            )
            return False
        except Exception as exc:
            error_summary = self._format_exception(exc)
            _rate_limited, _cooldown, classification = self._classify_provider_exception(exc)
            cache_status = (
                DebridCacheStatus.NOT_FOUND
                if classification == "content_policy_blocked"
                else DebridCacheStatus.ERROR
            )
            self.save_resolution(task_infohash, selected_service.key, cache_status)
            self.record_provider_exception(selected_service.key, exc)
            last_error = error_summary
            logger.debug(
                f"Queued resolution failed for {task_infohash} on {selected_service.key}: {error_summary}"
            )
            if classification == "content_policy_blocked":
                return self._blacklist_blocked_stream_and_advance(
                    task_id=task_id,
                    item_id=item_id,
                    infohash=task_infohash,
                    error=f"Provider policy blocked this hash on {selected_service.key}: {error_summary}",
                )

        self._requeue_task(
            task_id,
            error=last_error,
            delay=self._queue_backoff * 5,
        )
        return False

    def _blacklist_blocked_stream_and_advance(
        self,
        *,
        task_id: int,
        item_id: int,
        infohash: str,
        error: str,
    ) -> bool:
        from program.db import db_functions

        queued_follow_up = 0
        with db_session() as session:
            task = session.get(DebridResolutionTask, task_id)
            if task is None:
                return False

            item = db_functions.get_item_by_id(item_id, session=session)
            if item is None:
                self._finalize_task(
                    session,
                    task,
                    status=DebridTaskStatus.CANCELLED,
                    error="Item no longer exists",
                )
                session.commit()
                return False

            item = session.merge(item)
            stream = next((s for s in item.streams if s.infohash == infohash), None)
            if stream is not None:
                item.blacklist_stream(stream)

            self._finalize_task(
                session,
                task,
                status=DebridTaskStatus.FAILED,
                error=error,
            )
            session.commit()

        # Refill queue with the next best candidate after blacklisting this blocked hash.
        with db_session() as session:
            item = db_functions.get_item_by_id(item_id, session=session)
            if item is not None:
                item = session.merge(item)
                queued_follow_up = self.enqueue_resolution_tasks(
                    item,
                    trigger=DebridTaskTrigger.RETRY,
                    priority=DebridTaskPriority.NORMAL,
                    max_attempts=3,
                    max_streams=3,
                )

        logger.warning(
            "Task {} failed due to provider policy block for {}. "
            "Blacklisted hash and queued {} follow-up candidate task(s).".format(
                task_id,
                infohash,
                queued_follow_up,
            )
        )
        return False

    def _probe_provider_caches_parallel(
        self,
        provider_wrapper: ProviderResolveWrapper,
        services: list["DownloaderBase"],
        infohash: str,
        item,
        stream,
    ) -> tuple["DownloaderBase | None", ProviderCacheResult | None, str, bool]:
        if not services:
            return (None, None, "No providers available for cache probing", False)

        max_workers = max(1, len(services))
        acquiring_results: dict[str, ProviderCacheResult] = {}
        policy_blocked_providers: set[str] = set()
        last_error = ""
        logger.debug(
            f"Starting parallel cache probe for infohash={infohash} providers={[service.key for service in services]}"
        )
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_map = {
                executor.submit(
                    provider_wrapper.check_cache,
                    service,
                    infohash,
                    item=item,
                    stream=stream,
                    allow_pending=self._uncached_acquire_fallback,
                ): service
                for service in services
            }

            for future in as_completed(future_map):
                service = future_map[future]
                try:
                    cache_result = future.result()
                    if cache_result.is_cached:
                        for pending in future_map:
                            if pending is not future:
                                pending.cancel()
                        _log_debrid(
                            f"Cache probe winner for infohash={infohash}: provider={service.key}"
                        )
                        return (service, cache_result, "", False)

                    if cache_result.is_acquiring:
                        acquiring_results[service.key] = cache_result
                        logger.debug(
                            f"Cache probe pending for infohash={infohash} on provider={service.key}"
                        )
                        last_error = (
                            cache_result.container.torrent_info.status
                            if cache_result.container
                            and cache_result.container.torrent_info
                            and cache_result.container.torrent_info.status
                            else f"Provider acquisition started on {service.key}"
                        )
                        continue

                    self.save_resolution(infohash, service.key, DebridCacheStatus.NOT_FOUND)
                    logger.debug(
                        f"Cache probe miss for infohash={infohash} on provider={service.key}"
                    )
                    last_error = f"Stream not cached on {service.key}"
                except Exception as exc:
                    _rate_limited, _cooldown, classification = self._classify_provider_exception(exc)
                    status = (
                        DebridCacheStatus.NOT_FOUND
                        if classification == "content_policy_blocked"
                        else DebridCacheStatus.ERROR
                    )
                    self.save_resolution(infohash, service.key, status)
                    self.record_provider_exception(service.key, exc)
                    if classification == "content_policy_blocked":
                        policy_blocked_providers.add(service.key)
                    last_error = self._format_exception(exc)

        if acquiring_results:
            for service in services:
                cache_result = acquiring_results.get(service.key)
                if cache_result is None:
                    continue
                _log_debrid(
                    f"Cache probe pending winner for infohash={infohash}: provider={service.key}"
                )
                return (service, cache_result, "", False)

        blocked_everywhere = len(policy_blocked_providers) == len(services)
        return (
            None,
            None,
            last_error or "No provider could resolve stream",
            blocked_everywhere,
        )

    def _park_acquiring_task(
        self,
        task_id: int,
        *,
        provider: str,
        torrent_id: int | str,
        provider_status: str | None,
        delay: timedelta,
        error: str,
    ) -> None:
        with db_session() as session:
            task = session.get(DebridResolutionTask, task_id)
            if task is None:
                return

            now = datetime.utcnow()
            if task.attempts > 0:
                task.attempts -= 1

            task.status = DebridTaskStatus.PENDING
            task.provider = provider
            task.provider_torrent_id = str(torrent_id)
            task.provider_torrent_status = provider_status
            task.acquiring_started_at = task.acquiring_started_at or now
            task.available_at = now + delay
            task.locked_at = None
            task.updated_at = now
            task.last_error = error
            session.add(task)
            session.commit()
            with self._metrics_lock:
                self._queue_requeued_total += 1
            logger.info(
                "Parked task {} for provider-side acquisition on {} (torrent_id={}, status={})".format(
                    task_id,
                    provider,
                    torrent_id,
                    provider_status or "unknown",
                )
            )

    def _clear_pending_provider_state(self, task: DebridResolutionTask) -> None:
        task.provider_torrent_id = None
        task.provider_torrent_status = None
        task.acquiring_started_at = None

    def cleanup_item_state(
        self,
        item_ids: list[int] | set[int] | tuple[int, ...],
        downloader_services: list["DownloaderBase"] | tuple["DownloaderBase", ...],
    ) -> tuple[int, int]:
        normalized_ids = sorted({int(item_id) for item_id in item_ids if item_id})
        if not normalized_ids:
            return (0, 0)

        services_by_key = {service.key: service for service in downloader_services}
        task_count = 0
        torrents_to_delete = list[tuple[str, str]]()

        with db_session() as session:
            tasks = (
                session.execute(
                    select(DebridResolutionTask).where(
                        DebridResolutionTask.item_id.in_(normalized_ids)
                    )
                )
                .scalars()
                .all()
            )

            if not tasks:
                return (0, 0)

            task_count = len(tasks)
            provider_torrents = {
                (task.provider, task.provider_torrent_id)
                for task in tasks
                if task.provider and task.provider_torrent_id
            }

            for provider, torrent_id in provider_torrents:
                still_referenced = session.execute(
                    select(DebridResolutionTask.id)
                    .where(DebridResolutionTask.provider == provider)
                    .where(DebridResolutionTask.provider_torrent_id == torrent_id)
                    .where(~DebridResolutionTask.item_id.in_(normalized_ids))
                    .limit(1)
                ).scalar_one_or_none()

                if still_referenced is None:
                    torrents_to_delete.append((provider, torrent_id))

            for task in tasks:
                session.delete(task)

            session.commit()

        deleted_torrents = 0
        for provider, torrent_id in torrents_to_delete:
            service = services_by_key.get(provider)
            if service is None:
                logger.debug(
                    f"Skipping provider torrent cleanup for {provider}:{torrent_id} because provider is not initialized"
                )
                continue

            try:
                service.delete_torrent(torrent_id)
                deleted_torrents += 1
            except Exception as exc:
                logger.warning(
                    f"Failed to delete provider torrent {torrent_id} on {provider}: {exc}"
                )

        return (task_count, deleted_torrents)

    def _cleanup_provider_torrent(
        self,
        service: "DownloaderBase",
        torrent_id: int | str | None,
    ) -> None:
        if not torrent_id:
            return
        try:
            service.delete_torrent(torrent_id)
        except Exception as exc:
            logger.debug(
                f"Failed to delete provider torrent {torrent_id} on {service.key}: {exc}"
            )

    def _cancel_sibling_tasks(self, session, item_id: int, *, exclude_task_id: int) -> None:
        siblings = (
            session.execute(
                select(DebridResolutionTask)
                .where(DebridResolutionTask.item_id == item_id)
                .where(DebridResolutionTask.id != exclude_task_id)
                .where(
                    DebridResolutionTask.status.in_(
                        [DebridTaskStatus.PENDING, DebridTaskStatus.PROCESSING]
                    )
                )
            )
            .scalars()
            .all()
        )

        for sibling in siblings:
            sibling.status = DebridTaskStatus.CANCELLED
            sibling.completed_at = datetime.utcnow()
            sibling.updated_at = datetime.utcnow()
            sibling.last_error = "Superseded by successful queued resolution"
            session.add(sibling)

    def _requeue_task(
        self,
        task_id: int,
        *,
        error: str,
        delay: timedelta,
        consume_attempt: bool = True,
    ) -> None:
        with db_session() as session:
            task = session.get(DebridResolutionTask, task_id)
            if task is None:
                return

            if not consume_attempt and task.attempts > 0:
                task.attempts -= 1

            if task.attempts >= task.max_attempts:
                self._finalize_task(
                    session,
                    task,
                    status=DebridTaskStatus.FAILED,
                    error=error,
                )
                session.commit()
                return

            task.status = DebridTaskStatus.PENDING
            task.available_at = datetime.utcnow() + delay
            task.locked_at = None
            task.updated_at = datetime.utcnow()
            task.last_error = error
            session.add(task)
            session.commit()
            with self._metrics_lock:
                self._queue_requeued_total += 1
            logger.warning(
                f"Requeued task {task_id} (attempt {task.attempts}/{task.max_attempts}) in {int(delay.total_seconds())}s: {error}"
            )

    def _finalize_task(self, session, task: DebridResolutionTask, *, status: DebridTaskStatus, error: str | None) -> None:
        now = datetime.utcnow()
        task.status = status
        task.completed_at = now if status in (
            DebridTaskStatus.COMPLETED,
            DebridTaskStatus.FAILED,
            DebridTaskStatus.CANCELLED,
        ) else task.completed_at
        task.locked_at = None
        task.updated_at = now
        task.last_error = error
        self._clear_pending_provider_state(task)
        session.add(task)
        with self._metrics_lock:
            if status == DebridTaskStatus.COMPLETED:
                self._queue_resolved_total += 1
            elif status == DebridTaskStatus.FAILED:
                self._queue_failed_total += 1

    def get_cached(self, infohash: str, provider: str) -> DebridCacheStatus | None:
        try:
            with db_session() as session:
                result = session.scalar(
                    select(DebridResolutionCache)
                    .where(DebridResolutionCache.infohash == infohash)
                    .where(DebridResolutionCache.provider == provider)
                )
                if result is None:
                    with self._metrics_lock:
                        self._cache_misses += 1
                    return None
                if (
                    result.status == DebridCacheStatus.NOT_FOUND
                    and result.last_checked < datetime.utcnow() - self._negative_ttl
                ):
                    with self._metrics_lock:
                        self._cache_misses += 1
                    return None
                with self._metrics_lock:
                    if result.status == DebridCacheStatus.CACHED:
                        self._cache_hits += 1
                    elif result.status == DebridCacheStatus.NOT_FOUND:
                        self._cache_negative_hits += 1
                    else:
                        self._cache_misses += 1
                return result.status
        except Exception as exc:
            logger.debug(f"Debrid cache lookup failed for {provider}:{infohash}: {exc}")
            return None

    def save_resolution(self, infohash: str, provider: str, status: DebridCacheStatus) -> None:
        now = datetime.utcnow()
        try:
            with db_session() as session:
                existing = session.scalar(
                    select(DebridResolutionCache)
                    .where(DebridResolutionCache.infohash == infohash)
                    .where(DebridResolutionCache.provider == provider)
                )
                if existing is None:
                    existing = DebridResolutionCache(
                        infohash=infohash,
                        provider=provider,
                        status=status,
                        resolved_at=now if status == DebridCacheStatus.CACHED else None,
                        last_checked=now,
                    )
                    session.add(existing)
                else:
                    existing.status = status
                    existing.last_checked = now
                    existing.resolved_at = now if status == DebridCacheStatus.CACHED else existing.resolved_at
                session.commit()
        except Exception as exc:
            logger.debug(f"Debrid cache persist failed for {provider}:{infohash}: {exc}")

    def _classify_provider_exception(self, exc: Exception) -> tuple[bool, int, str]:
        orchestrator_settings = settings_manager.settings.downloaders.orchestrator
        error_text = str(exc).strip().lower()
        repr_text = repr(exc).lower()
        combined_text = f"{error_text} {repr_text}".strip()
        exc_name = exc.__class__.__name__.lower()

        if isinstance(exc, ProviderNoMatchingFilesError) or exc_name == "nomatchingfilesexception":
            return (False, 0, "content_mismatch")

        if "451" in combined_text or "infringing torrent" in combined_text or "infringing file" in combined_text:
            return (False, 0, "content_policy_blocked")

        if "429" in combined_text or "rate limit" in combined_text or "circuitbreakeropen" in exc_name:
            return (
                True,
                orchestrator_settings.cooldown_minutes_rate_limited,
                "rate_limited",
            )

        if "timeout" in combined_text or "timeout" in exc_name:
            return (
                False,
                orchestrator_settings.cooldown_minutes_timeout,
                "timeout",
            )

        provider_down_markers = (
            "connection refused",
            "connection reset",
            "connection aborted",
            "max retries exceeded",
            "name resolution",
            "name or service not known",
            "temporary failure in name resolution",
            "host unreachable",
            "network is unreachable",
            "service unavailable",
            "bad gateway",
            "gateway timeout",
            "internal server error",
            "http 500",
            "http 502",
            "http 503",
            "http 504",
        )
        if any(marker in combined_text for marker in provider_down_markers):
            return (False, orchestrator_settings.cooldown_minutes_down, "provider_down")

        if not error_text:
            return (False, 0, "transient_unknown")

        return (False, 0, "transient_error")

    def _format_exception(self, exc: Exception) -> str:
        error_text = str(exc).strip()
        if error_text:
            return f"{exc.__class__.__name__}: {error_text}"
        return f"{exc.__class__.__name__}: {repr(exc)}"

    def record_provider_exception(self, provider: str, exc: Exception) -> None:
        rate_limited, cooldown_minutes, classification = self._classify_provider_exception(exc)
        error_summary = self._format_exception(exc)
        logger.warning(
            "Provider error classified: "
            f"provider={provider}, class={classification}, cooldown={cooldown_minutes}m, error={error_summary}"
        )
        if classification in {
            "content_mismatch",
            "content_policy_blocked",
            "transient_unknown",
            "transient_error",
        }:
            self.mark_provider_content_mismatch(
                provider,
                reason=f"{classification}: {error_summary}",
            )
            return
        self.mark_provider_error(
            provider,
            rate_limited=rate_limited,
            cooldown_minutes=cooldown_minutes,
            reason=f"{classification}: {error_summary}",
        )

    def mark_provider_content_mismatch(
        self,
        provider: str,
        *,
        reason: str | None = None,
    ) -> None:
        with self._provider_state_lock:
            managed = self._registry.get(provider)
            if managed is None:
                return
            managed.last_error = reason

    def mark_provider_error(
        self,
        provider: str,
        *,
        rate_limited: bool = False,
        cooldown_minutes: int = 1,
        reason: str | None = None,
    ) -> None:
        with self._provider_state_lock:
            managed = self._registry.get(provider)
            if managed is None:
                return
            managed.health = (
                ProviderHealthState.RATE_LIMITED if rate_limited else ProviderHealthState.DOWN
            )
            managed.cooldown_until = datetime.utcnow() + timedelta(minutes=cooldown_minutes)
            managed.total_failures += 1
            managed.consecutive_failures += 1
            managed.last_failure_at = datetime.utcnow()
            managed.last_error = reason

    def mark_provider_healthy(self, provider: str) -> None:
        with self._provider_state_lock:
            managed = self._registry.get(provider)
            if managed is None:
                return
            previous_health = managed.health
            managed.health = ProviderHealthState.HEALTHY
            managed.cooldown_until = None
            managed.total_successes += 1
            managed.consecutive_failures = 0
            managed.last_success_at = datetime.utcnow()
            managed.last_error = None
            if previous_health != ProviderHealthState.HEALTHY:
                _log_debrid(
                    f"Provider recovered: provider={provider}, previous_state={previous_health.value}"
                )

    def get_status_snapshot(self) -> dict[str, object]:
        providers = list[dict[str, object]]()
        for managed in self._registry.all():
            limiter = self._rate_limiters.get(managed.key)
            providers.append(
                {
                    "key": managed.key,
                    "health": managed.health.value,
                    "cooldown_until": (
                        managed.cooldown_until.isoformat()
                        if managed.cooldown_until
                        else None
                    ),
                    "total_attempts": managed.total_attempts,
                    "total_successes": managed.total_successes,
                    "total_failures": managed.total_failures,
                    "consecutive_failures": managed.consecutive_failures,
                    "last_selected_at": (
                        managed.last_selected_at.isoformat()
                        if managed.last_selected_at
                        else None
                    ),
                    "last_success_at": (
                        managed.last_success_at.isoformat()
                        if managed.last_success_at
                        else None
                    ),
                    "last_failure_at": (
                        managed.last_failure_at.isoformat()
                        if managed.last_failure_at
                        else None
                    ),
                    "last_error": managed.last_error,
                    "rate_limit": {
                        "requests_per_minute": (
                            limiter.requests_per_minute if limiter else None
                        ),
                        "effective_limit": limiter.effective_limit if limiter else None,
                        "current_requests": (
                            limiter.current_requests() if limiter else None
                        ),
                        "remaining_budget": (
                            limiter.remaining_budget() if limiter else None
                        ),
                        "usage_ratio": limiter.usage_ratio() if limiter else None,
                    },
                }
            )

        queue_counts = {
            DebridTaskStatus.PENDING.value: 0,
            DebridTaskStatus.PROCESSING.value: 0,
            DebridTaskStatus.COMPLETED.value: 0,
            DebridTaskStatus.FAILED.value: 0,
            DebridTaskStatus.CANCELLED.value: 0,
        }
        cache_counts = {
            DebridCacheStatus.CACHED.value: 0,
            DebridCacheStatus.NOT_FOUND.value: 0,
            DebridCacheStatus.ERROR.value: 0,
        }
        acquiring_pending = 0
        acquiring_by_status = defaultdict(int)
        next_task = None

        try:
            with db_session() as session:
                cache_rows = (
                    session.execute(
                        select(
                            DebridResolutionCache.status,
                            func.count(DebridResolutionCache.id),
                        ).group_by(DebridResolutionCache.status)
                    )
                    .all()
                )
                for status, count in cache_rows:
                    cache_counts[status.value] = count

                queue_rows = (
                    session.execute(
                        select(
                            DebridResolutionTask.status,
                            DebridResolutionTask.available_at,
                            DebridResolutionTask.id,
                            DebridResolutionTask.provider,
                            DebridResolutionTask.provider_torrent_id,
                            DebridResolutionTask.provider_torrent_status,
                            DebridResolutionTask.acquiring_started_at,
                        )
                    )
                    .all()
                )
                for (
                    status,
                    available_at,
                    task_id,
                    provider,
                    provider_torrent_id,
                    provider_torrent_status,
                    acquiring_started_at,
                ) in queue_rows:
                    queue_counts[status.value] = queue_counts.get(status.value, 0) + 1
                    if status == DebridTaskStatus.PENDING and provider_torrent_id:
                        acquiring_pending += 1
                        acquiring_by_status[provider_torrent_status or "unknown"] += 1
                    if status in (DebridTaskStatus.PENDING, DebridTaskStatus.PROCESSING):
                        if (
                            next_task is None
                            or available_at < next_task["available_at"]
                            or (
                                available_at == next_task["available_at"]
                                and task_id < next_task["id"]
                            )
                        ):
                            next_task = {
                                "id": task_id,
                                "available_at": available_at,
                                "status": status.value,
                                "provider": provider,
                                "provider_torrent_id": provider_torrent_id,
                                "provider_torrent_status": provider_torrent_status,
                                "acquiring_started_at": acquiring_started_at,
                            }
        except Exception as exc:
            logger.debug(f"Failed to inspect orchestrator queue status: {exc}")

        total_cache_entries = sum(cache_counts.values())
        with self._metrics_lock:
            cache_hits = self._cache_hits
            cache_negative_hits = self._cache_negative_hits
            cache_misses = self._cache_misses
            queue_processed_total = self._queue_processed_total
            queue_resolved_total = self._queue_resolved_total
            queue_failed_total = self._queue_failed_total
            queue_requeued_total = self._queue_requeued_total

        total_cache_lookups = cache_hits + cache_negative_hits + cache_misses
        cache_hit_ratio = (
            cache_hits / total_cache_lookups if total_cache_lookups > 0 else 0.0
        )
        cache_negative_hit_ratio = (
            cache_negative_hits / total_cache_lookups
            if total_cache_lookups > 0
            else 0.0
        )

        return {
            "enabled": settings_manager.settings.downloaders.orchestrator.enabled,
            "strategy": self._strategy,
            "priority_order": self._priority_order,
            "negative_ttl_minutes": int(self._negative_ttl.total_seconds() / 60),
            "uncached_acquire_fallback": self._uncached_acquire_fallback,
            "uncached_acquire_poll_seconds": int(
                self._pending_acquire_poll.total_seconds()
            ),
            "uncached_acquire_max_wait_minutes": int(
                self._pending_acquire_max_wait.total_seconds() / 60
            ),
            "shared_queue_enabled": settings_manager.settings.downloaders.orchestrator.shared_queue,
            "metrics": {
                "cache": {
                    "entries_total": total_cache_entries,
                    "entries_by_status": cache_counts,
                    "lookups_total": total_cache_lookups,
                    "hits": cache_hits,
                    "negative_hits": cache_negative_hits,
                    "misses": cache_misses,
                    "hit_ratio": cache_hit_ratio,
                    "negative_hit_ratio": cache_negative_hit_ratio,
                },
                "queue": {
                    "processed_total": queue_processed_total,
                    "resolved_total": queue_resolved_total,
                    "failed_total": queue_failed_total,
                    "requeued_total": queue_requeued_total,
                    "last_run_at": (
                        self._last_queue_run_at.isoformat()
                        if self._last_queue_run_at
                        else None
                    ),
                },
            },
            "queue": {
                "counts": queue_counts,
                "acquiring_pending": acquiring_pending,
                "acquiring_by_status": dict(acquiring_by_status),
                "next_task": (
                    {
                        "id": next_task["id"],
                        "available_at": next_task["available_at"].isoformat(),
                        "status": next_task["status"],
                        "provider": next_task["provider"],
                        "provider_torrent_id": next_task["provider_torrent_id"],
                        "provider_torrent_status": next_task["provider_torrent_status"],
                        "acquiring_started_at": (
                            next_task["acquiring_started_at"].isoformat()
                            if next_task["acquiring_started_at"]
                            else None
                        ),
                    }
                    if next_task
                    else None
                ),
            },
            "providers": providers,
        }


debrid_manager = DebridManager()

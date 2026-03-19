from __future__ import annotations

from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta
import time
import threading
from types import SimpleNamespace
from typing import TYPE_CHECKING

from loguru import logger
from sqlalchemy import func, select

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
from program.orchestrator.provider_registry import ManagedProvider, ProviderRegistry
from program.orchestrator.provider_wrapper import (
    ProviderCacheResult,
    ProviderResolveStatus,
    ProviderResolveWrapper,
)
from program.orchestrator.provider_workers import ProviderQueueWorkers
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
        self._strategy = (
            settings_manager.settings.downloaders.orchestrator.provider_strategy
        )
        self._priority_order = (
            settings_manager.settings.downloaders.orchestrator.provider_priority
        )
        self._queue_backoff = timedelta(minutes=1)
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

    def select_providers(self, services: list["DownloaderBase"], infohash: str) -> list["DownloaderBase"]:
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
            elif cached == DebridCacheStatus.NOT_FOUND:
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

    def _select_parallel_task_batch(
        self,
        due_tasks: list[DueTaskCandidate],
        services: list["DownloaderBase"],
        *,
        limit: int,
    ) -> list[int]:
        grouped = defaultdict(list)
        for task in due_tasks:
            provider_key = self._preferred_provider_for_infohash(services, task.infohash)
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
            provider_key = self._preferred_provider_for_infohash(services, task.infohash)
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
            stream_infohash = stream.infohash

        providers = self.select_providers(downloader.initialized_services, task.infohash)
        if provider_hint:
            hinted = [service for service in providers if service.key == provider_hint]
            others = [service for service in providers if service.key != provider_hint]
            providers = hinted + others
            logger.debug(
                f"Task {task_id} provider hint='{provider_hint}', ordered providers={[service.key for service in providers]}"
            )
        if not providers:
            self._requeue_task(
                task_id,
                error="No providers available for task",
                delay=self._queue_backoff,
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
            )
            return False

        logger.debug(
            f"Task {task_id} probing cache in parallel across providers={[service.key for service in eligible_services]}"
        )
        probe_item = SimpleNamespace(id=item_id, type=item_type, log_string=item_log_string)
        probe_stream = SimpleNamespace(infohash=stream_infohash)
        selected_service, selected_cache, probe_error = self._probe_provider_caches_parallel(
            provider_wrapper,
            eligible_services,
            task.infohash,
            probe_item,
            probe_stream,
        )
        if selected_service is None or selected_cache is None:
            logger.warning(
                f"Task {task_id} failed cache probe across providers: {probe_error or last_error}"
            )
            self._requeue_task(
                task_id,
                error=probe_error or last_error,
                delay=self._queue_backoff * 5,
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
        except Exception as exc:
            self.save_resolution(task.infohash, selected_service.key, DebridCacheStatus.ERROR)
            self.record_provider_exception(selected_service.key, exc)
            last_error = str(exc)
            logger.debug(
                f"Queued resolution failed for {task.infohash} on {selected_service.key}: {exc}"
            )

        self._requeue_task(
            task_id,
            error=last_error,
            delay=self._queue_backoff * 5,
        )
        return False

    def _probe_provider_caches_parallel(
        self,
        provider_wrapper: ProviderResolveWrapper,
        services: list["DownloaderBase"],
        infohash: str,
        item,
        stream,
    ) -> tuple["DownloaderBase | None", ProviderCacheResult | None, str]:
        if not services:
            return (None, None, "No providers available for cache probing")

        max_workers = max(1, len(services))
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
                        return (service, cache_result, "")

                    self.save_resolution(infohash, service.key, DebridCacheStatus.NOT_FOUND)
                    logger.debug(
                        f"Cache probe miss for infohash={infohash} on provider={service.key}"
                    )
                    last_error = f"Stream not cached on {service.key}"
                except Exception as exc:
                    self.save_resolution(infohash, service.key, DebridCacheStatus.ERROR)
                    self.record_provider_exception(service.key, exc)
                    last_error = str(exc)

        return (None, None, last_error or "No provider could resolve stream")

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

    def _requeue_task(self, task_id: int, *, error: str, delay: timedelta) -> None:
        with db_session() as session:
            task = session.get(DebridResolutionTask, task_id)
            if task is None:
                return

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
        error_text = str(exc).lower()
        exc_name = exc.__class__.__name__.lower()

        if "429" in error_text or "rate limit" in error_text or "circuitbreakeropen" in exc_name:
            return (
                True,
                orchestrator_settings.cooldown_minutes_rate_limited,
                "rate_limited",
            )

        if "timeout" in error_text or "timeout" in exc_name:
            return (
                False,
                orchestrator_settings.cooldown_minutes_timeout,
                "timeout",
            )

        return (False, orchestrator_settings.cooldown_minutes_down, "provider_down")

    def record_provider_exception(self, provider: str, exc: Exception) -> None:
        rate_limited, cooldown_minutes, classification = self._classify_provider_exception(exc)
        logger.warning(
            "Provider error classified: "
            f"provider={provider}, class={classification}, cooldown={cooldown_minutes}m, error={exc}"
        )
        self.mark_provider_error(
            provider,
            rate_limited=rate_limited,
            cooldown_minutes=cooldown_minutes,
            reason=f"{classification}: {exc}",
        )

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
                        )
                    )
                    .all()
                )
                for status, available_at, task_id in queue_rows:
                    queue_counts[status.value] = queue_counts.get(status.value, 0) + 1
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
                "next_task": (
                    {
                        "id": next_task["id"],
                        "available_at": next_task["available_at"].isoformat(),
                        "status": next_task["status"],
                    }
                    if next_task
                    else None
                ),
            },
            "providers": providers,
        }


debrid_manager = DebridManager()

from __future__ import annotations

from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta
import time
from typing import TYPE_CHECKING

from loguru import logger
from sqlalchemy import select

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
from program.orchestrator.rate_limiter import ProviderRateLimiter
from program.settings import settings_manager

if TYPE_CHECKING:
    from program.program import Program
    from program.services.downloaders.shared import DownloaderBase


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

        processed = 0
        try:
            with db_session() as session:
                due_tasks = self._get_due_tasks(session, limit=max(limit * 3, limit))

            task_batch = self._select_parallel_task_batch(
                due_tasks,
                downloader.initialized_services,
                limit=limit,
            )

            if not task_batch:
                return 0

            max_workers = min(max(1, limit), len(task_batch))
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = [
                    executor.submit(self._process_single_task, program, task_id)
                    for task_id in task_batch
                ]

                for future in as_completed(futures):
                    try:
                        if future.result():
                            processed += 1
                    except Exception as exc:
                        logger.debug(
                            f"Parallel orchestrator queue worker failed: {exc}"
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

    def _process_single_task(self, program: "Program", task_id: int) -> bool:
        from program.db import db_functions
        from program.media.item import Episode, Season
        from program.media.state import States
        from program.services.downloaders.models import NoMatchingFilesException
        from program.types import Event
        from program.utils.request import CircuitBreakerOpen

        assert program.services
        downloader = program.services.downloader

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

        providers = self.select_providers(downloader.initialized_services, task.infohash)
        if not providers:
            self._requeue_task(
                task_id,
                error="No providers available for task",
                delay=self._queue_backoff,
            )
            return False

        last_error = "No provider could resolve stream"
        for service in providers:
            if not self.record_provider_attempt(service.key):
                last_error = f"No remaining provider budget for {service.key}"
                continue

            try:
                with db_session() as session:
                    task = session.get(DebridResolutionTask, task_id)
                    item = db_functions.get_item_by_id(task.item_id, session=session)
                    if task is None or item is None:
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

                    task.provider = service.key
                    task.updated_at = datetime.utcnow()
                    session.add(task)
                    session.commit()

                    container = downloader.validate_stream_on_service(
                        stream, item, service
                    )
                    if not container:
                        self.save_resolution(
                            stream.infohash, service.key, DebridCacheStatus.NOT_FOUND
                        )
                        last_error = f"Stream not cached on {service.key}"
                        continue

                    result = downloader.download_cached_stream_on_service(
                        stream, container, service
                    )

                    if not downloader.update_item_attributes(item, result, service):
                        raise NoMatchingFilesException(
                            f"No valid files found for {item.log_string} ({item.id})"
                        )

                    self.save_resolution(
                        stream.infohash, service.key, DebridCacheStatus.CACHED
                    )
                    self.mark_provider_healthy(service.key)

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
                        f"Resolved queued stream {stream.infohash} for {item.log_string} using {service.key}"
                    )
                    return True
            except CircuitBreakerOpen:
                self.mark_provider_error(service.key, rate_limited=True)
                last_error = f"Circuit breaker open for {service.key}"
            except Exception as exc:
                self.save_resolution(task.infohash, service.key, DebridCacheStatus.ERROR)
                self.mark_provider_error(service.key)
                last_error = str(exc)
                logger.debug(
                    f"Queued resolution failed for {task.infohash} on {service.key}: {exc}"
                )

        self._requeue_task(
            task_id,
            error=last_error,
            delay=self._queue_backoff * 5,
        )
        return False

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

    def get_cached(self, infohash: str, provider: str) -> DebridCacheStatus | None:
        try:
            with db_session() as session:
                result = session.scalar(
                    select(DebridResolutionCache)
                    .where(DebridResolutionCache.infohash == infohash)
                    .where(DebridResolutionCache.provider == provider)
                )
                if result is None:
                    return None
                if (
                    result.status == DebridCacheStatus.NOT_FOUND
                    and result.last_checked < datetime.utcnow() - self._negative_ttl
                ):
                    return None
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

    def mark_provider_error(self, provider: str, *, rate_limited: bool = False, cooldown_minutes: int = 1) -> None:
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

    def mark_provider_healthy(self, provider: str) -> None:
        managed = self._registry.get(provider)
        if managed is None:
            return
        managed.health = ProviderHealthState.HEALTHY
        managed.cooldown_until = None
        managed.total_successes += 1
        managed.consecutive_failures = 0
        managed.last_success_at = datetime.utcnow()

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
        next_task = None

        try:
            with db_session() as session:
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

        return {
            "enabled": settings_manager.settings.downloaders.orchestrator.enabled,
            "strategy": self._strategy,
            "priority_order": self._priority_order,
            "negative_ttl_minutes": int(self._negative_ttl.total_seconds() / 60),
            "shared_queue_enabled": settings_manager.settings.downloaders.orchestrator.shared_queue,
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

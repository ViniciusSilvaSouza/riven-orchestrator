from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

from loguru import logger
from sqlalchemy import select

from program.db.db import db_session
from program.orchestrator.models import (
    DebridCacheStatus,
    DebridResolutionCache,
    ProviderHealthState,
)
from program.orchestrator.provider_registry import ManagedProvider, ProviderRegistry
from program.orchestrator.rate_limiter import ProviderRateLimiter
from program.settings import settings_manager

if TYPE_CHECKING:
    from program.services.downloaders.shared import DownloaderBase


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

        return {
            "enabled": settings_manager.settings.downloaders.orchestrator.enabled,
            "strategy": self._strategy,
            "priority_order": self._priority_order,
            "negative_ttl_minutes": int(self._negative_ttl.total_seconds() / 60),
            "shared_queue_enabled": settings_manager.settings.downloaders.orchestrator.shared_queue,
            "providers": providers,
        }


debrid_manager = DebridManager()

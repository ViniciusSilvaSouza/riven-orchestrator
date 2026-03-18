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
from program.orchestrator.rate_limiter import ProviderRateLimiter
from program.settings import settings_manager

if TYPE_CHECKING:
    from program.services.downloaders.shared import DownloaderBase


@dataclass
class ManagedProvider:
    service: "DownloaderBase"
    health: ProviderHealthState = ProviderHealthState.HEALTHY
    cooldown_until: datetime | None = None

    @property
    def key(self) -> str:
        return self.service.key


class DebridManager:
    def __init__(self) -> None:
        self._health: dict[str, ManagedProvider] = {}
        self._rate_limiters: dict[str, ProviderRateLimiter] = {}
        self._negative_ttl = timedelta(
            minutes=settings_manager.settings.downloaders.orchestrator.cache_negative_ttl_minutes
        )
        self._strategy = (
            settings_manager.settings.downloaders.orchestrator.provider_strategy
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

    def select_providers(self, services: list["DownloaderBase"], infohash: str) -> list["DownloaderBase"]:
        available: list[DownloaderBase] = []
        cached_first: list[DownloaderBase] = []

        for service in services:
            managed = self._health.setdefault(service.key, ManagedProvider(service=service))
            if managed.cooldown_until and managed.cooldown_until > datetime.utcnow():
                continue
            limiter = self._rate_limiters.get(service.key)
            if limiter and not limiter.allow():
                logger.debug(f"Skipping {service.key} for {infohash}: rate limit threshold reached")
                continue

            cached = self.get_cached(infohash, service.key)
            if cached == DebridCacheStatus.CACHED:
                cached_first.append(service)
            elif cached == DebridCacheStatus.NOT_FOUND:
                logger.debug(f"Skipping {service.key} for {infohash}: negative cache still valid")
            else:
                available.append(service)

        ordered = cached_first + available
        if self._strategy == "priority":
            return ordered
        return sorted(ordered, key=lambda service: (service not in cached_first, service.key))

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
        managed = self._health.get(provider)
        if managed is None:
            return
        managed.health = (
            ProviderHealthState.RATE_LIMITED if rate_limited else ProviderHealthState.DOWN
        )
        managed.cooldown_until = datetime.utcnow() + timedelta(minutes=cooldown_minutes)

    def mark_provider_healthy(self, provider: str) -> None:
        managed = self._health.get(provider)
        if managed is None:
            return
        managed.health = ProviderHealthState.HEALTHY
        managed.cooldown_until = None


debrid_manager = DebridManager()

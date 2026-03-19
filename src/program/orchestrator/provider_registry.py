from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING

from program.orchestrator.models import ProviderHealthState

if TYPE_CHECKING:
    from program.services.downloaders.shared import DownloaderBase


@dataclass
class ManagedProvider:
    service: "DownloaderBase"
    health: ProviderHealthState = ProviderHealthState.HEALTHY
    cooldown_until: datetime | None = None
    last_selected_at: datetime | None = None
    last_success_at: datetime | None = None
    last_failure_at: datetime | None = None
    total_attempts: int = 0
    total_successes: int = 0
    total_failures: int = 0
    consecutive_failures: int = 0
    last_error: str | None = None

    @property
    def key(self) -> str:
        return self.service.key


class ProviderRegistry:
    def __init__(self) -> None:
        self._providers: dict[str, ManagedProvider] = {}

    def sync_services(self, services: list["DownloaderBase"]) -> None:
        active_keys = {service.key for service in services}

        for service in services:
            existing = self._providers.get(service.key)
            if existing is None:
                self._providers[service.key] = ManagedProvider(service=service)
            else:
                existing.service = service

        stale_keys = [key for key in self._providers if key not in active_keys]
        for key in stale_keys:
            del self._providers[key]

    def get(self, provider: str) -> ManagedProvider | None:
        return self._providers.get(provider)

    def get_or_create(self, service: "DownloaderBase") -> ManagedProvider:
        existing = self._providers.get(service.key)
        if existing is None:
            existing = ManagedProvider(service=service)
            self._providers[service.key] = existing
        else:
            existing.service = service
        return existing

    def all(self) -> list[ManagedProvider]:
        return list(self._providers.values())

    def keys(self) -> list[str]:
        return list(self._providers.keys())


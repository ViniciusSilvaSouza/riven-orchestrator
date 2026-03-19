from contextlib import contextmanager
from datetime import datetime, timedelta
import importlib
from unittest.mock import Mock

from program.orchestrator.debrid_manager import DebridManager, DueTaskCandidate
from program.orchestrator.models import DebridCacheStatus
from program.orchestrator.models import DebridTaskPriority
from program.orchestrator.rate_limiter import ProviderRateLimiter


class FakeSession:
    def __init__(self, stored=None):
        self.stored = stored
        self.added = None
        self.committed = False

    def scalar(self, _query):
        return self.stored

    def add(self, obj):
        self.added = obj
        self.stored = obj

    def commit(self):
        self.committed = True


def test_provider_rate_limiter_respects_threshold():
    limiter = ProviderRateLimiter(requests_per_minute=10, threshold_ratio=0.8)

    for _ in range(8):
        assert limiter.allow() is True

    assert limiter.allow() is False


def test_debrid_manager_prioritizes_cached_provider(monkeypatch):
    manager = DebridManager()
    service_a = Mock(key="realdebrid")
    service_b = Mock(key="alldebrid")

    monkeypatch.setattr(
        manager,
        "get_cached",
        lambda infohash, provider: (
            DebridCacheStatus.CACHED if provider == "alldebrid" else None
        ),
    )

    selected = manager.select_providers([service_a, service_b], "abc123")

    assert [service.key for service in selected] == ["alldebrid", "realdebrid"]


def test_debrid_manager_saves_new_resolution(monkeypatch):
    manager = DebridManager()
    session = FakeSession()
    debrid_manager_module = importlib.import_module("program.orchestrator.debrid_manager")

    @contextmanager
    def session_factory():
        yield session

    monkeypatch.setattr(debrid_manager_module, "db_session", session_factory)

    manager.save_resolution("abc123", "realdebrid", DebridCacheStatus.CACHED)

    assert session.added is not None
    assert session.added.infohash == "abc123"
    assert session.added.provider == "realdebrid"
    assert session.added.status == DebridCacheStatus.CACHED
    assert session.committed is True


def test_debrid_manager_negative_cache_expires(monkeypatch):
    manager = DebridManager()
    cached_entry = Mock(
        status=DebridCacheStatus.NOT_FOUND,
        last_checked=datetime.utcnow() - timedelta(hours=1),
    )
    debrid_manager_module = importlib.import_module("program.orchestrator.debrid_manager")

    @contextmanager
    def session_factory():
        yield FakeSession(stored=cached_entry)

    monkeypatch.setattr(debrid_manager_module, "db_session", session_factory)

    assert manager.get_cached("abc123", "realdebrid") is None


def test_debrid_manager_balanced_prefers_less_used_provider(monkeypatch):
    manager = DebridManager()
    service_a = Mock(key="realdebrid")
    service_b = Mock(key="alldebrid")

    monkeypatch.setattr(manager, "get_cached", lambda infohash, provider: None)
    manager.sync_services([service_a, service_b])

    assert manager.record_provider_attempt("realdebrid") is True
    assert manager.record_provider_attempt("realdebrid") is True

    selected = manager.select_providers([service_a, service_b], "abc123")

    assert [service.key for service in selected] == ["alldebrid", "realdebrid"]


def test_debrid_manager_priority_honors_configured_order(monkeypatch):
    from program.settings import settings_manager

    old_strategy = settings_manager.settings.downloaders.orchestrator.provider_strategy
    old_order = list(settings_manager.settings.downloaders.orchestrator.provider_priority)

    settings_manager.settings.downloaders.orchestrator.provider_strategy = "priority"
    settings_manager.settings.downloaders.orchestrator.provider_priority = [
        "alldebrid",
        "realdebrid",
        "debridlink",
    ]

    try:
        manager = DebridManager()
        service_a = Mock(key="realdebrid")
        service_b = Mock(key="alldebrid")

        monkeypatch.setattr(manager, "get_cached", lambda infohash, provider: None)

        selected = manager.select_providers([service_a, service_b], "abc123")

        assert [service.key for service in selected] == ["alldebrid", "realdebrid"]
    finally:
        settings_manager.settings.downloaders.orchestrator.provider_strategy = old_strategy
        settings_manager.settings.downloaders.orchestrator.provider_priority = old_order


def test_select_providers_does_not_consume_budget_until_attempt(monkeypatch):
    manager = DebridManager()
    service = Mock(key="realdebrid")

    monkeypatch.setattr(manager, "get_cached", lambda infohash, provider: None)
    manager.sync_services([service])
    manager._rate_limiters["realdebrid"] = ProviderRateLimiter(
        requests_per_minute=1,
        threshold_ratio=1.0,
    )

    selected = manager.select_providers([service], "abc123")

    assert [provider.key for provider in selected] == ["realdebrid"]
    assert manager._rate_limiters["realdebrid"].current_requests() == 0
    assert manager.record_provider_attempt("realdebrid") is True
    assert manager._rate_limiters["realdebrid"].current_requests() == 1


def test_parallel_batch_round_robin_by_provider(monkeypatch):
    manager = DebridManager()
    services = [Mock(key="realdebrid"), Mock(key="alldebrid")]
    now = datetime.utcnow()
    due_tasks = [
        DueTaskCandidate(task_id=1, infohash="h1", priority=DebridTaskPriority.NORMAL, available_at=now),
        DueTaskCandidate(task_id=2, infohash="h2", priority=DebridTaskPriority.NORMAL, available_at=now),
        DueTaskCandidate(task_id=3, infohash="h3", priority=DebridTaskPriority.NORMAL, available_at=now),
        DueTaskCandidate(task_id=4, infohash="h4", priority=DebridTaskPriority.NORMAL, available_at=now),
    ]

    provider_map = {
        "h1": "realdebrid",
        "h2": "realdebrid",
        "h3": "alldebrid",
        "h4": "alldebrid",
    }

    monkeypatch.setattr(
        manager,
        "_preferred_provider_for_infohash",
        lambda services, infohash: provider_map[infohash],
    )

    selected = manager._select_parallel_task_batch(due_tasks, services, limit=4)

    assert selected == [1, 3, 2, 4]

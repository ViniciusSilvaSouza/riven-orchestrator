from contextlib import contextmanager
from datetime import datetime, timedelta
from unittest.mock import Mock

from program.orchestrator.debrid_manager import DebridManager
from program.orchestrator.models import DebridCacheStatus
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

    @contextmanager
    def session_factory():
        yield session

    monkeypatch.setattr("program.orchestrator.debrid_manager.db_session", session_factory)

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

    @contextmanager
    def session_factory():
        yield FakeSession(stored=cached_entry)

    monkeypatch.setattr("program.orchestrator.debrid_manager.db_session", session_factory)

    assert manager.get_cached("abc123", "realdebrid") is None

import importlib
import importlib.util
import sys
import types
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import Mock

from program.orchestrator.debrid_manager import DebridManager
from program.orchestrator.debrid_manager import DueTaskCandidate
from program.orchestrator.models import (
    DebridCacheStatus,
    DebridResolutionTask,
    DebridTaskPriority,
    DebridTaskStatus,
    DebridTaskTrigger,
)
from program.orchestrator.provider_wrapper import (
    ProviderCacheResult,
    ProviderResolveStatus,
)

_STATES_SPEC = importlib.util.spec_from_file_location(
    "test_media_state",
    Path(__file__).resolve().parents[1] / "program" / "media" / "state.py",
)
assert _STATES_SPEC and _STATES_SPEC.loader
_states_module = importlib.util.module_from_spec(_STATES_SPEC)
_STATES_SPEC.loader.exec_module(_states_module)
States = _states_module.States

_fake_media_package = types.ModuleType("program.media")
_fake_media_package.__path__ = []
_fake_media_state = types.ModuleType("program.media.state")
_fake_media_state.States = States
_fake_media_package.state = _fake_media_state
sys.modules["program.media"] = _fake_media_package
sys.modules["program.media.state"] = _fake_media_state


class FakeQueueSession:
    def __init__(self, existing_infohashes=None):
        self.existing_infohashes = existing_infohashes or []
        self.added = []
        self.committed = False

    def execute(self, _query):
        session = self

        class _Result:
            def scalars(self_inner):
                return self_inner

            def all(self_inner):
                return session.existing_infohashes

        return _Result()

    def add_all(self, items):
        self.added.extend(items)

    def commit(self):
        self.committed = True


class FakeTaskSession:
    def __init__(self, task):
        self.task = task
        self.committed = False

    def get(self, _model, _task_id):
        return self.task

    def add(self, _obj):
        return None

    def commit(self):
        self.committed = True


class FakePolicyBlockSession:
    def __init__(self, task=None, item=None):
        self.task = task
        self.item = item
        self.committed = False

    def get(self, _model, _task_id):
        return self.task

    def merge(self, obj):
        return obj

    def add(self, _obj):
        return None

    def commit(self):
        self.committed = True


class FakeStaleProcessingSession:
    def __init__(self, stale_tasks):
        self.stale_tasks = stale_tasks
        self.added = []
        self.committed = False

    def execute(self, _query):
        session = self

        class _Result:
            def scalars(self_inner):
                return self_inner

            def all(self_inner):
                return session.stale_tasks

        return _Result()

    def add(self, obj):
        self.added.append(obj)

    def commit(self):
        self.committed = True


class FakeRecoveryScanSession:
    def __init__(self, candidates, *, has_open_tasks=False, latest_task=None):
        self.candidates = candidates
        self.has_open_tasks = has_open_tasks
        self.latest_task = latest_task
        self.execute_calls = 0

    def execute(self, _query):
        self.execute_calls += 1
        session = self

        class _Result:
            def scalars(self_inner):
                return self_inner

            def all(self_inner):
                return session.candidates

            def first(self_inner):
                return session.latest_task

            def scalar_one_or_none(self_inner):
                return 1 if session.has_open_tasks else None

        return _Result()

    def merge(self, item):
        return item


class FakeRecoveryLoadSession:
    def __init__(self):
        self.merged_item = None

    def merge(self, item):
        self.merged_item = item
        return item


class FakeQuery:
    def where(self, *_args, **_kwargs):
        return self

    def order_by(self, *_args, **_kwargs):
        return self

    def limit(self, *_args, **_kwargs):
        return self


class FakeColumn:
    __hash__ = object.__hash__

    def in_(self, *_args, **_kwargs):
        return self

    def asc(self):
        return self

    def desc(self):
        return self

    def nullsfirst(self):
        return self

    def __eq__(self, _other):
        return self


def test_enqueue_resolution_tasks_creates_top_stream_tasks(monkeypatch):
    manager = DebridManager()
    session = FakeQueueSession(existing_infohashes=["hash-existing"])
    debrid_manager_module = importlib.import_module("program.orchestrator.debrid_manager")

    item = Mock()
    item.id = 42
    item.last_state = States.Scraped
    item.streams = [
        Mock(infohash="hash-existing", raw_title="Old", resolution="2160p", rank=10),
        Mock(infohash="hash-1", raw_title="One", resolution="1080p", rank=8),
        Mock(infohash="hash-2", raw_title="Two", resolution="720p", rank=7),
    ]

    @contextmanager
    def session_factory():
        yield session

    monkeypatch.setattr(debrid_manager_module, "db_session", session_factory)

    queued = manager.enqueue_resolution_tasks(
        item,
        trigger=DebridTaskTrigger.PIPELINE,
        priority=DebridTaskPriority.NORMAL,
        max_streams=3,
    )

    assert queued == 2
    assert session.committed is True
    assert [task.infohash for task in session.added] == ["hash-1", "hash-2"]
    assert all(isinstance(task, DebridResolutionTask) for task in session.added)
    assert all(task.status == DebridTaskStatus.PENDING for task in session.added)


def test_enqueue_resolution_tasks_skips_when_not_scraped():
    manager = DebridManager()
    item = Mock()
    item.last_state = States.Downloaded
    item.streams = [Mock(infohash="hash-1", raw_title="One")]

    assert manager.enqueue_resolution_tasks(item) == 0


def test_requeue_without_consuming_attempt_restores_attempt_budget(monkeypatch):
    manager = DebridManager()
    task = Mock(
        attempts=1,
        max_attempts=3,
        status=DebridTaskStatus.PROCESSING,
        available_at=None,
        locked_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
        last_error=None,
    )
    session = FakeTaskSession(task)
    debrid_manager_module = importlib.import_module("program.orchestrator.debrid_manager")

    @contextmanager
    def session_factory():
        yield session

    monkeypatch.setattr(debrid_manager_module, "db_session", session_factory)

    manager._requeue_task(
        42,
        error="No providers available for task",
        delay=timedelta(minutes=1),
        consume_attempt=False,
    )

    assert session.committed is True
    assert task.status == DebridTaskStatus.PENDING
    assert task.attempts == 0
    assert task.locked_at is None
    assert task.last_error == "No providers available for task"


def test_recover_stale_processing_tasks_after_restart_requeues_worker_state(monkeypatch):
    manager = DebridManager()
    manager._processing_stale_after = timedelta(seconds=30)
    task = Mock(
        status=DebridTaskStatus.PROCESSING,
        available_at=datetime.utcnow() - timedelta(minutes=5),
        locked_at=datetime.utcnow() - timedelta(minutes=5),
        updated_at=datetime.utcnow() - timedelta(minutes=5),
        last_error=None,
    )
    session = FakeStaleProcessingSession([task])
    debrid_manager_module = importlib.import_module("program.orchestrator.debrid_manager")

    @contextmanager
    def session_factory():
        yield session

    monkeypatch.setattr(debrid_manager_module, "db_session", session_factory)

    recovered = manager._recover_stale_processing_tasks()

    assert recovered == 1
    assert session.committed is True
    assert session.added == [task]
    assert task.status == DebridTaskStatus.PENDING
    assert task.locked_at is None
    assert task.last_error == "Recovered stale processing task after interrupted worker"
    assert isinstance(task.available_at, datetime)
    assert isinstance(task.updated_at, datetime)


def test_blacklist_blocked_stream_and_advance_queues_next_candidate(monkeypatch):
    manager = DebridManager()
    debrid_manager_module = importlib.import_module("program.orchestrator.debrid_manager")

    task = Mock(
        status=DebridTaskStatus.PROCESSING,
        provider_torrent_id="rd-1",
        provider_torrent_status="queued",
        acquiring_started_at=datetime.utcnow(),
        completed_at=None,
        locked_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
        last_error=None,
    )
    blocked_stream = Mock(infohash="hash-blocked")
    item = Mock(
        id=321,
        streams=[blocked_stream],
        log_string="JoJo S01E01",
    )
    item.blacklist_stream = Mock(return_value=True)

    first_session = FakePolicyBlockSession(task=task, item=item)
    second_session = FakePolicyBlockSession(task=None, item=item)
    sessions = [first_session, second_session]
    enqueued = []

    @contextmanager
    def session_factory():
        yield sessions.pop(0)

    monkeypatch.setattr(debrid_manager_module, "db_session", session_factory)

    db_package = importlib.import_module("program.db")
    db_functions_module = types.ModuleType("program.db.db_functions")
    db_functions_module.get_item_by_id = (
        lambda item_id, **_kwargs: item if item_id == 321 else None
    )
    monkeypatch.setitem(sys.modules, "program.db.db_functions", db_functions_module)
    monkeypatch.setattr(db_package, "db_functions", db_functions_module, raising=False)

    monkeypatch.setattr(
        manager,
        "enqueue_resolution_tasks",
        lambda queued_item, **kwargs: enqueued.append((queued_item.id, kwargs)) or 1,
    )

    handled = manager._blacklist_blocked_stream_and_advance(
        task_id=99,
        item_id=321,
        infohash="hash-blocked",
        error="Provider policy blocked this hash on realdebrid: [451] Infringing Torrent",
    )

    assert handled is False
    item.blacklist_stream.assert_called_once_with(blocked_stream)
    assert task.status == DebridTaskStatus.FAILED
    assert "Provider policy blocked this hash" in str(task.last_error)
    assert first_session.committed is True
    assert enqueued == [
        (
            321,
            {
                "trigger": DebridTaskTrigger.RETRY,
                "priority": DebridTaskPriority.NORMAL,
                "max_attempts": 3,
                "max_streams": 3,
            },
        )
    ]


def test_park_acquiring_task_persists_provider_torrent_state(monkeypatch):
    manager = DebridManager()
    task = Mock(
        attempts=1,
        max_attempts=3,
        status=DebridTaskStatus.PROCESSING,
        provider=None,
        provider_torrent_id=None,
        provider_torrent_status=None,
        acquiring_started_at=None,
        available_at=None,
        locked_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
        last_error=None,
    )
    session = FakeTaskSession(task)
    debrid_manager_module = importlib.import_module("program.orchestrator.debrid_manager")

    @contextmanager
    def session_factory():
        yield session

    monkeypatch.setattr(debrid_manager_module, "db_session", session_factory)

    manager._park_acquiring_task(
        42,
        provider="realdebrid",
        torrent_id="rd-123",
        provider_status="queued",
        delay=timedelta(minutes=1),
        error="Waiting for provider acquisition on realdebrid (status=queued)",
    )

    assert session.committed is True
    assert task.status == DebridTaskStatus.PENDING
    assert task.attempts == 0
    assert task.provider == "realdebrid"
    assert task.provider_torrent_id == "rd-123"
    assert task.provider_torrent_status == "queued"
    assert task.acquiring_started_at is not None
    assert task.locked_at is None


def test_park_acquiring_task_preserves_existing_acquiring_started_at(monkeypatch):
    manager = DebridManager()
    started_at = datetime.utcnow() - timedelta(minutes=5)
    task = Mock(
        attempts=1,
        max_attempts=3,
        status=DebridTaskStatus.PROCESSING,
        provider="realdebrid",
        provider_torrent_id="rd-123",
        provider_torrent_status="queued",
        acquiring_started_at=started_at,
        available_at=None,
        locked_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
        last_error=None,
    )
    session = FakeTaskSession(task)
    debrid_manager_module = importlib.import_module("program.orchestrator.debrid_manager")

    @contextmanager
    def session_factory():
        yield session

    monkeypatch.setattr(debrid_manager_module, "db_session", session_factory)

    manager._park_acquiring_task(
        42,
        provider="realdebrid",
        torrent_id="rd-123",
        provider_status="downloading",
        delay=timedelta(minutes=1),
        error="Waiting for provider acquisition on realdebrid (status=downloading)",
    )

    assert session.committed is True
    assert task.acquiring_started_at == started_at
    assert task.provider_torrent_status == "downloading"


def test_recover_stranded_scraped_items_requeues_terminal_items(monkeypatch):
    manager = DebridManager()
    manager._stranded_recovery_delay = timedelta(minutes=5)
    debrid_manager_module = importlib.import_module("program.orchestrator.debrid_manager")

    item = Mock(
        id=777,
        type="episode",
        last_state=States.Scraped,
        scraped_at=datetime.utcnow() - timedelta(hours=1),
        available_in_vfs=False,
        media_entry=None,
        streams=[Mock(infohash="hash-1", raw_title="One")],
        log_string="JoJo S05E16",
    )
    latest_task = Mock(
        status=DebridTaskStatus.FAILED,
        updated_at=datetime.utcnow() - timedelta(hours=1),
    )
    scan_session = FakeRecoveryScanSession([item], latest_task=latest_task)
    load_session = FakeRecoveryLoadSession()
    sessions = [scan_session, load_session]
    queued = []

    @contextmanager
    def session_factory():
        yield sessions.pop(0)

    monkeypatch.setattr(debrid_manager_module, "db_session", session_factory)
    monkeypatch.setattr(debrid_manager_module, "select", lambda *_args, **_kwargs: FakeQuery())

    media_item_module = types.ModuleType("program.media.item")
    media_item_module.MediaItem = type(
        "MediaItem",
        (),
        {
            "last_state": FakeColumn(),
            "type": FakeColumn(),
            "scraped_at": FakeColumn(),
            "id": FakeColumn(),
        },
    )
    monkeypatch.setitem(sys.modules, "program.media.item", media_item_module)

    db_package = importlib.import_module("program.db")
    db_functions_module = types.ModuleType("program.db.db_functions")
    db_functions_module.get_item_by_id = (
        lambda item_id, **_kwargs: item if item_id == 777 else None
    )
    monkeypatch.setitem(sys.modules, "program.db.db_functions", db_functions_module)
    monkeypatch.setattr(db_package, "db_functions", db_functions_module, raising=False)
    monkeypatch.setattr(
        manager,
        "enqueue_resolution_tasks",
        lambda queued_item, **kwargs: queued.append((queued_item.id, kwargs)) or 2,
    )

    recovered = manager._recover_stranded_scraped_items(limit=3)

    assert recovered == 1
    assert queued == [
        (
            777,
            {
                "trigger": DebridTaskTrigger.RETRY,
                "priority": DebridTaskPriority.NORMAL,
                "max_attempts": 3,
                "max_streams": 3,
            },
        )
    ]


def test_recover_stranded_scraped_items_requeues_immediately_after_rescrape(monkeypatch):
    manager = DebridManager()
    manager._stranded_recovery_delay = timedelta(minutes=30)
    debrid_manager_module = importlib.import_module("program.orchestrator.debrid_manager")

    now = datetime.utcnow()
    item = Mock(
        id=778,
        type="season",
        last_state=States.Scraped,
        scraped_at=now,
        available_in_vfs=False,
        media_entry=None,
        streams=[Mock(infohash="hash-2", raw_title="Two")],
        log_string="JoJo S03",
    )
    latest_task = Mock(
        status=DebridTaskStatus.FAILED,
        updated_at=now - timedelta(minutes=1),
    )
    scan_session = FakeRecoveryScanSession([item], latest_task=latest_task)
    load_session = FakeRecoveryLoadSession()
    sessions = [scan_session, load_session]
    queued = []

    @contextmanager
    def session_factory():
        yield sessions.pop(0)

    monkeypatch.setattr(debrid_manager_module, "db_session", session_factory)
    monkeypatch.setattr(debrid_manager_module, "select", lambda *_args, **_kwargs: FakeQuery())

    media_item_module = types.ModuleType("program.media.item")
    media_item_module.MediaItem = type(
        "MediaItem",
        (),
        {
            "last_state": FakeColumn(),
            "type": FakeColumn(),
            "scraped_at": FakeColumn(),
            "id": FakeColumn(),
        },
    )
    monkeypatch.setitem(sys.modules, "program.media.item", media_item_module)

    db_package = importlib.import_module("program.db")
    db_functions_module = types.ModuleType("program.db.db_functions")
    db_functions_module.get_item_by_id = (
        lambda item_id, **_kwargs: item if item_id == 778 else None
    )
    monkeypatch.setitem(sys.modules, "program.db.db_functions", db_functions_module)
    monkeypatch.setattr(db_package, "db_functions", db_functions_module, raising=False)
    monkeypatch.setattr(
        manager,
        "enqueue_resolution_tasks",
        lambda queued_item, **kwargs: queued.append((queued_item.id, kwargs)) or 1,
    )

    recovered = manager._recover_stranded_scraped_items(limit=3)

    assert recovered == 1
    assert queued == [
        (
            778,
            {
                "trigger": DebridTaskTrigger.RETRY,
                "priority": DebridTaskPriority.NORMAL,
                "max_attempts": 3,
                "max_streams": 3,
            },
        )
    ]


def test_probe_provider_caches_parallel_returns_acquiring_without_negative_cache(monkeypatch):
    manager = DebridManager()
    service_a = Mock(key="realdebrid")
    service_b = Mock(key="alldebrid")
    saved = []

    class FakeWrapper:
        def check_cache(self, service, infohash, *, item, stream, allow_pending=False):
            _ = item, stream
            assert allow_pending is True
            if service.key == "realdebrid":
                return ProviderCacheResult(
                    infohash=infohash,
                    provider=service.key,
                    status=ProviderResolveStatus.ACQUIRING,
                    container=Mock(
                        files=[],
                        torrent_id="rd-123",
                        torrent_info=Mock(status="queued"),
                    ),
                )
            return ProviderCacheResult(
                infohash=infohash,
                provider=service.key,
                status=ProviderResolveStatus.NOT_CACHED,
                container=None,
            )

    monkeypatch.setattr(
        manager,
        "save_resolution",
        lambda _infohash, provider, status: saved.append((provider, status)),
    )

    provider, cache_result, error, policy_blocked = manager._probe_provider_caches_parallel(
        FakeWrapper(),
        [service_a, service_b],
        "abc123",
        Mock(id=1, type="episode", log_string="Episode"),
        Mock(infohash="abc123"),
    )

    assert provider is not None
    assert provider.key == "realdebrid"
    assert cache_result is not None
    assert cache_result.is_acquiring is True
    assert policy_blocked is False
    assert saved == [("alldebrid", DebridCacheStatus.NOT_FOUND)]


def test_build_provider_task_lanes_keeps_provider_affinity_for_pending_acquisition():
    manager = DebridManager()
    services = [Mock(key="realdebrid"), Mock(key="alldebrid")]
    due_task = DueTaskCandidate(
        task_id=99,
        infohash="hash-jojo",
        priority=DebridTaskPriority.NORMAL,
        available_at=datetime.utcnow(),
        provider="realdebrid",
        provider_torrent_id="rd-123",
        provider_torrent_status="downloading",
        acquiring_started_at=datetime.utcnow() - timedelta(minutes=2),
    )

    lanes = manager._build_provider_task_lanes([due_task], services, limit=1)

    assert lanes == {"realdebrid": [99]}

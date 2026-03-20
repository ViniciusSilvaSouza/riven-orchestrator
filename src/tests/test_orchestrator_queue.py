import importlib
import importlib.util
import sys
import types
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import Mock

from program.orchestrator.debrid_manager import DebridManager
from program.orchestrator.models import (
    DebridResolutionTask,
    DebridTaskPriority,
    DebridTaskStatus,
    DebridTaskTrigger,
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

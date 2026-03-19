from contextlib import contextmanager
import importlib
from unittest.mock import Mock

from program.media.state import States
from program.orchestrator.debrid_manager import DebridManager
from program.orchestrator.models import (
    DebridResolutionTask,
    DebridTaskPriority,
    DebridTaskStatus,
    DebridTaskTrigger,
)


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

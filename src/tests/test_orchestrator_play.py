from contextlib import contextmanager
from unittest.mock import Mock
import importlib

from program.orchestrator.debrid_manager import DebridManager


def test_resolve_on_play_requires_initialized_downloader():
    manager = DebridManager()
    program = Mock()
    program.services = Mock()
    program.services.downloader = Mock(initialized=False)

    result = manager.resolve_on_play(program, item_id=10)

    assert result.success is False
    assert result.status_code == 503


def test_resolve_on_play_returns_not_found_when_item_missing():
    manager = DebridManager()
    program = Mock()
    program.services = Mock()
    program.services.downloader = Mock(initialized=True)

    manager._get_item_play_snapshot = Mock(
        return_value={
            "exists": False,
            "resolved": False,
            "provider": None,
            "infohash": None,
            "last_state": None,
            "open_tasks": 0,
            "last_error": None,
        }
    )

    result = manager.resolve_on_play(program, item_id=11)

    assert result.success is False
    assert result.status_code == 404


def test_resolve_on_play_returns_immediate_success_when_already_resolved():
    manager = DebridManager()
    program = Mock()
    program.services = Mock()
    program.services.downloader = Mock(initialized=True)

    manager._get_item_play_snapshot = Mock(
        return_value={
            "exists": True,
            "resolved": True,
            "provider": "realdebrid",
            "infohash": "abc123",
            "last_state": "States.Completed",
            "open_tasks": 0,
            "last_error": None,
        }
    )

    result = manager.resolve_on_play(program, item_id=12)

    assert result.success is True
    assert result.status_code == 200
    assert result.provider == "realdebrid"
    assert result.infohash == "abc123"


def test_resolve_on_play_requires_scraped_state(monkeypatch):
    from program.media.state import States

    manager = DebridManager()
    debrid_manager_module = importlib.import_module("program.orchestrator.debrid_manager")
    db_functions_module = importlib.import_module("program.db.db_functions")

    item = Mock()
    item.last_state = States.Requested
    item.id = 13

    class FakeSession:
        def merge(self, value):
            return value

    @contextmanager
    def session_factory():
        yield FakeSession()

    monkeypatch.setattr(debrid_manager_module, "db_session", session_factory)
    monkeypatch.setattr(
        db_functions_module,
        "get_item_by_id",
        lambda item_id, session=None: item,
    )

    manager._get_item_play_snapshot = Mock(
        return_value={
            "exists": True,
            "resolved": False,
            "provider": None,
            "infohash": None,
            "last_state": "States.Requested",
            "open_tasks": 0,
            "last_error": None,
        }
    )

    program = Mock()
    program.services = Mock()
    program.services.downloader = Mock(initialized=True)

    result = manager.resolve_on_play(program, item_id=13)

    assert result.success is False
    assert result.status_code == 409

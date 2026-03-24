from concurrent.futures import Future
from contextlib import contextmanager
from datetime import datetime, timedelta
import threading
from types import SimpleNamespace

from program.managers.event_manager import EventManager, FutureWithEvent
from program.types import Event


def _content_item(*, item_id=None, tvdb_id=None, tmdb_id=None, imdb_id=None):
    return SimpleNamespace(
        id=item_id,
        tvdb_id=tvdb_id,
        tmdb_id=tmdb_id,
        imdb_id=imdb_id,
        log_string=f"TVDB ID {tvdb_id}" if tvdb_id else "content-item",
    )


def test_add_event_prunes_stale_content_only_event(monkeypatch):
    manager = EventManager()
    stale_item = _content_item(tvdb_id="262954")
    stale_event = Event(
        emitted_by="Overseerr",
        content_item=stale_item,
        run_at=datetime.now() - timedelta(minutes=5),
    )
    manager.add_event_to_queue(stale_event, log_message=False)

    fresh_item = _content_item(tvdb_id="262954")

    monkeypatch.setattr(
        "program.managers.event_manager.db_functions.item_exists_by_any_id",
        lambda *args, **kwargs: False,
    )

    added = manager.add_event(Event(emitted_by="Overseerr", content_item=fresh_item))

    assert added is True
    assert len(manager._queued_events) == 1
    assert manager._queued_events[0] is not stale_event
    assert manager._queued_events[0].content_item.tvdb_id == "262954"


def test_cancel_job_removes_matching_content_only_event(monkeypatch):
    manager = EventManager()
    item = _content_item(item_id=42, tvdb_id="262954", imdb_id="tt123")
    manager.add_event_to_queue(
        Event(emitted_by="Overseerr", content_item=_content_item(tvdb_id="262954")),
        log_message=False,
    )

    class FakeSession:
        def get(self, _model, item_id):
            if item_id == 42:
                return item
            return None

    @contextmanager
    def fake_db_session():
        yield FakeSession()

    monkeypatch.setattr("program.managers.event_manager.db_session", fake_db_session)
    monkeypatch.setattr(
        "program.managers.event_manager.db_functions.get_item_ids",
        lambda session, item_id: (item_id, []),
    )

    manager.cancel_job(42)

    assert manager._queued_events == []


def test_get_event_updates_uses_filesystem_service_stage():
    manager = EventManager()
    future = Future()
    future.set_result(None)

    class FilesystemService:
        pass

    manager._futures.append(
        FutureWithEvent(
            future=future,
            event=Event(emitted_by=FilesystemService(), item_id=77),
            cancellation_event=threading.Event(),
        )
    )

    updates = manager.get_event_updates()

    assert updates["FilesystemService"] == [77]
    assert "Symlinker" not in updates

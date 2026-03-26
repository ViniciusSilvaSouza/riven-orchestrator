from concurrent.futures import Future
from contextlib import contextmanager
from datetime import datetime, timedelta
import threading
from types import SimpleNamespace

from program.managers.event_manager import EventManager, FutureWithEvent
from program.media.item import MediaItem
from program.media.state import States
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


def test_add_item_requeues_existing_placeholder_for_indexing(monkeypatch):
    manager = EventManager()
    placeholder = MediaItem({"tmdb_id": "329865"})
    placeholder.id = 600
    placeholder.type = "mediaitem"

    monkeypatch.setattr(
        "program.managers.event_manager.db_functions.get_item_by_external_id",
        lambda **kwargs: placeholder,
    )
    monkeypatch.setattr(
        "program.managers.event_manager.db_functions.get_item_by_id",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        "program.managers.event_manager.db_functions.item_exists_by_any_id",
        lambda *args, **kwargs: False,
    )

    added = manager.add_item(placeholder, service="Overseerr")

    assert added is True
    assert len(manager._queued_events) == 1
    assert manager._queued_events[0].content_item is placeholder


def test_add_item_requeues_existing_overseerr_show_by_item_id(monkeypatch):
    manager = EventManager()
    existing_show = MediaItem({"tmdb_id": "45790"})
    existing_show.id = 401
    existing_show.type = "show"
    existing_show.last_state = States.Indexed

    monkeypatch.setattr(
        "program.managers.event_manager.db_functions.get_item_by_external_id",
        lambda **kwargs: existing_show,
    )
    monkeypatch.setattr(
        "program.managers.event_manager.db_functions.get_item_by_id",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        "program.managers.event_manager.db_functions.get_item_ids",
        lambda *_args, **_kwargs: (401, []),
    )
    monkeypatch.setattr(
        manager,
        "add_event_to_queue",
        lambda event: manager._queued_events.append(event),
    )

    added = manager.add_item(existing_show, service="Overseerr")

    assert added is True
    assert len(manager._queued_events) == 1
    assert manager._queued_events[0].item_id == 401
    assert manager._queued_events[0].content_item is None


def test_add_event_allows_followup_stage_when_item_is_running(monkeypatch):
    manager = EventManager()
    manager.add_event_to_running(Event(emitted_by="Scraping", item_id=404))

    class FakeSession:
        pass

    @contextmanager
    def fake_db_session():
        yield FakeSession()

    monkeypatch.setattr("program.managers.event_manager.db_session", fake_db_session)
    monkeypatch.setattr(
        "program.managers.event_manager.db_functions.get_item_ids",
        lambda *_args, **_kwargs: (404, []),
    )
    monkeypatch.setattr(
        manager,
        "add_event_to_queue",
        lambda event: manager._queued_events.append(event),
    )

    added = manager.add_event(Event(emitted_by="Downloader", item_id=404))

    assert added is True
    assert len(manager._queued_events) == 1
    assert manager._queued_events[0].item_id == 404
    assert manager._queued_events[0].emitted_by == "Downloader"


def test_add_event_skips_same_stage_when_item_is_running(monkeypatch):
    manager = EventManager()
    manager.add_event_to_running(Event(emitted_by="Scraping", item_id=404))

    class FakeSession:
        pass

    @contextmanager
    def fake_db_session():
        yield FakeSession()

    monkeypatch.setattr("program.managers.event_manager.db_session", fake_db_session)
    monkeypatch.setattr(
        "program.managers.event_manager.db_functions.get_item_ids",
        lambda *_args, **_kwargs: (404, []),
    )
    monkeypatch.setattr(
        manager,
        "add_event_to_queue",
        lambda event: manager._queued_events.append(event),
    )

    added = manager.add_event(Event(emitted_by="Scraping", item_id=404))

    assert added is False
    assert manager._queued_events == []


def test_process_future_allows_same_stage_requeue(monkeypatch):
    manager = EventManager()
    original_event = Event(emitted_by="Scraping", item_id=777)
    manager.add_event_to_running(original_event)

    class FakeSession:
        pass

    @contextmanager
    def fake_db_session():
        yield FakeSession()

    monkeypatch.setattr("program.managers.event_manager.db_session", fake_db_session)
    monkeypatch.setattr(
        "program.managers.event_manager.db_functions.get_item_ids",
        lambda *_args, **_kwargs: (777, []),
    )
    monkeypatch.setattr(
        manager,
        "add_event_to_queue",
        lambda event: manager._queued_events.append(event),
    )

    future = Future()
    future.set_result(777)
    future_with_event = FutureWithEvent(
        future=future,
        event=original_event,
        cancellation_event=threading.Event(),
    )
    manager._futures.append(future_with_event)

    class Scraping:
        pass

    manager._process_future(future_with_event, Scraping())

    assert len(manager._queued_events) == 1
    assert manager._queued_events[0].item_id == 777
    assert manager._stage_key(manager._queued_events[0].emitted_by) == "Scraping"
    assert manager._running_events == []

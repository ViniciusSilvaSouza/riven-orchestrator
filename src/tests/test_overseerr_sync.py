import threading
from contextlib import contextmanager
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import Mock

from program.apis.overseerr_api import OverseerrAPI
from program.core.runner import RunnerResult
from program.db import db_functions
from program.media.item import Episode, MediaItem, Movie, Season, Show
from program.services.content.overseerr import Overseerr
from program.types import Event


def _build_show(
    *,
    requested_seasons: list[int] | None,
    completed_seasons: set[int],
) -> Show:
    show = Show(
        {
            "title": "Test Show",
            "requested_by": "overseerr",
            "requested_id": 101,
            "overseerr_id": 202,
            "requested_seasons": requested_seasons,
            "seasons": [],
        }
    )

    seasons = []

    for season_number in sorted(requested_seasons or [1, 2]):
        episode = Episode(
            {
                "number": 1,
                "updated": season_number in completed_seasons,
            }
        )
        season = Season({"number": season_number, "episodes": [episode]})
        episode.parent = season
        season.parent = show
        seasons.append(season)

    show.seasons = seasons
    return show


def _build_overseerr_service() -> tuple[Overseerr, SimpleNamespace]:
    api = SimpleNamespace(
        MEDIA_STATUS_AVAILABLE="available",
        MEDIA_STATUS_PARTIALLY_AVAILABLE="partially_available",
        update_media_status=Mock(return_value=True),
    )
    service = Overseerr.__new__(Overseerr)
    service.key = "overseerr"
    service.initialized = True
    service.settings = SimpleNamespace(sync_status=True)
    service.api = api
    return service, api


def test_build_media_item_maps_request_and_media_ids():
    request = {
        "id": "42",
        "media": {
            "id": "24",
            "tvdbId": 81797,
        },
        "seasons": [{"seasonNumber": 1}, {"seasonNumber": "2"}, {"number": 2}],
    }

    item = OverseerrAPI.build_media_item("overseerr", request)

    assert item is not None
    assert item.requested_by == "overseerr"
    assert item.requested_id == 42
    assert item.overseerr_id == 24
    assert item.tvdb_id == 81797
    assert item.requested_seasons == [1, 2]


def test_sync_availability_marks_movie_available():
    service, api = _build_overseerr_service()
    movie = Movie(
        {
            "title": "Test Movie",
            "requested_by": "overseerr",
            "requested_id": 111,
            "overseerr_id": 222,
            "updated": True,
        }
    )

    synced = service.sync_availability(movie)

    assert synced is True
    api.update_media_status.assert_called_once_with(222, "available")


def test_sync_availability_marks_show_partially_available_until_requested_scope_ready():
    service, api = _build_overseerr_service()
    show = _build_show(requested_seasons=[1, 2], completed_seasons={1})

    synced = service.sync_availability(show)

    assert synced is True
    api.update_media_status.assert_called_once_with(202, "partially_available")


def test_sync_availability_marks_show_available_once_requested_scope_is_complete():
    service, api = _build_overseerr_service()
    show = _build_show(requested_seasons=[1, 2], completed_seasons={1, 2})

    synced = service.sync_availability(show)

    assert synced is True
    api.update_media_status.assert_called_once_with(202, "available")


class FakeContentIndexSession:
    def __init__(self, existing_item):
        self.existing_item = existing_item
        self.added = []
        self.deleted = []
        self.flushed = False
        self.committed = False
        self.rolled_back = False

    def merge(self, item):
        return item

    def add(self, obj):
        self.added.append(obj)

    def delete(self, obj):
        self.deleted.append(obj)

    def flush(self):
        self.flushed = True

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True


class FakeLookupSession:
    def __init__(self):
        self.params = None

    def execute(self, query):
        self.params = query.compile().params

        class _Result:
            def unique(self_inner):
                return self_inner

            def scalar_one_or_none(self_inner):
                return None

        return _Result()


def test_get_item_by_external_id_normalizes_numeric_ids_to_strings():
    session = FakeLookupSession()

    result = db_functions.get_item_by_external_id(tmdb_id=329865, session=session)

    assert result is None
    assert session.params["tmdb_id_1"] == "329865"


def test_run_thread_with_db_item_upgrades_placeholder_mediaitem(monkeypatch):
    placeholder = MediaItem(
        {
            "tmdb_id": "157336",
            "requested_by": "overseerr",
            "requested_id": 8,
            "requested_at": datetime(2026, 3, 24, 1, 37, 19),
            "requested_seasons": None,
            "overseerr_id": 9,
        }
    )
    placeholder.id = 599
    placeholder.type = "mediaitem"

    indexed_movie = Movie(
        {
            "title": "Interestelar",
            "tmdb_id": "157336",
            "imdb_id": "tt0816692",
        }
    )

    session = FakeContentIndexSession(placeholder)

    @contextmanager
    def session_factory():
        yield session

    def fake_indexer(_item):
        yield RunnerResult(media_items=[indexed_movie])

    monkeypatch.setattr(db_functions, "db_session", session_factory)
    lookup = {}

    def fake_get_item_by_external_id(**kwargs):
        lookup.update(kwargs)
        return placeholder

    monkeypatch.setattr(
        db_functions,
        "get_item_by_external_id",
        fake_get_item_by_external_id,
    )

    result = db_functions.run_thread_with_db_item(
        fake_indexer,
        Overseerr.__new__(Overseerr),
        SimpleNamespace(),
        Event(emitted_by="Overseerr", content_item=placeholder),
        threading.Event(),
    )

    assert result == 599
    assert session.deleted == [placeholder]
    assert session.flushed is True
    assert session.added == [indexed_movie]
    assert indexed_movie.id == 599
    assert lookup["item_types"] == ["movie", "show", "mediaitem"]
    assert indexed_movie.requested_by == "overseerr"
    assert indexed_movie.requested_id == 8
    assert indexed_movie.overseerr_id == 9
    assert session.committed is True

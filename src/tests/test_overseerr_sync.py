from types import SimpleNamespace
from unittest.mock import Mock

from program.apis.overseerr_api import OverseerrAPI
from program.media.item import Episode, Movie, Season, Show
from program.services.content.overseerr import Overseerr


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

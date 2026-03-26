from types import MethodType, SimpleNamespace

from program.media.item import Episode, Season, Show
from program.services.downloaders import Downloader


def _build_show_with_two_seasons() -> tuple[Show, Season, Season]:
    show = Show(
        {
            "imdb_id": "tt1405406",
            "requested_by": "user",
            "title": "Test Show",
        }
    )
    season_1 = Season({"number": 1})
    season_2 = Season({"number": 2})
    season_1.add_episode(Episode({"number": 1}))
    season_2.add_episode(Episode({"number": 1}))
    show.add_season(season_1)
    show.add_season(season_2)
    return show, season_1, season_2


def _build_downloader_with_capture() -> tuple[Downloader, list[int]]:
    downloader = object.__new__(Downloader)
    downloader.service = SimpleNamespace(key="realdebrid")
    matched_seasons: list[int] = []

    def _capture_update(self, item, *_args, **_kwargs):
        if isinstance(item, Episode):
            matched_seasons.append(item.parent.number)

    downloader._update_attributes = MethodType(_capture_update, downloader)
    return downloader, matched_seasons


def test_match_file_to_item_prefers_requested_season_context_without_sxx():
    downloader, matched_seasons = _build_downloader_with_capture()
    show, _, requested_season = _build_show_with_two_seasons()
    file_data = SimpleNamespace(type="episode", seasons=[], episodes=[1])
    file = SimpleNamespace(filename="Test Show - 01.mkv")
    download_result = SimpleNamespace(
        infohash="abc123",
        info=SimpleNamespace(id="torrent-id"),
    )

    found = downloader.match_file_to_item(
        item=requested_season,
        file_data=file_data,
        file=file,
        download_result=download_result,
        show=show,
        episode_cap=100,
        processed_episode_ids=set(),
        service=downloader.service,
    )

    assert found is True
    assert matched_seasons == [2]


def test_match_file_to_item_show_keeps_absolute_fallback_without_sxx():
    downloader, matched_seasons = _build_downloader_with_capture()
    show, _, _ = _build_show_with_two_seasons()
    # Absolute episode 2 should map to season 2 episode 1.
    file_data = SimpleNamespace(type="episode", seasons=[], episodes=[2])
    file = SimpleNamespace(filename="Test Show - 02.mkv")
    download_result = SimpleNamespace(
        infohash="def456",
        info=SimpleNamespace(id="torrent-id"),
    )

    found = downloader.match_file_to_item(
        item=show,
        file_data=file_data,
        file=file,
        download_result=download_result,
        show=show,
        episode_cap=100,
        processed_episode_ids=set(),
        service=downloader.service,
    )

    assert found is True
    assert matched_seasons == [2]

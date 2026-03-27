from types import SimpleNamespace

from program.media.item import Episode, Season, Show
from program.services.scrapers.prowlarr import Prowlarr, SearchParams


def _build_show_tree() -> tuple[Show, Season, Episode]:
    show = Show(
        {
            "title": "The Rookie",
            "imdb_id": "tt7587890",
            "requested_by": "tester",
            "type": "show",
        }
    )
    season = Season({"number": 3, "type": "season"})
    episode = Episode({"number": 14, "type": "episode"})
    show.add_season(season)
    season.add_episode(episode)
    return show, season, episode


def test_build_episode_search_prefers_imdb_id_when_supported():
    _, _, episode = _build_show_tree()
    search_params = SearchParams(
        search=["q"],
        movie=["q"],
        tv=["q", "season", "ep", "imdbId"],
    )

    query, search_type, season, episode_number = Prowlarr._build_episode_search(
        search_params,
        episode,
        "The Rookie",
        "TestIndexer",
    )

    assert query == "tt7587890"
    assert search_type == "tv-search"
    assert season == 3
    assert episode_number == 14


def test_build_season_search_prefers_imdb_id_when_supported():
    _, season, _ = _build_show_tree()
    search_params = SearchParams(
        search=["q"],
        movie=["q"],
        tv=["q", "season", "imdbId"],
    )

    query, search_type, season_number, episode_number = Prowlarr._build_season_search(
        search_params,
        season,
        "The Rookie",
        "TestIndexer",
    )

    assert query == "tt7587890"
    assert search_type == "tv-search"
    assert season_number == 3
    assert episode_number is None


def test_anime_only_indexer_detection_includes_known_anime_sources():
    assert Prowlarr._is_anime_only_indexer(SimpleNamespace(name="Tokyo Toshokan"))
    assert Prowlarr._is_anime_only_indexer(SimpleNamespace(name="Shana Project"))
    assert not Prowlarr._is_anime_only_indexer(SimpleNamespace(name="YTS"))


def test_get_item_categories_maps_episode_to_tv_categories():
    _, _, episode = _build_show_tree()
    indexer = SimpleNamespace(
        capabilities=SimpleNamespace(
            categories=[
                SimpleNamespace(type="movie", ids=[2000]),
                SimpleNamespace(type="tv", ids=[5000, 5030]),
                SimpleNamespace(type="anime", ids=[5070]),
            ]
        )
    )

    categories = Prowlarr._get_item_categories(indexer, episode)
    assert sorted(categories) == [5000, 5030]


def test_get_item_categories_includes_anime_bucket_for_anime_episode():
    _, _, episode = _build_show_tree()
    episode.is_anime = True
    indexer = SimpleNamespace(
        capabilities=SimpleNamespace(
            categories=[
                SimpleNamespace(type="tv", ids=[5000]),
                SimpleNamespace(type="anime", ids=[5070]),
            ]
        )
    )

    categories = Prowlarr._get_item_categories(indexer, episode)
    assert sorted(categories) == [5000, 5070]

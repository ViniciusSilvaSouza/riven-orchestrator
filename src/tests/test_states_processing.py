from types import SimpleNamespace

import pytest
from kink import di
from kink.errors.service_error import ServiceError

from program.media.item import Episode, Movie, Season, Show
from program.media.state import States
from program.program import Program
from program.state_transition import process_event


class DummyService:
    def __init__(self, name: str):
        self.name = name

    def __repr__(self) -> str:
        return self.name


class DummyScrapingService(DummyService):
    def __init__(self):
        super().__init__("scraping")
        self.submit_by_type = {
            "movie": True,
            "show": True,
            "season": True,
            "episode": True,
            "mediaitem": True,
        }

    def should_submit(self, item) -> bool:
        return self.submit_by_type.get(getattr(item, "type", "mediaitem"), True)


@pytest.fixture
def services():
    had_previous_program = Program in di
    previous_program = None
    if had_previous_program:
        try:
            previous_program = di[Program]
        except ServiceError:
            had_previous_program = False

    current_services = SimpleNamespace(
        indexer=DummyService("indexer"),
        scraping=DummyScrapingService(),
        downloader=DummyService("downloader"),
        filesystem=DummyService("filesystem"),
        updater=DummyService("updater"),
        post_processing=DummyService("post_processing"),
    )
    di[Program] = SimpleNamespace(services=current_services)

    try:
        yield current_services
    finally:
        if had_previous_program and previous_program is not None:
            di[Program] = previous_program
        else:
            di._services.pop(Program, None)
            di._memoized_services.pop(Program, None)


def _build_show() -> Show:
    show = Show({"imdb_id": "tt0903747", "requested_by": "Iceberg"})
    season_one = Season({"number": 1})
    season_two = Season({"number": 2})
    episode_one = Episode({"number": 1})
    episode_two = Episode({"number": 1})
    season_one.add_episode(episode_one)
    season_two.add_episode(episode_two)
    show.add_season(season_one)
    show.add_season(season_two)
    return show


def test_process_event_routes_requested_content_item_to_indexer(services):
    movie = Movie({"imdb_id": "tt1375666", "requested_by": "Iceberg"})

    processed_event = process_event("Manual", content_item=movie)

    assert processed_event.service is services.indexer
    assert list(processed_event.related_media_items) == [movie]


def test_process_event_routes_indexed_movie_to_scraping(services):
    movie = Movie({"imdb_id": "tt1375666", "requested_by": "Iceberg"})
    movie.last_state = States.Indexed

    processed_event = process_event("Manual", existing_item=movie)

    assert processed_event.service is services.scraping
    assert list(processed_event.related_media_items) == [movie]


def test_process_event_breaks_show_into_incomplete_seasons_for_scraping(services):
    show = _build_show()
    show.last_state = States.Indexed
    show.seasons[0].last_state = States.Indexed
    show.seasons[1].last_state = States.Completed
    services.scraping.submit_by_type["show"] = False

    processed_event = process_event("Manual", existing_item=show)

    assert processed_event.service is services.scraping
    assert list(processed_event.related_media_items) == [show.seasons[0]]


def test_process_event_allows_scraping_emitted_retry_when_due(services):
    movie = Movie({"imdb_id": "tt1375666", "requested_by": "Iceberg"})
    movie.last_state = States.Indexed

    processed_event = process_event(services.scraping, existing_item=movie)

    assert processed_event.service is services.scraping
    assert list(processed_event.related_media_items) == [movie]


@pytest.mark.parametrize(
    ("state", "expected_service_name"),
    [
        (States.Scraped, "downloader"),
        (States.Downloaded, "filesystem"),
        (States.Symlinked, "updater"),
        (States.Completed, "post_processing"),
    ],
)
def test_process_event_routes_terminal_pipeline_states(
    services,
    state,
    expected_service_name,
):
    movie = Movie({"imdb_id": "tt1375666", "requested_by": "Iceberg"})
    movie.last_state = state

    processed_event = process_event("Manual", existing_item=movie)

    assert processed_event.service is getattr(services, expected_service_name)
    assert list(processed_event.related_media_items) == [movie]


def test_process_event_stops_after_post_processing(services):
    movie = Movie({"imdb_id": "tt1375666", "requested_by": "Iceberg"})
    movie.last_state = States.Completed

    processed_event = process_event(
        services.post_processing,
        existing_item=movie,
    )

    assert processed_event.service is None
    assert list(processed_event.related_media_items) == []


def test_process_event_skips_paused_items(services):
    movie = Movie({"imdb_id": "tt1375666", "requested_by": "Iceberg"})
    movie.last_state = States.Paused

    processed_event = process_event("Manual", existing_item=movie)

    assert processed_event.service is None
    assert list(processed_event.related_media_items) == []

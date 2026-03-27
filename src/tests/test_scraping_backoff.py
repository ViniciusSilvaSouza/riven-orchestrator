from datetime import datetime, timedelta

import program.services.scrapers as scraping_module
from program.media.item import Movie
from program.media.state import States
from program.media.stream import Stream
from program.services.scrapers import Scraping
from program.settings import settings_manager


class _FakeParsedData:
    def __init__(self, parsed_title: str = "Fake", resolution: str = "1080p"):
        self.parsed_title = parsed_title
        self.resolution = resolution


class _FakeTorrent:
    def __init__(self, infohash: str):
        self.infohash = infohash
        self.raw_title = "Fake.Title.1080p"
        self.data = _FakeParsedData()
        self.rank = 100
        self.lev_ratio = 0.9


def test_scrape_backoff_is_progressive(monkeypatch):
    scraping = Scraping.__new__(Scraping)

    monkeypatch.setattr(settings_manager.settings.scraping, "after_2", 2.0, raising=False)
    monkeypatch.setattr(settings_manager.settings.scraping, "after_5", 6.0, raising=False)
    monkeypatch.setattr(settings_manager.settings.scraping, "after_10", 24.0, raising=False)

    delay_1 = scraping._scrape_backoff_seconds(1)
    delay_2 = scraping._scrape_backoff_seconds(2)
    delay_5 = scraping._scrape_backoff_seconds(5)
    delay_6 = scraping._scrape_backoff_seconds(6)

    assert delay_1 == 30 * 60
    assert delay_1 < delay_2 < delay_5
    assert delay_5 <= delay_6
    # Second retry should not jump straight to 2 hours anymore.
    assert delay_2 < 2 * 60 * 60


def test_scraping_run_schedules_retry_on_no_new_streams(monkeypatch):
    monkeypatch.setattr(scraping_module.logger, "log", lambda *args, **kwargs: None)
    scraping = Scraping.__new__(Scraping)
    scraping.max_failed_attempts = 0

    monkeypatch.setattr(scraping, "scrape", lambda _item: {})
    monkeypatch.setattr(settings_manager.settings.scraping, "after_2", 2.0, raising=False)
    monkeypatch.setattr(settings_manager.settings.scraping, "after_5", 6.0, raising=False)
    monkeypatch.setattr(settings_manager.settings.scraping, "after_10", 24.0, raising=False)

    item = Movie({"imdb_id": "tt1375666", "requested_by": "Iceberg"})
    item.last_state = States.Indexed
    item.scraped_times = 0
    item.failed_attempts = 0
    item.scraped_at = datetime.now() - timedelta(hours=2)

    result = next(scraping.run(item))

    assert result.run_at is not None
    assert result.run_at > datetime.now()
    assert item.scraped_times == 1
    assert item.failed_attempts == 1


def test_scraping_run_does_not_increment_failed_attempts_for_known_candidates(monkeypatch):
    monkeypatch.setattr(scraping_module.logger, "log", lambda *args, **kwargs: None)
    scraping = Scraping.__new__(Scraping)
    scraping.max_failed_attempts = 0

    known_stream = Stream(_FakeTorrent("abc123"))
    monkeypatch.setattr(scraping, "scrape", lambda _item: {"abc123": known_stream})
    monkeypatch.setattr(settings_manager.settings.scraping, "after_2", 2.0, raising=False)
    monkeypatch.setattr(settings_manager.settings.scraping, "after_5", 6.0, raising=False)
    monkeypatch.setattr(settings_manager.settings.scraping, "after_10", 24.0, raising=False)

    item = Movie({"imdb_id": "tt1375666", "requested_by": "Iceberg"})
    item.last_state = States.Indexed
    item.scraped_times = 0
    item.failed_attempts = 2
    item.scraped_at = datetime.now() - timedelta(hours=2)
    item.streams.append(known_stream)

    result = next(scraping.run(item))

    assert result.run_at is not None
    assert item.scraped_times == 1
    assert item.failed_attempts == 2

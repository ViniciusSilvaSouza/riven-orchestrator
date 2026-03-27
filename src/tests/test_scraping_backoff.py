from datetime import datetime, timedelta

from program.media.item import Movie
from program.media.state import States
from program.services.scrapers import Scraping
from program.settings import settings_manager


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

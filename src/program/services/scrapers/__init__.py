import threading
from collections.abc import Generator
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from queue import Queue, Empty


from loguru import logger

from program.core.runner import MediaItemGenerator, Runner, RunnerResult
from program.media.item import MediaItem
from program.media.state import States
from program.media.stream import Stream
from program.services.scrapers.aiostreams import AIOStreams
from program.services.scrapers.base import ScraperService
from program.services.scrapers.comet import Comet
from program.services.scrapers.jackett import Jackett
from program.services.scrapers.mediafusion import Mediafusion
from program.services.scrapers.orionoid import Orionoid
from program.services.scrapers.prowlarr import Prowlarr
from program.services.scrapers.rarbg import Rarbg
from program.services.scrapers.shared import ParseDiagnostics, parse_results
from program.services.scrapers.torrentio import Torrentio
from program.services.scrapers.zilean import Zilean
from program.settings import settings_manager
from program.settings.models import Observable, ScraperModel


def _log_scraper(message: str) -> None:
    try:
        logger.log("SCRAPER", message)
    except ValueError:
        logger.info(message)


class Scraping(Runner[ScraperModel, ScraperService[Observable]]):
    def __init__(self):
        super().__init__()

        self.initialized = False
        self.settings = settings_manager.settings.scraping
        self.max_failed_attempts = (
            settings_manager.settings.scraping.max_failed_attempts
        )

        self.services = {
            AIOStreams: AIOStreams(),
            Comet: Comet(),
            Jackett: Jackett(),
            Mediafusion: Mediafusion(),
            Orionoid: Orionoid(),
            Prowlarr: Prowlarr(),
            Rarbg: Rarbg(),
            Torrentio: Torrentio(),
            Zilean: Zilean(),
        }

        self.initialized_services = [
            service for service in self.services.values() if service.initialized
        ]
        self._last_scrape_diagnostics: ParseDiagnostics | None = None
        self.initialized = self.validate()

        if not self.initialized:
            return

    def validate(self) -> bool:
        """Validate that at least one scraper service is initialized."""

        return len(self.initialized_services) > 0

    def run(
        self,
        item: MediaItem,
    ) -> MediaItemGenerator:
        """Scrape an item."""
        next_run_at: datetime | None = None

        # Skip if item is already satisfied (e.g. by a parallel season scrape)
        if item.last_state in (States.Downloaded, States.Symlinked, States.Completed):
            logger.debug(f"Skipping scrape for {item.log_string}: Item is already {item.last_state}")
            return

        sorted_streams = self.scrape(item)
        diagnostics = getattr(self, "_last_scrape_diagnostics", None)

        new_streams = [
            stream
            for stream in sorted_streams.values()
            if stream not in item.streams and stream not in item.blacklisted_streams
        ]
        known_existing = sum(1 for stream in sorted_streams.values() if stream in item.streams)
        known_blacklisted = sum(
            1 for stream in sorted_streams.values() if stream in item.blacklisted_streams
        )
        known_total = known_existing + known_blacklisted

        if new_streams:
            item.streams.extend(new_streams)
            item.updated = False

            if item.failed_attempts > 0:
                item.failed_attempts = 0  # Reset failed attempts on success

            _log_scraper(f"Added {len(new_streams)} new streams to {item.log_string}")
        else:
            if sorted_streams:
                _log_scraper(
                    "No new streams added for {} (parsed_candidates={}, known_existing={}, known_blacklisted={})".format(
                        item.log_string,
                        len(sorted_streams),
                        known_existing,
                        known_blacklisted,
                    )
                )
            elif diagnostics and diagnostics.input_results:
                _log_scraper(
                    "No new streams added for {} (raw_results={}, all_filtered=true, rejections={})".format(
                        item.log_string,
                        diagnostics.input_results,
                        diagnostics.rejection_summary(),
                    )
                )
            else:
                _log_scraper(f"No new streams added for {item.log_string}")

            next_scraped_times = item.scraped_times + 1
            count_as_failure = not (sorted_streams and known_total == len(sorted_streams))

            if count_as_failure:
                item.failed_attempts += 1
            else:
                logger.debug(
                    "All parsed candidates for {} are already known; keeping failed_attempts at {}".format(
                        item.log_string, item.failed_attempts
                    )
                )

            if (
                count_as_failure
                and self.max_failed_attempts > 0
                and item.failed_attempts >= self.max_failed_attempts
            ):
                item.store_state(States.Failed)
                logger.debug(
                    f"Failed scraping after {item.failed_attempts}/{self.max_failed_attempts} tries. Marking as failed: {item.log_string}"
                )
            else:
                delay_seconds = self._scrape_backoff_seconds(next_scraped_times)
                next_run_at = datetime.now() + timedelta(seconds=delay_seconds)
                if count_as_failure:
                    logger.debug(
                        "Failed scraping after {}/{} tries with no new streams: {}. "
                        "Scheduling next attempt at {}.".format(
                            item.failed_attempts,
                            self.max_failed_attempts,
                            item.log_string,
                            next_run_at.isoformat(timespec="seconds"),
                        )
                    )
                else:
                    logger.debug(
                        "Rescheduling scrape for {} at {} because candidates are already known.".format(
                            item.log_string,
                            next_run_at.isoformat(timespec="seconds"),
                        )
                    )

        item.set("scraped_at", datetime.now())
        item.set("scraped_times", item.scraped_times + 1)

        yield RunnerResult(media_items=[item], run_at=next_run_at)

    @staticmethod
    def _interpolate_delay_seconds(
        start_seconds: float,
        end_seconds: float,
        step_index: int,
        total_steps: int,
    ) -> int:
        if total_steps <= 0:
            return int(end_seconds)

        ratio = max(0.0, min(1.0, step_index / total_steps))
        return int(start_seconds + ((end_seconds - start_seconds) * ratio))

    def _scrape_backoff_seconds(self, scraped_times: int) -> int:
        """
        Compute progressive scrape backoff.

        This avoids abrupt jumps while still respecting configured upper bounds:
        - 1: 30 minutes
        - 2..5: gradually ramps from 30 minutes to after_2 hours
        - 6..10: gradually ramps from after_2 to after_5 hours
        - >10: after_10 hours
        """
        settings = settings_manager.settings.scraping
        base_seconds = 30 * 60
        after_2_seconds = max(base_seconds, int(settings.after_2 * 60 * 60))
        after_5_seconds = max(after_2_seconds, int(settings.after_5 * 60 * 60))
        after_10_seconds = max(after_5_seconds, int(settings.after_10 * 60 * 60))

        if scraped_times <= 1:
            return base_seconds

        if scraped_times <= 5:
            # scraped_times=2 -> first ramp step, scraped_times=5 -> after_2
            return self._interpolate_delay_seconds(
                base_seconds,
                after_2_seconds,
                scraped_times - 1,
                4,
            )

        if scraped_times <= 10:
            # scraped_times=6 -> first ramp step, scraped_times=10 -> after_5
            return self._interpolate_delay_seconds(
                after_2_seconds,
                after_5_seconds,
                scraped_times - 5,
                5,
            )

        return after_10_seconds

    def scrape(
        self,
        item: MediaItem,
        verbose_logging: bool = True,
        manual: bool = False,
    ) -> dict[str, Stream]:
        """Scrape an item.

        Args:
            item: The media item to scrape.
            verbose_logging: Whether to log verbose messages.
            manual: If True, bypass content filters for manual scraping.
        """

        results = dict[str, str]()
        results_lock = threading.RLock()
        self._last_scrape_diagnostics = None

        def run_service(svc: "ScraperService[Observable]", item: MediaItem) -> None:
            """Run a single service and update the results."""

            service_results = svc.run(item)

            with results_lock:
                try:
                    results.update(service_results)
                except Exception as e:
                    logger.exception(
                        f"Error updating results for {svc.__class__.__name__}: {e}"
                    )

        with ThreadPoolExecutor(
            thread_name_prefix="ScraperService_",
            max_workers=max(1, len(self.initialized_services)),
        ) as executor:
            futures = {
                executor.submit(run_service, service, item): service.key
                for service in self.initialized_services
            }

            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logger.error(
                        f"Exception occurred while running service {futures[future]}: {e}"
                    )

        if not results:
            logger.log("NOT_FOUND", f"No streams to process for {item.log_string}")
            return {}

        sorted_streams, diagnostics = parse_results(
            item,
            results,
            manual=manual,
            return_diagnostics=True,
        )
        self._last_scrape_diagnostics = diagnostics

        if sorted_streams and (verbose_logging and settings_manager.settings.log_level):
            top_results = list(sorted_streams.values())[:10]

            logger.debug(
                f"Displaying top {len(top_results)} results for {item.log_string}"
            )

            for stream in top_results:
                logger.debug(
                    f"[Rank: {stream.rank}][Res: {stream.parsed_data.resolution}] {stream.raw_title} ({stream.infohash})"
                )

        return sorted_streams

    def scrape_streaming(
        self,
        item: MediaItem,
        manual: bool = False,
    ) -> Generator[tuple[str, dict[str, Stream]], None, None]:
        """Scrape an item and yield results incrementally as each scraper finishes.

        Args:
            item: The media item to scrape.
            manual: If True, bypass content filters for manual scraping.

        Yields:
            Tuples of (service_name, parsed_streams_dict) as each service completes.
        """
        results_queue: Queue[tuple[str, dict[str, str]]] = Queue()
        all_raw_results = dict[str, str]()
        results_lock = threading.RLock()

        def run_service_streaming(
            svc: "ScraperService[Observable]", item: MediaItem
        ) -> None:
            """Run a single service and put results in the queue."""
            try:
                service_results = svc.run(item)
                if service_results:
                    results_queue.put((svc.key, service_results))
                else:
                    results_queue.put((svc.key, {}))
            except Exception as e:
                logger.error(f"Error running {svc.key}: {e}")
                results_queue.put((svc.key, {}))

        with ThreadPoolExecutor(
            thread_name_prefix="ScraperServiceStreaming_",
            max_workers=max(1, len(self.initialized_services)),
        ) as executor:
            futures = {
                executor.submit(run_service_streaming, service, item): service.key
                for service in self.initialized_services
            }

            services_completed = 0
            total_services = len(futures)

            while services_completed < total_services:
                try:
                    service_name, raw_results = results_queue.get(timeout=60.0)
                    services_completed += 1

                    if raw_results:
                        with results_lock:
                            all_raw_results.update(raw_results)

                        parsed_streams = parse_results(
                            item,
                            all_raw_results,
                            manual=manual,
                        )

                        yield (service_name, parsed_streams)
                    else:
                        yield (service_name, {})

                except Empty:
                    logger.warning("Timeout waiting for scraper results")
                    break

    def should_submit(self, item: MediaItem) -> bool:
        """Check if an item should be submitted for scraping."""

        settings = settings_manager.settings.scraping
        scrape_time = self._scrape_backoff_seconds(item.scraped_times)

        is_scrapeable = (
            not item.scraped_at
            or (datetime.now() - item.scraped_at).total_seconds() > scrape_time
        )

        if not is_scrapeable:
            return False

        if (
            settings.max_failed_attempts > 0
            and item.failed_attempts >= settings.max_failed_attempts
        ):
            return False

        return True

"""Prowlarr scraper module"""

import concurrent.futures
import time
from collections.abc import Iterable
from datetime import datetime, timedelta, timezone

from loguru import logger
from pydantic import BaseModel, ConfigDict, Field
from requests import ReadTimeout, RequestException

from program.media.item import Episode, MediaItem, Movie, Season, Show
from program.services.scrapers.base import ScraperService
from program.settings import settings_manager
from program.settings.models import ProwlarrConfig
from program.utils.request import SmartSession
from program.utils.torrent import extract_infohash, normalize_infohash
from schemas.prowlarr import (
    IndexerResource,
    IndexerStatusResource,
    MovieSearchParam,
    ReleaseResource,
    SearchParam,
    TvSearchParam,
)


class GetIndexersResponse(BaseModel):
    indexers: list[IndexerResource]


class GetIndexerStatusResponse(BaseModel):
    statuses: list[IndexerStatusResource]


class Category(BaseModel):
    name: str
    type: str
    ids: list[int]


class SearchParams(BaseModel):
    search: list[SearchParam]
    movie: list[MovieSearchParam]
    tv: list[TvSearchParam]


class Capabilities(BaseModel):
    supports_raw_search: bool | None
    categories: list[Category]
    search_params: SearchParams


class Indexer(BaseModel):
    id: int | None
    name: str | None
    enable: bool
    protocol: str
    language: str | None = None
    capabilities: Capabilities


class Params(BaseModel):
    model_config = ConfigDict(serialize_by_alias=True)

    query: str | None = None
    type: str | None = None
    indexer_ids: int | None = Field(serialization_alias="indexerIds", default=None)
    categories: list[int]
    limit: int | None = None
    season: int | None = None
    ep: int | None = None


class ScrapeResponse(BaseModel):
    items: list[ReleaseResource]


class ScrapeErrorResponse(BaseModel):
    message: str | None = None


ANIME_ONLY_INDEXERS = ("Nyaa.si", "SubsPlease", "Anidub", "Anidex")


class Prowlarr(ScraperService[ProwlarrConfig]):
    """Scraper for `Prowlarr`"""

    def __init__(self):
        super().__init__()

        self.settings = settings_manager.settings.scraping.prowlarr
        self.api_key = self.settings.api_key
        self.indexers = []
        self.headers = {
            "Content-Type": "application/json",
            "X-Api-Key": self.api_key,
        }
        self.timeout = self.settings.timeout
        self.session = None
        self.last_indexer_scan = None
        self._initialize()

    def _create_session(self) -> SmartSession:
        """Create a session for Prowlarr"""

        return SmartSession(
            base_url=f"{self.settings.url.rstrip('/')}/api/v1",
            retries=self.settings.retries,
            backoff_factor=0.3,
        )

    def validate(self) -> bool:
        """Validate Prowlarr settings."""

        if not self.settings.enabled:
            return False

        if self.settings.url and self.settings.api_key:
            self.api_key = self.settings.api_key

            try:
                if self.timeout <= 0:
                    logger.error("Prowlarr timeout must be a positive integer.")
                    return False

                self.session = self._create_session()
                self.indexers = self.get_indexers()

                if not self.indexers:
                    logger.error("No Prowlarr indexers configured.")
                    return False

                return True
            except ReadTimeout:
                logger.error(
                    "Prowlarr request timed out. Check your indexers, they may be too slow to respond."
                )
                return False
            except Exception as e:
                logger.error(f"Prowlarr failed to initialize with API Key: {e}")
                return False
        logger.warning("Prowlarr is not configured and will not be used.")
        return False

    def get_indexers(self) -> list[Indexer]:
        assert self.session

        statuses = self.session.get("/indexerstatus", timeout=15, headers=self.headers)
        response = self.session.get("/indexer", timeout=15, headers=self.headers)

        data = GetIndexersResponse.model_validate(
            {
                "indexers": response.json(),
            }
        ).indexers
        statuses = GetIndexerStatusResponse.model_validate(
            {
                "statuses": statuses.json(),
            }
        ).statuses

        indexers = list[Indexer]()

        for indexer_data in data:
            id = indexer_data.id

            if statuses:
                status = next(
                    (x for x in statuses if x.indexer_id == id),
                    None,
                )

                if (
                    status
                    and status.disabled_till
                    and status.disabled_till > datetime.now(timezone.utc)
                ):
                    disabled_until = status.disabled_till.strftime("%Y-%m-%d %H:%M")

                    logger.debug(
                        f"Indexer {indexer_data.name} is disabled until {disabled_until}, skipping"
                    )

                    continue

            name = indexer_data.name
            enable = indexer_data.enable

            if not enable:
                logger.debug(f"Indexer {name} is disabled, skipping")
                continue

            protocol = indexer_data.protocol

            if protocol != "torrent":
                logger.debug(f"Indexer {name} is not a torrent indexer, skipping")
                continue

            categories = list[Category]()

            if not indexer_data.capabilities:
                logger.warning(
                    f"No capabilities found for indexer {name}. Consider removing this indexer."
                )
                continue

            if indexer_data.capabilities and indexer_data.capabilities.categories:
                for cap in indexer_data.capabilities.categories:
                    if cap.name:
                        if "TV" in cap.name:
                            category = next(
                                (x for x in categories if "TV" in x.name), None
                            )

                            if cap.id:
                                if category:
                                    category.ids.append(cap.id)
                                else:
                                    categories.append(
                                        Category(name="TV", type="tv", ids=[cap.id])
                                    )
                        elif "Movies" in cap.name:
                            category = next(
                                (x for x in categories if "Movies" in x.name), None
                            )

                            if cap.id:
                                if category:
                                    category.ids.append(cap.id)
                                else:
                                    categories.append(
                                        Category(
                                            name="Movies", type="movie", ids=[cap.id]
                                        )
                                    )
                        elif "Anime" in cap.name:
                            category = next(
                                (x for x in categories if "Anime" in x.name), None
                            )

                            if cap.id:
                                if category:
                                    category.ids.append(cap.id)
                                else:
                                    categories.append(
                                        Category(
                                            name="Anime", type="anime", ids=[cap.id]
                                        )
                                    )

            if not categories:
                logger.warning(
                    f"No valid capabilities found for indexer {name}. Consider removing this indexer."
                )
                continue

            search_params = SearchParams(
                search=list(set(indexer_data.capabilities.search_params or [])),
                movie=list(set(indexer_data.capabilities.movie_search_params or [])),
                tv=list(set(indexer_data.capabilities.tv_search_params or [])),
            )

            capabilities = Capabilities(
                supports_raw_search=indexer_data.capabilities.supports_raw_search,
                categories=categories,
                search_params=search_params,
            )

            indexers.append(
                Indexer(
                    id=id,
                    name=name,
                    enable=enable,
                    protocol=protocol,
                    language=indexer_data.language,
                    capabilities=capabilities,
                )
            )

        self.last_indexer_scan = datetime.now(timezone.utc)

        return indexers

    def _periodic_indexer_scan(self):
        """Scan indexers every 30 minutes"""

        previous_count = len(self.indexers)

        if (
            self.last_indexer_scan is None
            or (datetime.now(timezone.utc) - self.last_indexer_scan).total_seconds()
            > 1800
        ):
            self.indexers = self.get_indexers()
            self.last_indexer_scan = datetime.now(timezone.utc)

            if len(self.indexers) != previous_count:
                logger.info(
                    f"Indexers count changed from {previous_count} to {len(self.indexers)}"
                )

                next_scan_time = self.last_indexer_scan + timedelta(seconds=1800)

                logger.info(
                    f"Next scan will be at {next_scan_time.strftime('%Y-%m-%d %H:%M')}"
                )

    def run(self, item: MediaItem) -> dict[str, str]:
        """
        Scrape the Prowlarr site for the given media items
        and update the object with scraped streams
        """

        try:
            return self.scrape(item)
        except Exception as e:
            if "rate limit" in str(e).lower() or "429" in str(e):
                logger.debug(f"Prowlarr ratelimit exceeded for item: {item.log_string}")
            elif isinstance(e, RequestException):
                logger.error(f"Prowlarr request exception: {e}")
            else:
                logger.exception(f"Prowlarr failed to scrape item with error: {e}")
        return {}

    def scrape(self, item: MediaItem) -> dict[str, str]:
        """Scrape a single item from all indexers at the same time, return a list of streams"""

        self._periodic_indexer_scan()

        torrents = dict[str, str]()
        start_time = time.time()

        with concurrent.futures.ThreadPoolExecutor(
            thread_name_prefix="ProwlarrScraper", max_workers=len(self.indexers)
        ) as executor:
            future_to_indexer = {
                executor.submit(self.scrape_indexer, indexer, item): indexer
                for indexer in self.indexers
            }

            for future, indexer in future_to_indexer.items():
                try:
                    result = future.result(timeout=self.timeout)

                    torrents.update(result)
                except concurrent.futures.TimeoutError:
                    logger.debug(f"Timeout for indexer {indexer.name}, skipping.")
                except Exception as e:
                    logger.error(f"Error processing indexer {indexer.name}: {e}")

        elapsed = time.time() - start_time

        if torrents:
            logger.log(
                "SCRAPER", f"Found {len(torrents)} streams for {item.log_string}"
            )
            logger.debug(f"Total time taken: {elapsed:.2f} seconds")
        else:
            logger.log("NOT_FOUND", f"No streams found for {item.log_string}")

        return torrents

    @staticmethod
    def _normalize_query(query: str | None) -> str | None:
        """Normalize a title query to keep candidate matching stable."""

        if not query:
            return None

        normalized = " ".join(query.split()).strip()
        return normalized or None

    def _iter_alias_titles(
        self, aliases: dict[str, list[str]] | None, country_codes: Iterable[str]
    ) -> list[str]:
        """Return alias titles in the order of the provided country codes."""

        if not aliases:
            return []

        ordered_aliases = list[str]()
        seen = set[str]()

        for code in country_codes:
            for title in aliases.get(code, []):
                normalized = self._normalize_query(title)

                if normalized and normalized.casefold() not in seen:
                    ordered_aliases.append(normalized)
                    seen.add(normalized.casefold())

        return ordered_aliases

    def _preferred_country_codes(self, item: MediaItem, indexer: Indexer) -> list[str]:
        """Return the preferred alias country codes for a Prowlarr query."""

        preferred_countries = list[str]()
        indexer_language = (indexer.language or "").lower()

        preferred_countries.extend(
            self._language_country_preferences(indexer_language)
        )

        if item.is_anime:
            preferred_countries.extend(["us", "jp"])
        elif item.language == "pt":
            preferred_countries.extend(["us", "br", "pt"])
        elif item.language == "en":
            preferred_countries.extend(["us", "uk", "gb"])

        preferred_countries.extend(
            country.lower() for country in self.settings.preferred_alias_countries
        )

        deduped_countries = list[str]()
        seen_countries = set[str]()

        for country in preferred_countries:
            if country and country not in seen_countries:
                deduped_countries.append(country)
                seen_countries.add(country)

        return deduped_countries

    @staticmethod
    def _language_country_preferences(indexer_language: str) -> list[str]:
        """Map a Prowlarr indexer language into country-code preferences."""

        language_country_map = {
            "en": ["us", "uk", "gb"],
            "ja": ["jp"],
            "zh-cn": ["cn"],
            "zh-tw": ["tw"],
            "pt-br": ["br", "pt"],
            "pt": ["pt", "br"],
        }

        for language_prefix, countries in language_country_map.items():
            if indexer_language.startswith(language_prefix):
                return countries.copy()

        return []

    def _dedupe_queries(self, candidates: Iterable[str]) -> list[str]:
        """Remove duplicate query candidates while preserving order."""

        deduped_candidates = list[str]()
        seen_queries = set[str]()

        for candidate in candidates:
            normalized = self._normalize_query(candidate)

            if normalized and normalized.casefold() not in seen_queries:
                deduped_candidates.append(normalized)
                seen_queries.add(normalized.casefold())

        return deduped_candidates

    def _build_query_candidates(self, indexer: Indexer, item: MediaItem) -> list[str]:
        """Build prioritized title candidates for a single Prowlarr query."""

        primary_title = self._normalize_query(item.top_title)

        if not primary_title:
            return []

        if not self.settings.use_aliases:
            return [primary_title]

        aliases = item.get_aliases() or {}
        preferred_country_codes = self._preferred_country_codes(item, indexer)
        preferred_aliases = self._iter_alias_titles(aliases, preferred_country_codes)
        remaining_aliases = self._iter_alias_titles(
            aliases,
            [country for country in aliases if country not in preferred_country_codes],
        )

        candidates = list[str]()

        if item.is_anime or item.language == "pt" or (item.country or "").upper() == "BR":
            candidates.extend(preferred_aliases)
            candidates.append(primary_title)
        else:
            candidates.append(primary_title)
            candidates.extend(preferred_aliases)

        candidates.extend(remaining_aliases)

        return self._dedupe_queries(candidates)[: self.settings.max_query_variants]

    @staticmethod
    def _season_release_query(item_title: str, season_number: int) -> str:
        """Build a stable season query for search-only indexers."""

        return f"{item_title} S{season_number:02}"

    @classmethod
    def _episode_release_query(cls, item_title: str, item: Episode) -> str:
        """Build a stable episode query for search-only indexers."""

        return (
            f"{cls._season_release_query(item_title, item.parent.number)}"
            f"E{item.number:02}"
        )

    @staticmethod
    def _build_movie_or_show_search(
        search_params: SearchParams,
        item: Movie | Show,
        item_title: str,
        indexer_name: str | None,
    ) -> tuple[str, str, int | None, int | None]:
        """Build search params for movie and show items."""

        if isinstance(item, Movie):
            media_params = search_params.movie
            media_type = "movie"
        else:
            media_params = search_params.tv
            media_type = "show"

        if "imdbId" in media_params and item.imdb_id:
            search_query = item.imdb_id
        elif "q" in media_params:
            search_query = item_title
        elif "q" in search_params.search:
            return item_title, "search", None, None
        else:
            raise ValueError(
                f"Indexer {indexer_name} does not support {media_type} search"
            )

        return search_query, f"{media_type}-search", None, None

    @classmethod
    def _build_season_search(
        cls, search_params: SearchParams, item: Season, item_title: str, indexer_name: str | None
    ) -> tuple[str, str, int | None, int | None]:
        """Build search params for season items."""

        season_query = cls._season_release_query(item_title, item.number)

        if "q" in search_params.tv:
            season = item.number if "season" in search_params.tv else None
            return season_query, "tv-search", season, None

        if "q" in search_params.search:
            return season_query, "search", None, None

        raise ValueError(f"Indexer {indexer_name} does not support season search")

    @classmethod
    def _build_episode_search(
        cls,
        search_params: SearchParams,
        item: Episode,
        item_title: str,
        indexer_name: str | None,
    ) -> tuple[str, str, int | None, int | None]:
        """Build search params for episode items."""

        if "q" in search_params.tv:
            if "ep" in search_params.tv:
                return item_title, "tv-search", item.parent.number, item.number

            return (
                cls._episode_release_query(item_title, item),
                "tv-search",
                None,
                None,
            )

        if "q" in search_params.search:
            return cls._episode_release_query(item_title, item), "search", None, None

        raise ValueError(f"Indexer {indexer_name} does not support episode search")

    @staticmethod
    def _get_item_categories(indexer: Indexer, item: MediaItem) -> list[int]:
        """Resolve matching indexer categories for a media item."""

        categories = {
            cat_id
            for category in indexer.capabilities.categories
            if category.type == item.type
            or (category.type == "anime" and item.is_anime)
            for cat_id in category.ids
        }

        return list(categories)

    def build_search_params(
        self, indexer: Indexer, item: MediaItem, title_override: str | None = None
    ) -> Params:
        """Build a search query for a single indexer."""

        item_title = self._normalize_query(title_override or item.top_title) or item.top_title

        search_params = indexer.capabilities.search_params

        if isinstance(item, Movie | Show):
            search_query, search_type, season, episode = (
                self._build_movie_or_show_search(
                    search_params, item, item_title, indexer.name
                )
            )
        elif isinstance(item, Season):
            search_query, search_type, season, episode = self._build_season_search(
                search_params, item, item_title, indexer.name
            )
        else:
            search_query, search_type, season, episode = self._build_episode_search(
                search_params,
                item,
                item_title,
                indexer.name,
            )

        return Params(
            season=season,
            ep=episode,
            query=search_query,
            type=search_type,
            categories=self._get_item_categories(indexer, item),
            indexer_ids=indexer.id,
            limit=1000,
        )

    @staticmethod
    def _is_anime_only_indexer(indexer: Indexer) -> bool:
        """Return whether the current indexer is explicitly anime-oriented."""

        return bool(
            indexer.name in ANIME_ONLY_INDEXERS
            or "anime" in (indexer.name or "").lower()
        )

    def _request_search(
        self, indexer: Indexer, item: MediaItem, query: str
    ) -> list[ReleaseResource]:
        """Execute a single Prowlarr search query and validate the response."""

        try:
            params = self.build_search_params(indexer, item, title_override=query)
        except ValueError as e:
            logger.error(f"Failed to build search params for {indexer.name}: {e}")
            return []

        assert self.session

        response = self.session.get(
            "/search",
            params=params.model_dump(),
            timeout=self.timeout,
            headers=self.headers,
        )

        if response.ok:
            return ScrapeResponse.model_validate({"items": response.json()}).items

        data = ScrapeErrorResponse.model_validate(response.json())
        message = data.message or "Unknown error"

        logger.debug(
            f"Failed to scrape {indexer.name}: [{response.status_code}] {message}"
        )

        self.indexers.remove(indexer)

        logger.debug(
            f"Removed indexer {indexer.name} from the list of usable indexers"
        )
        return []

    @staticmethod
    def _extract_available_streams(
        data: list[ReleaseResource],
    ) -> tuple[dict[str, str], list[tuple[ReleaseResource, str]]]:
        """Extract immediately available hashes and collect releases that need URL fetches."""

        streams = dict[str, str]()
        urls_to_fetch = list[tuple[ReleaseResource, str]]()

        for torrent in data:
            title = torrent.title
            infohash = None

            if torrent.info_hash:
                infohash = normalize_infohash(torrent.info_hash)

            if not infohash and torrent.guid:
                infohash = extract_infohash(torrent.guid)

            if not infohash and torrent.download_url and title:
                urls_to_fetch.append((torrent, title))
            elif infohash and title:
                streams[infohash] = title

        return streams, urls_to_fetch

    def _fetch_streams_from_urls(
        self, urls_to_fetch: list[tuple[ReleaseResource, str]]
    ) -> dict[str, str]:
        """Resolve infohashes from release download URLs."""

        streams = dict[str, str]()

        if not urls_to_fetch:
            return streams

        with concurrent.futures.ThreadPoolExecutor(
            thread_name_prefix="ProwlarrHashExtract", max_workers=10
        ) as executor:
            future_to_torrent = {
                executor.submit(self.get_infohash_from_url, torrent.download_url): (
                    torrent,
                    title,
                )
                for torrent, title in urls_to_fetch
                if torrent.download_url
            }

            done, pending = concurrent.futures.wait(
                future_to_torrent.keys(),
                timeout=self.settings.infohash_fetch_timeout,
            )

            for future in done:
                _, title = future_to_torrent[future]

                try:
                    infohash = future.result()
                    if infohash:
                        streams[infohash] = title
                except Exception as e:
                    logger.debug(
                        f"Failed to get infohash from downloadUrl for {title}: {e}"
                    )

            for future in pending:
                _, title = future_to_torrent[future]
                future.cancel()
                logger.debug(f"Timeout getting infohash from downloadUrl for {title}")

        return streams

    def scrape_indexer(self, indexer: Indexer, item: MediaItem) -> dict[str, str]:
        """Scrape from a single indexer"""

        if self._is_anime_only_indexer(indexer) and not item.is_anime:
            logger.debug(f"Indexer {indexer.name} is anime only, skipping")
            return {}

        query_candidates = self._build_query_candidates(indexer, item)
        seen_param_signatures = set[str]()

        for query in query_candidates:
            params = self.build_search_params(indexer, item, title_override=query)

            params_signature = repr(params.model_dump())

            if params_signature in seen_param_signatures:
                continue

            seen_param_signatures.add(params_signature)
            start_time = time.time()
            data = self._request_search(indexer, item, query)
            streams, urls_to_fetch = self._extract_available_streams(data)
            streams.update(self._fetch_streams_from_urls(urls_to_fetch))

            if streams:
                if query != item.top_title:
                    logger.debug(
                        f"Prowlarr matched {item.log_string} using query variant '{query}' on {indexer.name}"
                    )

                logger.debug(
                    f"Indexer {indexer.name} found {len(streams)} streams for {item.log_string} in {time.time() - start_time:.2f} seconds"
                )
                return streams

        return {}

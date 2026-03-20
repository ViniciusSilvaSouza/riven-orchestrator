"""Zilean scraper module"""

from collections.abc import Iterable

from loguru import logger
from pydantic import BaseModel, ConfigDict, Field

from program.media.item import Episode, MediaItem, Season, Show
from program.services.scrapers.base import ScraperService
from program.settings import settings_manager
from program.settings.models import ZileanConfig
from program.utils.request import SmartSession, get_hostname_from_url


class Params(BaseModel):
    model_config = ConfigDict(serialize_by_alias=True)

    query: str = Field(serialization_alias="Query")
    season: int | None = Field(default=None, serialization_alias="Season")
    episode: int | None = Field(default=None, serialization_alias="Episode")
    year: int | None = Field(default=None, serialization_alias="Year")
    imdb_id: str | None = Field(default=None, serialization_alias="ImdbId")


class ZileanScrapeResponse(BaseModel):
    class ResultItem(BaseModel):
        raw_title: str | None
        info_hash: str | None

    data: list[ResultItem]


class Zilean(ScraperService[ZileanConfig]):
    """Scraper for `Zilean`"""

    def __init__(self):
        super().__init__()

        self.settings = settings_manager.settings.scraping.zilean
        self.timeout = self.settings.timeout

        self.session = SmartSession(
            rate_limits=(
                {
                    get_hostname_from_url(self.settings.url): {
                        "rate": 500 / 60,
                        "capacity": 500,
                    }
                }
                if self.settings.ratelimit
                else None
            ),
            retries=self.settings.retries,
            backoff_factor=0.3,
        )

        self._initialize()

    def validate(self) -> bool:
        """Validate the Zilean settings."""

        if not self.settings.enabled:
            return False

        if not self.settings.url:
            logger.error("Zilean URL is not configured and will not be used.")
            return False

        if self.timeout <= 0:
            logger.error("Zilean timeout must be a positive integer.")
            return False

        try:
            url = f"{self.settings.url}/healthchecks/ping"
            response = self.session.get(url, timeout=self.timeout)

            return response.ok
        except Exception as e:
            logger.error(f"Zilean failed to initialize: {e}")
            return False

    def run(self, item: MediaItem) -> dict[str, str]:
        """Scrape the Zilean site for the given media items and update the object with scraped items."""

        try:
            return self.scrape(item)
        except Exception as e:
            if "rate limit" in str(e).lower() or "429" in str(e):
                logger.debug(f"Zilean rate limit exceeded for item: {item.log_string}")
            else:
                logger.exception(f"Zilean exception thrown: {e}")

        return {}

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

    def _preferred_country_codes(self, item: MediaItem) -> list[str]:
        """Return the preferred alias country codes for the given media item."""

        preferred_countries = list[str]()

        if item.country:
            preferred_countries.append(item.country.lower())

        if item.language == "pt":
            preferred_countries.extend(["br", "pt"])
        elif item.language == "en":
            preferred_countries.extend(["us", "uk", "gb"])

        preferred_countries.extend(
            country.lower() for country in self.settings.preferred_alias_countries
        )

        deduped_countries = list[str]()
        seen_countries = set[str]()

        for country in preferred_countries:
            if country not in seen_countries:
                deduped_countries.append(country)
                seen_countries.add(country)

        return deduped_countries

    def _dedupe_queries(self, candidates: Iterable[str]) -> list[str]:
        """Remove duplicate Zilean queries while preserving order."""

        deduped_candidates = list[str]()
        seen_queries = set[str]()

        for candidate in candidates:
            normalized = self._normalize_query(candidate)

            if normalized and normalized.casefold() not in seen_queries:
                deduped_candidates.append(normalized)
                seen_queries.add(normalized.casefold())

        return deduped_candidates

    def _build_query_candidates(self, item: MediaItem) -> list[str]:
        """Build prioritized Zilean query candidates for a media item."""

        primary_title = self._normalize_query(item.top_title)

        if not primary_title:
            return []

        if not self.settings.use_aliases:
            return [primary_title]

        aliases = item.get_aliases() or {}
        preferred_country_codes = self._preferred_country_codes(item)
        preferred_aliases = self._iter_alias_titles(aliases, preferred_country_codes)

        remaining_aliases = self._iter_alias_titles(
            aliases,
            [country for country in aliases if country not in preferred_country_codes],
        )

        candidates = list[str]()

        if item.language == "pt" or (item.country or "").upper() == "BR":
            candidates.extend(preferred_aliases)
            candidates.append(primary_title)
        else:
            candidates.append(primary_title)
            candidates.extend(preferred_aliases)

        candidates.extend(remaining_aliases)

        return self._dedupe_queries(candidates)[: self.settings.max_query_variants]

    def _build_query_params(self, item: MediaItem, query: str) -> Params:
        """Build the query params for the Zilean API."""
        season = None
        episode = None

        if isinstance(item, Show):
            season = 1
        elif isinstance(item, Season):
            season = item.number
        elif isinstance(item, Episode):
            season = item.parent.number
            episode = item.number

        return Params(
            query=query,
            season=season,
            episode=episode,
            year=item.year if self.settings.include_year else None,
            imdb_id=item.imdb_id if self.settings.include_imdb_id else None,
        )

    def scrape(self, item: MediaItem) -> dict[str, str]:
        """Wrapper for `Zilean` scrape method"""

        url = f"{self.settings.url}/dmm/filtered"
        query_candidates = self._build_query_candidates(item)

        if not query_candidates:
            logger.log("NOT_FOUND", f"No valid Zilean queries for {item.log_string}")
            return {}

        for query in query_candidates:
            params = self._build_query_params(item, query)

            response = self.session.get(
                url,
                params=params.model_dump(exclude_none=True),
                timeout=self.timeout,
            )

            if not response.ok:
                logger.error(
                    f"Zilean responded with status code {response.status_code} for {item.log_string}: {response.text}"
                )
                return {}

            data = ZileanScrapeResponse.model_validate({"data": response.json()}).data

            if not data:
                continue

            torrents = dict[str, str]()

            for result in data:
                if not result.raw_title or not result.info_hash:
                    continue

                torrents[result.info_hash] = result.raw_title

            if torrents:
                if query != item.top_title:
                    logger.debug(
                        f"Zilean matched {item.log_string} using query variant: {query}"
                    )

                logger.log(
                    "SCRAPER", f"Found {len(torrents)} streams for {item.log_string}"
                )
                return torrents

        logger.log("NOT_FOUND", f"No streams found for {item.log_string}")
        return {}

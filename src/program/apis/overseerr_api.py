"""Overseerr API client."""

from collections.abc import Iterable
from typing import Any, Literal

from loguru import logger
from requests.exceptions import ConnectionError, RetryError
from urllib3.exceptions import MaxRetryError, NewConnectionError

from program.media.item import MediaItem
from program.utils.request import SmartSession, get_hostname_from_url

ItemType = Literal["tv", "movie"]


class OverseerrAPIError(Exception):
    """Base exception for OverseerrAPI related errors."""


class OverseerrAPI:
    """Handles Overseerr API communication."""

    REQUEST_PENDING = 1
    REQUEST_APPROVED = 2
    REQUEST_DECLINED = 3
    REQUEST_FAILED = 4
    REQUEST_COMPLETED = 5

    MEDIA_UNKNOWN = 1
    MEDIA_PENDING = 2
    MEDIA_PROCESSING = 3
    MEDIA_PARTIALLY_AVAILABLE = 4
    MEDIA_AVAILABLE = 5

    MEDIA_STATUS_UNKNOWN = "unknown"
    MEDIA_STATUS_PENDING = "pending"
    MEDIA_STATUS_PROCESSING = "processing"
    MEDIA_STATUS_PARTIALLY_AVAILABLE = "partially_available"
    MEDIA_STATUS_AVAILABLE = "available"
    MEDIA_STATUS_DELETED = "deleted"

    def __init__(self, api_key: str, base_url: str):
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")

        self.session = SmartSession(
            base_url=base_url,
            rate_limits={
                # 1000 calls per 5 minutes, retries=3, backoff_factor=0.3
                get_hostname_from_url(self.base_url): {
                    "rate": 1000 / 300,
                    "capacity": 1000,
                }
            },
        )

        self.session.headers.update(
            {
                "X-Api-Key": self.api_key,
            }
        )

    def validate(self):
        """Validate API connection."""

        try:
            return self.session.get("api/v1/auth/me", timeout=15).ok
        except (ConnectionError, RetryError, MaxRetryError, NewConnectionError):
            logger.error("Overseerr URL is not reachable, or it timed out")
        except Exception as e:
            logger.error(f"Unexpected error during Overseerr validation: {str(e)}")

        return False

    @classmethod
    def _has_external_link(cls, media: dict[str, Any]) -> bool:
        """Return True when Seerr already linked the request to a real target."""

        link_fields = (
            "serviceId",
            "serviceId4k",
            "externalServiceId",
            "externalServiceId4k",
            "externalServiceSlug",
            "externalServiceSlug4k",
            "ratingKey",
            "ratingKey4k",
            "jellyfinMediaId",
            "jellyfinMediaId4k",
        )

        return any(media.get(field) not in (None, "", []) for field in link_fields)

    @staticmethod
    def _coerce_optional_int(value: Any) -> int | None:
        if value in (None, ""):
            return None

        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    @classmethod
    def _normalize_requested_seasons(cls, value: Any) -> list[int] | None:
        if value in (None, ""):
            return None

        normalized = list[int]()

        if isinstance(value, str):
            raw_values: Iterable[Any] = value.split(",")
        elif isinstance(value, list):
            raw_values = value
        else:
            raw_values = [value]

        for raw_value in raw_values:
            season_number: Any = raw_value

            if isinstance(raw_value, dict):
                season_number = (
                    raw_value.get("seasonNumber")
                    or raw_value.get("season_number")
                    or raw_value.get("number")
                )

            season = cls._coerce_optional_int(season_number)

            if season is not None and season not in normalized:
                normalized.append(season)

        return normalized or None

    @classmethod
    def _extract_requested_seasons(
        cls,
        request: dict[str, Any],
        requested_seasons: list[int] | None = None,
    ) -> list[int] | None:
        if requested_seasons:
            return cls._normalize_requested_seasons(requested_seasons)

        media = request.get("media") or {}

        for candidate in (
            request.get("requestedSeasons"),
            request.get("requested_seasons"),
            request.get("seasons"),
            media.get("requestedSeasons"),
            media.get("requested_seasons"),
            media.get("seasons"),
        ):
            normalized = cls._normalize_requested_seasons(candidate)
            if normalized:
                return normalized

        return None

    @classmethod
    def build_media_item(
        cls,
        service_key: str,
        request: dict[str, Any],
        requested_seasons: list[int] | None = None,
    ) -> MediaItem | None:
        media = request.get("media") or {}
        tmdb_id = media.get("tmdbId")
        tvdb_id = media.get("tvdbId")
        payload = {
            "requested_by": service_key,
            "requested_id": cls._coerce_optional_int(request.get("id")),
            "overseerr_id": cls._coerce_optional_int(media.get("id")),
            "requested_seasons": cls._extract_requested_seasons(
                request,
                requested_seasons=requested_seasons,
            ),
        }

        if tvdb_id is not None:
            payload["tvdb_id"] = tvdb_id
            return MediaItem(payload)

        if tmdb_id is not None:
            payload["tmdb_id"] = tmdb_id
            return MediaItem(payload)

        logger.error(
            "Could not determine ID for overseerr item: {}",
            request.get("id"),
        )
        return None

    @classmethod
    def _is_actionable_request(
        cls, request: dict[str, Any], filter_pending_items: bool
    ) -> bool:
        """
        Decide whether a Seerr/Overseerr request should become a Riven MediaItem.

        Supported paths:
        - approved requests that are not yet available
        - Seerr-only requests marked completed/available but still not linked to
          any arr/media-server target
        """

        media = request.get("media") or {}
        request_status = request.get("status")
        media_status = media.get("status")

        if request_status == cls.REQUEST_APPROVED:
            if not filter_pending_items:
                return media_status != cls.MEDIA_AVAILABLE

            return media_status in (
                None,
                cls.MEDIA_UNKNOWN,
                cls.MEDIA_PENDING,
                cls.MEDIA_PROCESSING,
                cls.MEDIA_PARTIALLY_AVAILABLE,
            )

        if (
            request_status == cls.REQUEST_COMPLETED
            and media_status == cls.MEDIA_AVAILABLE
        ):
            return not cls._has_external_link(media)

        return False

    def get_media_requests(
        self,
        service_key: str,
        filter: (
            Literal[
                "all",
                "approved",
                "available",
                "pending",
                "processing",
                "unavailable",
                "failed",
                "deleted",
                "completed",
            ]
            | None
        ) = "approved",
        take: int = 10000,
        filter_pending_items: bool = True,
    ) -> list[MediaItem]:
        """Get media requests from `Overseerr`."""

        url = f"api/v1/request?take={take}&sort=added"

        if filter:
            url += f"&filter={filter}"

        try:
            response = self.session.get(url)

            if not response.ok:
                logger.error(f"Failed to get response from overseerr: {response.data}")
                return []

            response_data = response.json()
            response_results = response_data.get("results") or []

            if not response_results:
                logger.debug("No overseerr requests found for the current filter")
                return []

        except Exception as e:
            logger.error(f"Failed to get response from overseerr: {str(e)}")
            return []

        actionable_requests = [
            item
            for item in response_results
            if self._is_actionable_request(item, filter_pending_items)
        ]

        if not actionable_requests:
            logger.debug("No actionable overseerr requests found for Riven")
            return []

        completed_unlinked_count = sum(
            1
            for item in actionable_requests
            if item.get("status") == self.REQUEST_COMPLETED
        )

        if completed_unlinked_count:
            logger.info(
                "Treating {} completed Seerr request(s) without downstream links as actionable",
                completed_unlinked_count,
            )

        media_items = [
            media_item
            for item in actionable_requests
            if (media_item := self.build_media_item(service_key, item)) is not None
        ]

        return media_items

    def get_request_details(self, request_id: int) -> dict[str, Any] | None:
        """Fetch a single request payload from Seerr/Overseerr."""

        try:
            response = self.session.get(f"api/v1/request/{request_id}")

            if response.ok:
                return response.json()

            logger.warning(
                "Failed to get overseerr request {} details: {}",
                request_id,
                response.data,
            )
        except Exception as e:
            logger.error(f"Failed to get overseerr request details: {str(e)}")

        return None

    def delete_request(self, request_id: int) -> bool:
        """Delete request from Seerr/Overseerr."""

        try:
            response = self.session.delete(f"api/v1/request/{request_id}")

            logger.debug(f"Deleted request {request_id} from Overseerr")

            return response.ok
        except Exception as e:
            logger.error(f"Failed to delete request from Overseerr: {str(e)}")

            return False

    def update_media_status(
        self,
        media_id: int,
        status: Literal[
            "unknown",
            "pending",
            "processing",
            "partially_available",
            "available",
            "deleted",
        ],
        *,
        is_4k: bool = False,
    ) -> bool:
        """Update media availability status in Seerr/Overseerr."""

        try:
            response = self.session.post(
                f"api/v1/media/{media_id}/{status}",
                json={"is4k": is_4k},
            )

            if response.ok:
                logger.info(
                    "Updated Seerr media {} status to {}",
                    media_id,
                    status,
                )
                return True

            logger.warning(
                "Failed to update Seerr media {} status to {}: {}",
                media_id,
                status,
                response.data,
            )
        except Exception as e:
            logger.error(f"Failed to update Seerr media status: {str(e)}")

        return False


# Statuses for Media Requests endpoint /api/v1/request:
# item.status:
# 1 = PENDING APPROVAL,
# 2 = APPROVED,
# 3 = DECLINED,
# 4 = FAILED,
# 5 = COMPLETED

# Statuses for Media Info endpoint /api/v1/media:
# item.media.status:
# 1 = UNKNOWN,
# 2 = PENDING,
# 3 = PROCESSING,
# 4 = PARTIALLY_AVAILABLE,
# 5 = AVAILABLE,
# 6 = BLOCKLISTED,
# 7 = DELETED

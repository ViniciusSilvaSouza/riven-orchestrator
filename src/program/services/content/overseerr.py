"""Overseerr content module"""

from kink import di
from loguru import logger
from sqlalchemy import or_, select

from program.apis.overseerr_api import OverseerrAPI
from program.core.runner import MediaItemGenerator, Runner, RunnerResult
from program.db.db import db_session
from program.media.item import Episode, MediaItem, Movie, Season, Show
from program.media.state import States
from program.settings import settings_manager
from program.settings.models import OverseerrModel


class Overseerr(Runner[OverseerrModel]):
    """Content class for overseerr"""

    is_content_service = True

    def __init__(self):
        super().__init__()

        self.settings = settings_manager.settings.content.overseerr

        if not self.enabled:
            return

        self.api = di[OverseerrAPI]
        self.initialized = self.validate()
        self.run_once = False

        if not self.initialized:
            return

        logger.success("Overseerr initialized!")

    def validate(self) -> bool:
        if not self.settings.enabled:
            return False

        if self.settings.api_key == "":
            logger.error("Overseerr API key is not set.")
            return False

        if len(self.settings.api_key) != 68:
            logger.error("Overseerr API key length is invalid.")
            return False

        try:
            return self.api.validate()
        except Exception:
            return False

    @staticmethod
    def _merge_requested_seasons(
        existing_seasons: list[int] | None,
        new_seasons: list[int] | None,
    ) -> list[int] | None:
        merged = list[int]()

        for seasons in (existing_seasons, new_seasons):
            if not seasons:
                continue

            for season in seasons:
                if season not in merged:
                    merged.append(season)

        return merged or None

    @staticmethod
    def _requested_season_scope(item: Show) -> list[Season]:
        requested_seasons = item.requested_seasons or []

        if not requested_seasons:
            return [season for season in item.seasons if season.number and season.number > 0]

        return [
            season
            for season in item.seasons
            if season.number in requested_seasons
        ]

    @classmethod
    def _season_is_available(cls, season: Season) -> bool:
        return season.state == States.Completed

    @classmethod
    def _season_is_partially_available(cls, season: Season) -> bool:
        return season.state in (
            States.Completed,
            States.PartiallyCompleted,
            States.Symlinked,
            States.Downloaded,
        )

    def _determine_sync_status(self, item: MediaItem) -> str | None:
        if isinstance(item, Movie):
            if item.available_in_vfs or item.state in (
                States.Completed,
                States.Symlinked,
                States.Downloaded,
            ):
                return self.api.MEDIA_STATUS_AVAILABLE

            return None

        if not isinstance(item, Show):
            return None

        requested_scope = self._requested_season_scope(item)

        if not requested_scope:
            return None

        if all(self._season_is_available(season) for season in requested_scope):
            return self.api.MEDIA_STATUS_AVAILABLE

        if any(
            self._season_is_partially_available(season) for season in requested_scope
        ):
            return self.api.MEDIA_STATUS_PARTIALLY_AVAILABLE

        return None

    @staticmethod
    def _get_sync_target(item: MediaItem) -> MediaItem:
        if isinstance(item, (Show, Movie)):
            return item

        if isinstance(item, (Season, Episode)):
            return item.top_parent

        return item

    def sync_availability(self, item: MediaItem) -> bool:
        if not self.initialized or not self.settings.sync_status:
            return False

        target = self._get_sync_target(item)

        if (
            target.requested_by != self.key
            or not target.overseerr_id
            or not target.requested_id
        ):
            return False

        desired_status = self._determine_sync_status(target)

        if not desired_status:
            return False

        return self.api.update_media_status(target.overseerr_id, desired_status)

    def _merge_existing_request_context(
        self,
        overseerr_items: list[MediaItem],
    ) -> list[MediaItem]:
        if not overseerr_items:
            return []

        new_items = list[MediaItem]()

        with db_session() as session:
            for incoming_item in overseerr_items:
                clauses = list()

                if incoming_item.tvdb_id:
                    clauses.append(Show.tvdb_id == str(incoming_item.tvdb_id))

                if incoming_item.tmdb_id:
                    clauses.append(Movie.tmdb_id == str(incoming_item.tmdb_id))
                    clauses.append(Show.tmdb_id == str(incoming_item.tmdb_id))

                if not clauses:
                    new_items.append(incoming_item)
                    continue

                existing_item = session.execute(
                    select(MediaItem)
                    .where(MediaItem.type.in_(["movie", "show"]))
                    .where(or_(*clauses))
                ).scalar_one_or_none()

                if not existing_item:
                    new_items.append(incoming_item)
                    continue

                changed = False

                if incoming_item.requested_by and existing_item.requested_by != incoming_item.requested_by:
                    existing_item.requested_by = incoming_item.requested_by
                    changed = True

                if incoming_item.requested_id and existing_item.requested_id != incoming_item.requested_id:
                    existing_item.requested_id = incoming_item.requested_id
                    changed = True

                if incoming_item.overseerr_id and existing_item.overseerr_id != incoming_item.overseerr_id:
                    existing_item.overseerr_id = incoming_item.overseerr_id
                    changed = True

                merged_requested_seasons = self._merge_requested_seasons(
                    existing_item.requested_seasons,
                    incoming_item.requested_seasons,
                )

                if merged_requested_seasons != existing_item.requested_seasons:
                    existing_item.requested_seasons = merged_requested_seasons
                    changed = True

                if changed:
                    logger.info(
                        "Updated Seerr request context for existing item {}",
                        existing_item.log_string,
                    )

            session.commit()

        return new_items

    def run(self, _item: MediaItem) -> MediaItemGenerator:
        """Fetch new media from `Overseerr`"""

        if self.settings.use_webhook and self.run_once:
            return

        overseerr_items = self.api.get_media_requests(
            self.key,
            filter="all",
            filter_pending_items=self.run_once,
        )

        if self.settings.use_webhook:
            logger.info(
                "Webhook is enabled. Running Overseerr once before switching to webhook only mode"
            )

        self.run_once = True
        overseerr_items = self._merge_existing_request_context(overseerr_items)

        logger.info(f"Fetched {len(overseerr_items)} items from overseerr")

        yield RunnerResult(media_items=overseerr_items)

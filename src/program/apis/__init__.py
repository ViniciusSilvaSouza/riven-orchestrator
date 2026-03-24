from typing import TYPE_CHECKING

from kink import di

from program.settings import settings_manager

if TYPE_CHECKING:
    from .listrr_api import ListrrAPI
    from .mdblist_api import MdblistAPI
    from .overseerr_api import OverseerrAPI
    from .plex_api import PlexAPI
    from .tmdb_api import TMDBApi
    from .trakt_api import TraktAPI
    from .tvdb_api import TVDBApi


__all__ = ["bootstrap_apis"]


def bootstrap_apis():
    __setup_plex()
    __setup_mdblist()
    __setup_overseerr()
    __setup_listrr()
    __setup_trakt()
    __setup_tmdb()
    __setup_tvdb()


def __setup_trakt():
    from .trakt_api import TraktAPI

    di[TraktAPI] = TraktAPI(settings_manager.settings.content.trakt)


def __setup_tmdb():
    from .tmdb_api import TMDBApi

    di[TMDBApi] = TMDBApi()


def __setup_tvdb():
    from .tvdb_api import TVDBApi

    di[TVDBApi] = TVDBApi()


def __setup_plex():
    from .plex_api import PlexAPI

    if not settings_manager.settings.updaters.plex.enabled:
        return

    di[PlexAPI] = PlexAPI(
        settings_manager.settings.updaters.plex.token,
        settings_manager.settings.updaters.plex.url,
    )


def __setup_overseerr():
    from .overseerr_api import OverseerrAPI

    if not settings_manager.settings.content.overseerr.enabled:
        return

    di[OverseerrAPI] = OverseerrAPI(
        settings_manager.settings.content.overseerr.api_key,
        settings_manager.settings.content.overseerr.url,
    )


def __setup_mdblist():
    from .mdblist_api import MdblistAPI

    if not settings_manager.settings.content.mdblist.enabled:
        return

    di[MdblistAPI] = MdblistAPI(settings_manager.settings.content.mdblist.api_key)


def __setup_listrr():
    from .listrr_api import ListrrAPI

    if not settings_manager.settings.content.listrr.enabled:
        return

    di[ListrrAPI] = ListrrAPI(settings_manager.settings.content.listrr.api_key)

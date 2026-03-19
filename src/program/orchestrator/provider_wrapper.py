from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from program.media.item import MediaItem
    from program.media.stream import Stream
    from program.services.downloaders import Downloader
    from program.services.downloaders.models import DownloadedTorrent, TorrentContainer
    from program.services.downloaders.shared import DownloaderBase


class ProviderResolveStatus(str, Enum):
    RESOLVED = "resolved"
    NOT_CACHED = "not_cached"


class ProviderNoMatchingFilesError(RuntimeError):
    """Provider resolved torrent but no valid files matched the requested media item."""


@dataclass
class ProviderCacheResult:
    infohash: str
    provider: str
    status: ProviderResolveStatus
    container: "TorrentContainer | None" = None

    @property
    def is_cached(self) -> bool:
        return self.status == ProviderResolveStatus.RESOLVED and self.container is not None


@dataclass
class ProviderResolveResult:
    infohash: str
    provider: str
    status: ProviderResolveStatus
    download_result: "DownloadedTorrent | None" = None

    @property
    def is_resolved(self) -> bool:
        return self.status == ProviderResolveStatus.RESOLVED and self.download_result is not None


class ProviderResolveWrapper:
    """Adapter that provides a clear cache/resolve contract for debrid providers."""

    def __init__(self, downloader: "Downloader") -> None:
        self._downloader = downloader

    def check_cache(
        self,
        provider: "DownloaderBase",
        infohash: str,
        *,
        item: "MediaItem",
        stream: "Stream",
    ) -> ProviderCacheResult:
        container = self._downloader.validate_stream_on_service(stream, item, provider)
        if not container:
            return ProviderCacheResult(
                infohash=infohash,
                provider=provider.key,
                status=ProviderResolveStatus.NOT_CACHED,
                container=None,
            )

        return ProviderCacheResult(
            infohash=infohash,
            provider=provider.key,
            status=ProviderResolveStatus.RESOLVED,
            container=container,
        )

    def resolve(
        self,
        provider: "DownloaderBase",
        infohash: str,
        *,
        item: "MediaItem",
        stream: "Stream",
    ) -> ProviderResolveResult:
        cache_result = self.check_cache(
            provider,
            infohash,
            item=item,
            stream=stream,
        )
        if not cache_result.is_cached:
            return ProviderResolveResult(
                infohash=infohash,
                provider=provider.key,
                status=ProviderResolveStatus.NOT_CACHED,
                download_result=None,
            )

        return self.resolve_cached(
            provider,
            infohash,
            item=item,
            stream=stream,
            cache_result=cache_result,
        )

    def resolve_cached(
        self,
        provider: "DownloaderBase",
        infohash: str,
        *,
        item: "MediaItem",
        stream: "Stream",
        cache_result: ProviderCacheResult,
    ) -> ProviderResolveResult:
        if not cache_result.is_cached:
            return ProviderResolveResult(
                infohash=infohash,
                provider=provider.key,
                status=ProviderResolveStatus.NOT_CACHED,
                download_result=None,
            )

        assert cache_result.container is not None
        download_result = self._downloader.download_cached_stream_on_service(
            stream,
            cache_result.container,
            provider,
        )

        if not self._downloader.update_item_attributes(item, download_result, provider):
            raise ProviderNoMatchingFilesError(
                f"No valid files found for {item.log_string} ({item.id})"
            )

        return ProviderResolveResult(
            infohash=infohash,
            provider=provider.key,
            status=ProviderResolveStatus.RESOLVED,
            download_result=download_result,
        )

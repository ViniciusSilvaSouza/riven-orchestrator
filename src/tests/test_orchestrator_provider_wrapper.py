from unittest.mock import Mock

import pytest

from program.orchestrator.provider_wrapper import (
    ProviderNoMatchingFilesError,
    ProviderResolveStatus,
    ProviderResolveWrapper,
)


def test_check_cache_returns_not_cached_when_provider_has_no_container():
    downloader = Mock()
    downloader.validate_stream_on_service.return_value = None

    wrapper = ProviderResolveWrapper(downloader)
    provider = Mock(key="realdebrid")
    item = Mock(id=1, log_string="Movie A")
    stream = Mock(infohash="hash-1")

    result = wrapper.check_cache(provider, "hash-1", item=item, stream=stream)

    assert result.status == ProviderResolveStatus.NOT_CACHED
    assert result.is_cached is False
    assert result.container is None


def test_check_cache_returns_acquiring_when_provider_keeps_pending_torrent():
    downloader = Mock()
    pending_container = Mock(files=[], torrent_id="rd-123", torrent_info=Mock(status="queued"))
    downloader.validate_stream_on_service.return_value = pending_container

    wrapper = ProviderResolveWrapper(downloader)
    provider = Mock(key="realdebrid")
    item = Mock(id=10, log_string="Episode A")
    stream = Mock(infohash="hash-pending")

    result = wrapper.check_cache(
        provider,
        "hash-pending",
        item=item,
        stream=stream,
        allow_pending=True,
    )

    assert result.status == ProviderResolveStatus.ACQUIRING
    assert result.is_cached is False
    assert result.is_acquiring is True
    assert result.container is pending_container


def test_resolve_returns_resolved_when_provider_succeeds():
    downloader = Mock()
    container = Mock()
    download_result = Mock()
    downloader.validate_stream_on_service.return_value = container
    downloader.download_cached_stream_on_service.return_value = download_result
    downloader.update_item_attributes.return_value = True

    wrapper = ProviderResolveWrapper(downloader)
    provider = Mock(key="alldebrid")
    item = Mock(id=2, log_string="Movie B")
    stream = Mock(infohash="hash-2")

    result = wrapper.resolve(provider, "hash-2", item=item, stream=stream)

    assert result.status == ProviderResolveStatus.RESOLVED
    assert result.is_resolved is True
    assert result.download_result is download_result


def test_resolve_raises_when_download_has_no_matching_files():
    downloader = Mock()
    downloader.validate_stream_on_service.return_value = Mock()
    downloader.download_cached_stream_on_service.return_value = Mock()
    downloader.update_item_attributes.return_value = False

    wrapper = ProviderResolveWrapper(downloader)
    provider = Mock(key="debridlink")
    item = Mock(id=3, log_string="Movie C")
    stream = Mock(infohash="hash-3")

    with pytest.raises(ProviderNoMatchingFilesError):
        wrapper.resolve(provider, "hash-3", item=item, stream=stream)

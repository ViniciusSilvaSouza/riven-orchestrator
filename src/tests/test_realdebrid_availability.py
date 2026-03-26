from program.services.downloaders.models import (
    DebridFile,
    TorrentContainer,
    TorrentInfo,
    TorrentProbeResult,
    TorrentProbeStatus,
)
from program.services.downloaders.realdebrid import RealDebridDownloader


def test_get_instant_availability_caches_torrent_context(monkeypatch):
    downloader = RealDebridDownloader()
    container = TorrentContainer(
        infohash="abc123",
        files=[
            DebridFile(
                filename="Movie.2025.1080p.mkv",
                filesize=2_000_000_000,
                file_id=1,
            )
        ],
    )
    torrent_info = TorrentInfo(
        id="rd-123",
        name="Movie.2025.1080p",
        status="downloaded",
        infohash="abc123",
    )

    monkeypatch.setattr(downloader, "add_torrent", lambda infohash: "rd-123")
    monkeypatch.setattr(
        downloader,
        "probe_torrent",
        lambda torrent_id, infohash, item_type, greedy=True: TorrentProbeResult(
            status=TorrentProbeStatus.READY,
            container=container,
            info=torrent_info,
        ),
    )

    result = downloader.get_instant_availability("abc123", "movie")

    assert result is not None
    assert result.torrent_id == "rd-123"
    assert result.torrent_info == torrent_info

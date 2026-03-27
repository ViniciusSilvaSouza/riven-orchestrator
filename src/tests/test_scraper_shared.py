from program.services.scraper_selection import get_episode_stream_rank_adjustment
from program.services.scrapers.shared import (
    ParseDiagnostics,
    _classify_parse_exception,
    _with_rank_adjustment,
)
from pydantic import BaseModel, ConfigDict


class FrozenTorrent(BaseModel):
    model_config = ConfigDict(frozen=True)

    infohash: str
    rank: int


def test_episode_specific_release_gets_large_bonus():
    adjustment = get_episode_stream_rank_adjustment(
        episode_number=31,
        absolute_number=None,
        season_number=5,
        candidate_episodes=[31],
        candidate_seasons=[5],
    )

    assert adjustment == 2000


def test_multi_episode_pack_gets_penalty_for_single_episode_item():
    adjustment = get_episode_stream_rank_adjustment(
        episode_number=31,
        absolute_number=None,
        season_number=5,
        candidate_episodes=list(range(25, 39)),
        candidate_seasons=[5],
    )

    assert adjustment < 0


def test_season_only_pack_gets_penalty_for_single_episode_item():
    adjustment = get_episode_stream_rank_adjustment(
        episode_number=31,
        absolute_number=None,
        season_number=5,
        candidate_episodes=[],
        candidate_seasons=[5],
    )

    assert adjustment == -1000


def test_with_rank_adjustment_returns_updated_copy_for_frozen_torrent():
    torrent = FrozenTorrent(infohash="abc123", rank=100)

    adjusted = _with_rank_adjustment(torrent, 200)

    assert adjusted.rank == 300
    assert torrent.rank == 100
    assert adjusted is not torrent


def test_parse_diagnostics_rejection_summary_is_sorted():
    diagnostics = ParseDiagnostics()
    diagnostics.reject("year_mismatch")
    diagnostics.reject("parse_error")
    diagnostics.reject("year_mismatch")

    assert diagnostics.rejection_summary() == "year_mismatch=2, parse_error=1"


def test_classify_parse_exception_maps_title_mismatch():
    error = ValueError(
        "GarbageTorrent 'foo' does not match the correct title. correct title: 'The Rookie'"
    )
    assert _classify_parse_exception(error) == "title_mismatch"

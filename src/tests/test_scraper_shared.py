from program.services.scraper_selection import get_episode_stream_rank_adjustment


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

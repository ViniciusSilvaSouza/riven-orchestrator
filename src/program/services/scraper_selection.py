def get_episode_stream_rank_adjustment(
    *,
    episode_number: int,
    absolute_number: int | None,
    season_number: int | None,
    candidate_episodes: list[int] | None,
    candidate_seasons: list[int] | None,
) -> int:
    """Prefer exact episode releases over broad season packs for episode items."""

    normalized_episodes = {
        episode for episode in (candidate_episodes or []) if episode and episode > 0
    }
    if normalized_episodes:
        if episode_number in normalized_episodes or (
            absolute_number is not None and absolute_number in normalized_episodes
        ):
            episode_count = len(normalized_episodes)
            if episode_count == 1:
                return 2000
            if episode_count <= 3:
                return 750
            return -1500 - min(episode_count, 50) * 10
        return 0

    normalized_seasons = {
        season for season in (candidate_seasons or []) if season and season > 0
    }
    if season_number is not None and season_number in normalized_seasons:
        return -1000

    return 0

from program.settings import settings_manager
from program.settings.models import AppModel


def test_orchestrator_env_overrides_parse_and_validate(monkeypatch):
    monkeypatch.setenv("RIVEN_DOWNLOADERS_ORCHESTRATOR_ENABLED", "true")
    monkeypatch.setenv("RIVEN_DOWNLOADERS_ORCHESTRATOR_PROVIDER_STRATEGY", "priority")
    monkeypatch.setenv(
        "RIVEN_DOWNLOADERS_ORCHESTRATOR_PROVIDER_PRIORITY",
        '["alldebrid","realdebrid","debridlink"]',
    )
    monkeypatch.setenv("RIVEN_DOWNLOADERS_ORCHESTRATOR_CACHE_NEGATIVE_TTL_MINUTES", "45")
    monkeypatch.setenv("RIVEN_DOWNLOADERS_ORCHESTRATOR_SHARED_QUEUE", "true")
    monkeypatch.setenv("RIVEN_DOWNLOADERS_ORCHESTRATOR_SHARED_QUEUE_POLL_SECONDS", "7")
    monkeypatch.setenv(
        "RIVEN_DOWNLOADERS_ORCHESTRATOR_SHARED_QUEUE_MAX_PARALLEL_TASKS",
        "12",
    )
    monkeypatch.setenv(
        "RIVEN_DOWNLOADERS_ORCHESTRATOR_COOLDOWN_MINUTES_RATE_LIMITED",
        "4",
    )
    monkeypatch.setenv("RIVEN_DOWNLOADERS_ORCHESTRATOR_COOLDOWN_MINUTES_TIMEOUT", "3")
    monkeypatch.setenv("RIVEN_DOWNLOADERS_ORCHESTRATOR_COOLDOWN_MINUTES_DOWN", "6")
    monkeypatch.setenv(
        "RIVEN_DOWNLOADERS_ORCHESTRATOR_RATE_LIMITS_REALDEBRID_PER_MINUTE",
        "180",
    )
    monkeypatch.setenv(
        "RIVEN_DOWNLOADERS_ORCHESTRATOR_RATE_LIMITS_DEBRIDLINK_PER_MINUTE",
        "220",
    )
    monkeypatch.setenv(
        "RIVEN_DOWNLOADERS_ORCHESTRATOR_RATE_LIMITS_ALLDEBRID_PER_MINUTE",
        "380",
    )
    monkeypatch.setenv(
        "RIVEN_DOWNLOADERS_ORCHESTRATOR_RATE_LIMITS_THRESHOLD_RATIO",
        "0.7",
    )

    base = settings_manager.settings.model_dump()
    checked = settings_manager.check_environment(base, "RIVEN")
    validated = AppModel.model_validate(checked)
    orchestrator = validated.downloaders.orchestrator

    assert orchestrator.enabled is True
    assert orchestrator.provider_strategy == "priority"
    assert orchestrator.provider_priority == ["alldebrid", "realdebrid", "debridlink"]
    assert orchestrator.cache_negative_ttl_minutes == 45
    assert orchestrator.shared_queue is True
    assert orchestrator.shared_queue_poll_seconds == 7
    assert orchestrator.shared_queue_max_parallel_tasks == 12
    assert orchestrator.cooldown_minutes_rate_limited == 4
    assert orchestrator.cooldown_minutes_timeout == 3
    assert orchestrator.cooldown_minutes_down == 6
    assert orchestrator.rate_limits.realdebrid_per_minute == 180
    assert orchestrator.rate_limits.debridlink_per_minute == 220
    assert orchestrator.rate_limits.alldebrid_per_minute == 380
    assert orchestrator.rate_limits.threshold_ratio == 0.7


from program.orchestrator.provider_workers import ProviderQueueWorkers


def test_provider_workers_run_lanes_with_provider_isolation():
    workers = ProviderQueueWorkers()
    calls = []

    def worker_fn(provider: str, task_id: int) -> bool:
        calls.append((provider, task_id))
        return task_id % 2 == 0

    result = workers.run_provider_lanes(
        {"realdebrid": [1, 2], "alldebrid": [3, 4]},
        worker_fn,
        max_workers=2,
    )

    assert result.attempted_tasks == 4
    assert result.successful_tasks == 2
    assert result.providers_used == 2
    assert ("realdebrid", 1) in calls
    assert ("realdebrid", 2) in calls
    assert ("alldebrid", 3) in calls
    assert ("alldebrid", 4) in calls


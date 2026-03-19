from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Callable


@dataclass
class ProviderWorkerRunResult:
    attempted_tasks: int
    successful_tasks: int
    providers_used: int


class ProviderQueueWorkers:
    """Runs provider task lanes with one worker thread per provider lane."""

    def run_provider_lanes(
        self,
        task_lanes: dict[str, list[int]],
        worker_fn: Callable[[str, int], bool],
        *,
        max_workers: int,
    ) -> ProviderWorkerRunResult:
        if not task_lanes:
            return ProviderWorkerRunResult(
                attempted_tasks=0,
                successful_tasks=0,
                providers_used=0,
            )

        attempted = 0
        successful = 0
        providers = len(task_lanes)
        lane_workers = min(max(1, max_workers), providers)

        with ThreadPoolExecutor(max_workers=lane_workers) as executor:
            futures = [
                executor.submit(self._run_provider_lane, provider, task_ids, worker_fn)
                for provider, task_ids in task_lanes.items()
            ]

            for future in as_completed(futures):
                lane_attempted, lane_successful = future.result()
                attempted += lane_attempted
                successful += lane_successful

        return ProviderWorkerRunResult(
            attempted_tasks=attempted,
            successful_tasks=successful,
            providers_used=providers,
        )

    def _run_provider_lane(
        self,
        provider: str,
        task_ids: list[int],
        worker_fn: Callable[[str, int], bool],
    ) -> tuple[int, int]:
        attempted = 0
        successful = 0

        for task_id in task_ids:
            attempted += 1
            if worker_fn(provider, task_id):
                successful += 1

        return (attempted, successful)


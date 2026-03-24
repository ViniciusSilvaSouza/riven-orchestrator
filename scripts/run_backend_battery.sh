#!/usr/bin/env bash
set -euo pipefail

uv run pytest \
  src/tests/test_main_runtime.py \
  src/tests/test_runtime_health.py \
  src/tests/test_settings_migration.py \
  src/tests/test_event_manager.py \
  src/tests/test_states_processing.py \
  src/tests/test_overseerr_sync.py \
  src/tests/test_orchestrator.py \
  src/tests/test_orchestrator_play.py \
  src/tests/test_orchestrator_provider_workers.py \
  src/tests/test_orchestrator_provider_wrapper.py \
  src/tests/test_orchestrator_queue.py \
  src/tests/test_orchestrator_settings_env.py

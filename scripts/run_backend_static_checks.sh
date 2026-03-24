#!/usr/bin/env bash
set -euo pipefail

uv run python -m py_compile \
  src/main.py \
  src/program/apis/__init__.py \
  src/program/managers/event_manager.py \
  src/program/orchestrator/debrid_manager.py \
  src/program/program.py \
  src/program/scheduling/scheduler.py \
  src/routers/secure/default.py \
  src/routers/secure/scrape.py \
  src/tests/test_event_manager.py \
  src/tests/test_main_runtime.py \
  src/tests/test_orchestrator.py \
  src/tests/test_settings_migration.py \
  src/tests/test_states_processing.py

docker compose -f docker-compose.dev.yml config --quiet
docker compose -f docker-compose.yml config --quiet

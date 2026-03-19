# Riven Orchestrator - 100% Delivery Checklist

This checklist is the source of truth for closing the original implementation plan.
Nothing is considered done without code, tests, and execution evidence.

## Delivery Gate (Definition of Done)

An EPIC can be marked as `DONE` only when all items below are satisfied:

- Code implementation is merged and callable in runtime flow.
- Unit/integration tests exist for the new behavior.
- Test execution is green on the targeted suite.
- Operational evidence is recorded (endpoint/log/status snapshot).
- No known blocker remains open for the EPIC.

## Current Baseline (as of 2026-03-18)

- EPIC 1: DONE
- EPIC 2: DONE
- EPIC 3: DONE
- EPIC 4: DONE
- EPIC 5: DONE
- EPIC 6: DONE
- EPIC 7: DONE
- EPIC 8: PARTIAL

## EPIC Acceptance Checklist

### EPIC 1 - Debrid orchestration layer

- [x] Interception exists between scraping/downloader flow.
- [x] Central manager module exists (`DebridManager`).
- [x] Explicit provider wrapper contract implemented (`resolve(hash)`, `check_cache(hash)`).
- [x] Wrapper contract covered by tests.
- Status: DONE

### EPIC 2 - Resolution cache

- [x] Persistent table exists (`DebridResolutionCache`).
- [x] Lookup before resolve exists.
- [x] Persistence after provider attempt exists.
- [x] Negative cache TTL behavior exists.
- Status: DONE

### EPIC 3 - Provider rate limiting

- [x] Per-provider limiter exists.
- [x] Threshold ratio exists (safe budget).
- [x] Integrated before provider attempt.
- [x] Configurable in settings.
- Status: DONE

### EPIC 4 - Multi-debrid (parallel)

- [x] Multiple active providers supported.
- [x] Strategy modes exist (`priority`, `balanced`).
- [x] Provider usage persisted via queue/cache/status.
- [x] True parallel resolve for same hash across providers.
- [x] Test proving parallel same-hash fallback race behavior.
- Status: DONE

### EPIC 5 - Shared queue (feature flag)

- [x] Persistent queue model/table exists.
- [x] Scheduler polling with feature flag exists.
- [x] Priority levels exist (high/normal/low).
- [x] Parallel task processing exists.
- [x] Dedicated worker isolation per provider exists as explicit component.
- [x] Tests for provider-worker fairness/isolation exist.
- Status: DONE

### EPIC 6 - Resolve on play

- [x] Internal flow exists (`resolve_on_play`).
- [x] High-priority enqueue on play exists.
- [x] Blocking wait with timeout exists.
- [x] Routed through API and stream entrypoint.
- Status: DONE

### EPIC 7 - Provider health checks

- [x] Error classification exists (429/timeout/down).
- [x] Health states exist (`healthy`, `rate_limited`, `down`).
- [x] Cooldown policy exists with per-class durations.
- [x] Snapshot/status endpoint exposes provider health.
- Status: DONE

### EPIC 8 - Configuration

- [x] Settings model exposes orchestrator knobs.
- [ ] `.env.example` documents orchestrator controls clearly.
- [ ] Runtime validation/tests for full config surface.
- Status: PARTIAL

## Execution Plan to Reach 100%

### Step 1 - Close EPIC 1 gap

- Implement explicit provider wrapper contract and plug manager into it.
- Add unit tests for wrapper behavior and adapter mapping.

### Step 2 - Close EPIC 4 gap

- Implement same-hash multi-provider parallel resolution with first-success short-circuit.
- Add tests validating race/fallback behavior.

### Step 3 - Close EPIC 5 gap

- Implement explicit provider-dedicated worker lane on top of shared queue.
- Add fairness/isolation tests.

### Step 4 - Close EPIC 8 gap

- Add orchestrator env examples and operational docs.
- Add config-load tests and startup validation checks.

### Step 5 - Final audit

- Re-run orchestrator test suite.
- Produce final EPIC matrix with all statuses set to `DONE`.

## Verification Commands

Use this as minimum verification before marking any EPIC as `DONE`:

```bash
uv run --no-project --with pytest --with trio --with trio-util --with plexapi --with lxml --with loguru --with kink --with sqlalchemy --with pydantic --with psycopg2-binary --with sqla-wrapper --with rank-torrent-name --with httpx --with requests --with lazy-imports python -m pytest src/tests/test_orchestrator.py src/tests/test_orchestrator_queue.py src/tests/test_orchestrator_play.py src/tests/test_orchestrator_provider_wrapper.py -q
```

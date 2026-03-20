# Riven Fork Notes: Seerr, Jellyfin, and Availability Sync

## Why this fork exists

This fork is focused on a Jellyfin-first, Debrid-first workflow where:

- `Seerr` is the request UI
- `Riven` is the orchestrator for indexing, scraping, debrid resolution, VFS, and playback readiness
- `Jellyfin` is the media server

The upstream stack already had the right building blocks, but it still behaved as if
`Seerr/Overseerr` were only a request source and not a system that needed accurate
status feedback once Riven actually resolved content.

That left a gap:

- requests could enter Riven
- Riven could scrape, resolve, and materialize files in VFS
- Jellyfin could see the content
- but `Seerr` still required manual availability updates in scenarios where no Arr stack
  was present to report status back

For a self-hosted `Stremio + Debrid` style experience, that manual step breaks the flow.

## Problem statement

Before this fork work, the Seerr integration was effectively read-only:

- Riven polled or received webhook requests from Seerr
- requests were converted into `MediaItem`s
- Seerr request context was not persisted cleanly
- Seerr media availability was not written back when Riven actually finished the job

There were also two structural issues:

1. `request id` and `media id` were not clearly separated in persistence.
2. Show availability had no clean path to express `partially_available` vs `available`
   in a Jellyfin-first workflow.

## Why not emulate Radarr/Sonarr

One option would be to mimic the Arr stack closely enough that Seerr thinks it is
talking to Radarr/Sonarr.

This fork deliberately does **not** do that.

Reasons:

- it would duplicate queue, history, profile, monitoring, and import semantics that
  Riven already owns
- it would create a compatibility layer that is expensive to maintain
- it would make upstream rebases harder
- it would optimize for pretending to be Arr rather than exposing what Riven actually
  knows: scrape status, debrid resolution, VFS availability, and media-server refresh

The better design is native integration:

- Seerr remains the request frontend
- Riven remains the execution engine
- Jellyfin remains the media server
- Seerr receives accurate status write-back from Riven

## What this fork changes

This fork evolves the Seerr integration in a Jellyfin-native direction:

### 1. Seerr request context is persisted more cleanly

- `requested_id` stores the Seerr request id
- `overseerr_id` stores the Seerr media id
- `requested_seasons` stores requested season scope when that information is available

This makes later status sync possible without guessing which identifier is which.

### 2. Webhook and polling flows share the same item-building logic

The Seerr/Overseerr API adapter now centralizes how incoming requests are converted into
Riven `MediaItem`s.

Benefits:

- less duplicated request parsing
- cleaner mapping of request id vs media id
- better handling of requested season scope

### 3. Existing items can be enriched with Seerr context

When polling sees a request for content that already exists in the Riven database,
the fork updates the existing item's Seerr request metadata instead of simply dropping it.

This helps avoid stale request context after retries, re-requests, or mixed polling/webhook flows.

### 4. Availability is written back to Seerr

Once content is materialized and refreshed through the updater pipeline, Riven can now
push status back to Seerr:

- `available` for movies that are actually ready
- `partially_available` for shows where some requested scope is ready
- `available` for shows once the requested scope is fully ready

This removes the need for manual "mark as available" actions in the normal Riven flow.

## Current behavior and scope

This work intentionally focuses on the availability loop, not on imitating the full Arr model.

Current scope:

- Seerr request ingestion
- Seerr request context persistence
- availability sync back to Seerr
- Jellyfin-first flow without depending on Sonarr/Radarr

Not in scope yet:

- full Arr emulation
- synthetic Arr queue/history APIs
- perfect season-by-season parity with every Seerr edge case

## Why this matters for the community

This fork solves a real operator problem:

- users running Riven without a full Arr stack can still use Seerr as the request surface
- availability becomes visible in Seerr without manual intervention
- the stack better supports a self-hosted streaming workflow centered on Debrid + Jellyfin

That is the main justification for keeping this work in a maintained fork if upstream
cannot adopt it quickly.

## Upstream strategy

The long-term goal is still to keep this work as upstream-friendly as possible.

The strategy for that is:

1. keep the implementation small and native to existing Riven services
2. avoid compatibility hacks that would be hard to rebase
3. document the operational problem clearly
4. upstream isolated improvements whenever they are stable enough

In other words: the fork is not meant to diverge for the sake of divergence. It exists
to solve a Jellyfin/Seerr/Riven workflow gap with a design that can still be proposed
upstream later.

# Riven Fork Notes: Queue Orchestration, Seerr, Jellyfin, and Playback Readiness

## Why this fork exists

This fork is focused on a Jellyfin-first, Debrid-first workflow where:

- `Seerr` is the request UI
- `Riven` is the execution engine for indexing, scraping, ranking, debrid resolution, VFS, and media refresh
- `Jellyfin` is the media server

The main motivation for the fork is larger than "read requests from Seerr".

The real goal is a self-hosted `Stremio + Debrid` style stack with better control,
observability, and playback readiness than the upstream defaults currently provide for
this use case.

That means solving the full path:

1. request enters the system
2. content is indexed correctly
3. sources are scraped and ranked correctly
4. the right debrid provider is chosen at the right time
5. queueing, retries, and rate limits behave predictably
6. content becomes materially available in VFS
7. Jellyfin sees it
8. Seerr reflects the real availability state without manual intervention

This fork exists because that full loop matters more than any single integration point.

## Core motivations

### 1. Queue management needed to become first-class

Before this work, it was too easy for the stack to behave like a collection of
independent steps instead of a controlled pipeline.

For this workflow, queueing is not an implementation detail. It is the backbone of the
system:

- scraped streams need to become debrid tasks
- tasks need ordering, retries, and visibility
- provider failures and cache misses need deterministic handling
- playback should depend on actual readiness, not just "something was scraped"

That is why the fork treats queue orchestration as a product concern, not just a helper.

### 2. Multi-debrid support needed to be operationally real

Supporting multiple debrid providers is not only about storing multiple API keys.

To be useful in practice, multi-debrid orchestration needs:

- explicit provider strategy such as `priority` or `balanced`
- provider-aware queue routing
- health tracking
- cooldowns
- rate-budget awareness
- distinction between provider-level failures and content-level failures

Without that, "multi-debrid" becomes theoretical rather than reliable.

### 3. Rate limiting and provider health needed to be part of scheduling

In a real self-hosted deployment, the bottleneck is often not scraping itself but what
happens after a stream is found:

- provider request budgets can be exhausted
- uncached torrents can trigger bad retry patterns
- transient failures can poison the workflow
- one wrong error classification can incorrectly mark a provider as "down"

This fork treats rate limiting, cooldown windows, and negative cache as core
orchestration concerns because that is what keeps the stack stable over time.

### 4. Playback readiness matters more than scrape count

Finding torrents is not the same as having something playable.

For this workflow, "done" should mean:

- the stream was ranked appropriately
- the debrid service resolved it successfully
- the correct file was matched
- VFS materialized the resulting entry
- Jellyfin can actually present and play it

That is why this fork cares deeply about:

- ranking quality
- episode-vs-pack selection
- debrid cache checks
- VFS readiness
- on-demand resolution paths such as `resolve_on_play`

### 5. Seerr and Jellyfin needed a native bridge without Arr

The original gap was real:

- Seerr could send the request
- Riven could process the request
- Jellyfin could receive the resulting media
- but Seerr still required manual marking in a no-Arr workflow

For this stack, manual "mark as available" breaks the experience.

This fork therefore treats Seerr as a first-class request frontend that deserves status
write-back from Riven, instead of pretending that only Arr should own that loop.

### 6. Localization needed to be separated from scraping logic

Another important operational lesson from this fork is that display locale and scraper
query locale are not the same problem.

Users should be able to see titles, summaries, and metadata in their preferred language,
but trackers and indexers often work best with canonical or alias titles in other
languages.

That means the system needs to separate:

- metadata locale for user-facing display
- query variants and aliases for scraping
- language preferences for subtitles and playback

This fork moves in that direction because localization should not sabotage discoverability.

### 7. Self-hosted operation needed more control and less guesswork

The fork is also motivated by operational concerns:

- running the stack outside constrained environments
- understanding why VFS/FUSE fails
- having a compose-based deployment with more control
- exposing settings clearly enough to tune the system
- making the behavior observable enough to debug in the real world

This matters because a self-hosted media stack is only valuable if operators can reason
about it.

## Problem statement

Before this fork work, the main gaps were:

- Seerr integration was effectively read-only
- request context was not persisted cleanly enough for later status sync
- queue behavior was not documented clearly enough for operators
- multi-debrid behavior existed but needed stronger orchestration semantics
- rate limit and provider cooldown behavior needed clearer operational intent
- source selection for episodes could prefer bad candidates such as season packs
- playback readiness was harder to reason about than it should be
- localized titles could interfere with tracker/indexer query quality

In short: the building blocks existed, but the experience was not yet cohesive for a
Jellyfin + Debrid + Seerr workflow.

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
  knows: scrape state, queue state, debrid state, VFS readiness, and media-server refresh

The better design is native integration:

- Seerr remains the request frontend
- Riven remains the execution engine
- Jellyfin remains the media server
- Seerr receives accurate availability updates from Riven

## What this fork changes

This fork evolves the stack in a Jellyfin-native, queue-aware direction.

### 1. Queue and orchestration become explicit product concerns

This work treats orchestration as more than "call the downloader after scraping".

The direction of the fork includes:

- persistent/shared debrid resolution queue scheduling
- explicit orchestration settings
- provider strategy and provider lanes
- retry and cooldown behavior
- visibility into orchestrator state

The point is to make the queue observable and predictable.

### 2. Multi-debrid behavior is shaped around real provider constraints

The fork supports a model where multiple providers are not just configured but managed:

- providers can be prioritized or balanced
- rate budgets matter
- cooldown windows matter
- negative cache matters
- provider health should not be poisoned by content-specific failures

That is what makes multi-debrid usable for real traffic.

### 3. Scraping and ranking are optimized for playback readiness

This fork improves the path from found stream to playable file by focusing on:

- better query construction
- alias-aware scraping
- episode-first logic where appropriate
- distinction between "found something" and "found something useful"
- debrid cache awareness

This matters especially for episodic content where season packs can be technically valid
but operationally wrong for immediate playback.

### 4. Seerr request context is persisted more cleanly

- `requested_id` stores the Seerr request id
- `overseerr_id` stores the Seerr media id
- `requested_seasons` stores requested season scope when that information is available

This makes later availability sync possible without guessing which identifier is which.

### 5. Webhook and polling flows share the same item-building logic

The Seerr/Overseerr API adapter now centralizes how incoming requests are converted into
Riven `MediaItem`s.

Benefits:

- less duplicated request parsing
- cleaner mapping of request id vs media id
- better handling of requested season scope

### 6. Existing items can be enriched with fresh Seerr context

When polling sees a request for content that already exists in the Riven database,
the fork updates the existing item's Seerr metadata instead of silently dropping it.

This helps avoid stale request context after retries, re-requests, or mixed
polling/webhook flows.

### 7. Availability is written back to Seerr

Once content is materialized and refreshed through the updater pipeline, Riven can now
push status back to Seerr:

- `available` for movies that are actually ready
- `partially_available` for shows where some requested scope is ready
- `available` for shows once the requested scope is fully ready

This removes the need for manual "mark as available" actions in the normal Riven flow.

### 8. Metadata and language preferences can be tuned more intentionally

The fork also moves toward a better separation between:

- user-facing metadata locale
- scraper query aliases
- subtitle language preferences

That matters for multilingual operators who want localized UI/metadata without hurting
source discovery.

## Current behavior and scope

This work intentionally focuses on the execution loop, not on imitating the full Arr model.

Current scope:

- Seerr request ingestion
- Seerr request context persistence
- queue-aware debrid orchestration direction
- availability sync back to Seerr
- Jellyfin-first flow without depending on Sonarr/Radarr
- better operator visibility into how content becomes playable

Not in scope yet:

- full Arr emulation
- synthetic Arr queue/history APIs
- perfect season-by-season parity with every Seerr edge case
- universal support for every tracker niche without operator tuning

## Why this matters for the community

This fork solves real operator problems:

- users running Riven without a full Arr stack can still use Seerr as the request surface
- operators get a clearer execution model for queueing and debrid resolution
- availability becomes visible in Seerr without manual intervention
- playback readiness is treated as an end-to-end concern
- the stack better supports a self-hosted streaming workflow centered on Debrid + Jellyfin

That is the main justification for keeping this work in a maintained fork if upstream
cannot adopt it quickly.

## Upstream strategy

The long-term goal is still to keep this work as upstream-friendly as possible.

The strategy for that is:

1. keep the implementation small and native to existing Riven services
2. avoid compatibility hacks that would be hard to rebase
3. document the operator problem clearly
4. upstream isolated improvements whenever they are stable enough

In other words: the fork is not meant to diverge for the sake of divergence. It exists
to solve a queue, orchestration, and Jellyfin/Seerr workflow gap with a design that can
still be proposed upstream later.

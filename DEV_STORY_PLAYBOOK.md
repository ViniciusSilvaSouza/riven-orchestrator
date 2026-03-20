# Developer Story Playbook

## Purpose

This document is a practical guide for turning this fork into a professional narrative
for GitHub, LinkedIn, and future technical writing.

The goal is not to sound like marketing. The goal is to explain:

- what problem you ran into
- why the existing workflow was not enough
- what you changed
- what evidence shows the work mattered
- what kind of engineer that demonstrates you are

## The Core Positioning

Use this as the anchor sentence when describing the project:

> I built and maintain an opinionated Riven fork focused on queue orchestration,
> multi-debrid scheduling, Seerr/Jellyfin integration, and self-updating media
> libraries that are actually ready to play.

That line is useful because it says:

- this was not a toy project
- you solved distributed workflow and product problems
- you can reason about operator experience, not only code

## The Story Metric to Feed Another Model

If you want another model to generate posts, summaries, threads, or profile copy, use
this input format every time.

Call it a `Technical Story Card`.

### Technical Story Card

- `Title`: short working title
- `Audience`: hiring managers, backend engineers, self-hosted community, product-minded engineers
- `Problem`: what was broken or missing
- `Why existing tools/workflow failed`: why the default setup was not enough
- `Constraints`: runtime, infra, product, compatibility, user expectations
- `Intervention`: what you changed technically
- `Tradeoffs`: what you kept fork-specific vs upstream-friendly
- `Evidence`: logs, metrics, before/after behavior, commits, reduced manual work
- `User impact`: what became simpler, safer, faster, or more reliable
- `Engineering signal`: what this demonstrates about you
- `Call to action`: what you want the reader to do next

This is the single best structure to reuse across platforms.

## The Narrative Arc

For this project, the strongest overall story is:

1. I wanted a self-hosted media stack that behaved like a living catalog, not a pile of
   disconnected tools.
2. The original workflow could ingest requests and lists, but it lacked enough queue
   control, provider orchestration, and feedback loops to make that safe and reliable.
3. I forked the project to solve the operational gaps that mattered in the real world:
   queueing, rate limiting, multi-debrid behavior, scraper quality, Seerr status sync,
   locale separation, and deployment control.
4. I kept a clear distinction between changes that should stay in the fork and changes
   that should eventually go upstream.
5. The result is a more coherent `Seerr -> Riven -> Debrid -> VFS -> Jellyfin` workflow,
   backed by concrete debugging, code changes, and operator-focused documentation.

That arc is both technical and promotable.

## Evidence Buckets You Should Collect

When writing about this work, avoid vague claims. Use evidence from these buckets:

### Reliability

- items moving from request to playable state
- provider staying healthy after content-level failures
- fewer manual recovery steps
- fewer false "down" states

### Orchestration

- shared queue enabled
- provider-aware scheduling
- retries, cooldowns, and negative cache behaving predictably
- episode requests preferring episode-specific releases over season packs

### Product Workflow

- Seerr request gets ingested by Riven
- Riven can write availability back to Seerr
- VFS materializes the resolved content for Jellyfin
- playback readiness is based on actual resolution, not just scrape success

### Operator Experience

- clearer settings UI
- dev stack reproducibility
- easier deployment outside constrained environments
- better debugging and documentation

### Localization

- PT-BR metadata display
- subtitle preferences limited to Portuguese when appropriate
- canonical aliases preserved for scraper success

## Recommended Post Series

Do not try to tell the entire story in one post. Break it into a series.

### Post 1: Why I Forked It

Goal:

- explain the pain clearly

Angle:

- I wanted a Jellyfin-first, Debrid-first stack that could keep a library alive without
  becoming uncontrolled

What to include:

- self-updating library goal
- problems with queue pressure and provider abuse risk
- why the original workflow was close, but not enough for your use case

### Post 2: The Queue Was the Product

Goal:

- highlight backend and systems thinking

Angle:

- queue orchestration is not an implementation detail when the product promise is "press
  play on anything in the catalog"

What to include:

- shared queue
- provider lanes
- rate limiting
- cooldowns
- negative cache
- why multi-debrid is meaningless without scheduling

### Post 3: Scraping Is Not Playback

Goal:

- show judgment and operational realism

Angle:

- finding torrents is not the same as making content playable

What to include:

- ranking quality
- episode vs season pack issue
- debrid resolution
- file matching
- VFS/Jellyfin readiness

### Post 4: Seerr Without Arr

Goal:

- show product integration thinking

Angle:

- the request flow was incomplete because Seerr could not reflect real availability
  without manual marking

What to include:

- why mimicking Sonarr/Radarr is the wrong abstraction
- native Seerr availability write-back
- keeping Seerr as frontend and Riven as execution engine

### Post 5: Localization Without Breaking Search

Goal:

- show attention to user experience and internationalization

Angle:

- users should see metadata in their language without sabotaging scraper queries

What to include:

- PT-BR display locale
- canonical aliases for trackers/indexers
- subtitle preferences

### Post 6: How I Keep a Fork Maintainable

Goal:

- show engineering maturity

Angle:

- a fork should not become a dead end

What to include:

- upstream split plan
- what should be proposed upstream
- what should stay opinionated
- branch strategy and review boundaries

## GitHub Profile Framing

Good GitHub profile themes for this project:

- backend systems engineering
- product-minded infrastructure work
- self-hosted media automation with real operational constraints
- integrating open source projects without creating a maintenance trap

Good profile repo description:

> Opinionated Riven fork focused on queue orchestration, multi-debrid scheduling,
> Seerr/Jellyfin workflows, and playback-ready self-updating libraries.

Good profile pin description:

> Built to make a self-hosted `Seerr -> Riven -> Debrid -> Jellyfin` workflow observable,
> reliable, and actually usable at scale.

## LinkedIn Framing

For LinkedIn, the tone should be:

- technical
- reflective
- evidence-based
- not overly self-congratulatory

Use this structure:

1. open with the real pain
2. explain the technical constraint
3. show the intervention
4. show the result
5. end with what you learned

Good opener:

> I wanted a self-hosted media stack that could keep itself fresh without turning every
> new tracked list into uncontrolled scraping and debrid pressure.

Good close:

> The most interesting lesson was that queue design, provider health, and playback
> readiness ended up being product problems, not just backend implementation details.

## Tone Rules

When talking publicly about the fork:

- do not frame the upstream project as bad
- frame the fork as solving a different operational need
- be explicit about what is opinionated
- separate "my workflow needed this" from "everyone should do this"
- give credit to the original project

That makes you sound like a serious maintainer rather than someone trying to create
drama around a fork.

## Reusable Post Prompt

If you want another model to draft a post, use this:

> Write a technical but engaging post based on the following Technical Story Card.
> Keep the tone credible and reflective, not hype-heavy.
> Emphasize the engineering tradeoffs, operational lessons, and user impact.
> Avoid generic motivational language.

Then paste one filled `Technical Story Card`.

## Suggested First Three Public Posts

### 1. Intro post

Theme:

- why the fork exists

Desired outcome:

- establish the project and your point of view

### 2. Deep technical post

Theme:

- queue orchestration, multi-debrid, and playback readiness

Desired outcome:

- demonstrate backend/system design skill

### 3. Product integration post

Theme:

- Seerr, Jellyfin, and the no-Arr availability loop

Desired outcome:

- show that you can turn messy toolchains into coherent products

## Simple Success Metrics

Do not optimize for vanity first. Track:

- number of serious technical interactions on the post
- profile visits after each post
- stars/watchers/follows on the repo
- conversations started with engineers or recruiters
- whether readers understand the problem you solved without extra explanation

Those are better signals than raw likes alone.

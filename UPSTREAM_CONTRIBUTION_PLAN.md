# Upstream Contribution Plan

## Purpose

This document separates:

- what this fork should try to contribute back to `rivenmedia/riven`
- what should remain fork-specific because it reflects an opinionated product direction

The goal is to keep the fork maintainable without turning upstream collaboration into a
single giant PR that mixes broad improvements with workflow-specific choices.

## Contribution Principles

An upstream candidate should meet most of these criteria:

- improves correctness, stability, or observability for many users
- does not depend on this fork's branding, deployment model, or product narrative
- can be explained in isolation
- has a small enough scope to review independently
- does not require the upstream project to adopt the entire Jellyfin-first fork vision

A fork-only change usually has one or more of these properties:

- it is tightly tied to a `Seerr + Jellyfin + Debrid` operating model
- it changes the product direction rather than just improving implementation quality
- it depends on this fork's deployment assumptions
- it is valuable, but too opinionated to be a safe default upstream

## Recommended Branch Strategy

For upstream work, do not open PRs from the fork's general `main` branch.

Use a fresh branch per candidate, created from `upstream/main`, and port changes
manually or by careful cherry-picking.

Suggested naming pattern:

- `upstream/orchestrator-provider-error-classification`
- `upstream/prowlarr-canonical-aliases`
- `upstream/seerr-availability-writeback`
- `upstream/metadata-locale-separation`

Suggested workflow:

1. Create an issue upstream first when the change is product-visible or architectural.
2. Create a clean branch from `upstream/main`.
3. Port only the minimal files needed for that improvement.
4. Remove fork-only docs, deployment changes, and unrelated refactors from the branch.
5. Run formatting and tests expected by upstream.
6. Open a narrowly scoped PR with a before/after operational explanation.

## Good Upstream Candidates

### 1. Debrid provider error classification and recovery

Why it is a good candidate:

- prevents provider health from being poisoned by content-specific failures
- improves queue stability without forcing a specific product direction
- benefits any deployment using one or more debrid providers

Fork context:

- provider cooldown and health handling was improved to distinguish real provider outages
  from file-matching failures and similar content-level errors

Suggested branch:

- `upstream/orchestrator-provider-error-classification`

### 2. Episode-aware ranking over season packs

Why it is a good candidate:

- improves correctness for episodic content
- addresses a real playback-readiness problem
- does not depend on fork branding or deployment choices

Fork context:

- ranking logic was adjusted so an episode request is less likely to prefer a season pack
  when an episode-specific release exists

Suggested branch:

- `upstream/episode-ranking-over-packs`

### 3. Canonical aliases for Prowlarr queries

Why it is a good candidate:

- improves scraper quality for localized or alternate titles
- keeps display locale and query locale separate
- helps users outside English-first environments without forcing UI localization choices

Fork context:

- Prowlarr queries were updated to use canonical and alias-aware variants instead of
  relying only on the localized display title

Suggested branch:

- `upstream/prowlarr-canonical-aliases`

### 4. Seerr availability write-back

Why it is a good candidate:

- addresses a real gap in no-Arr workflows
- improves request status accuracy
- aligns with the idea that Riven should report real availability when it knows it

Fork context:

- request/media identity handling was improved
- requested season context was persisted
- updater flow was extended so Riven can push availability state back to Seerr

Suggested branch:

- `upstream/seerr-availability-writeback`

Note:

- this should probably be proposed only after being reduced to the smallest coherent
  patch set and explained clearly as a no-Arr integration improvement, not as part of the
  broader fork vision

### 5. Metadata locale vs scraper query locale separation

Why it is a good candidate:

- improves user-facing metadata for non-English users
- avoids weakening scraping quality
- has a clean conceptual model

Fork context:

- PT-BR-facing metadata and subtitle preferences were added while keeping scraping based
  on canonical or alias titles

Suggested branch:

- `upstream/metadata-locale-separation`

## Likely Fork-Specific Changes

These are valuable, but they are more opinionated and should probably stay in the fork
unless upstream explicitly wants them.

### 1. Full queue-first product framing

Examples:

- treating orchestration as the central product concern
- positioning the fork as a `Stremio + Debrid`-like self-hosted execution engine

Why it stays here:

- this is more a product strategy than an isolated implementation improvement

### 2. Deployment and operator experience tailored to this workflow

Examples:

- Cosmos-specific deployment guidance
- DUMB/Cosmos operational workarounds
- local dev stack decisions shaped around this exact setup

Why it stays here:

- these changes are environment-specific and do not necessarily belong in upstream docs

### 3. Fork branding and public rationale

Examples:

- `FORK_RATIONALE.md`
- fork README positioning
- communication around why this fork exists

Why it stays here:

- this is fork governance, not upstream product documentation

### 4. PT-BR-first defaults as an opinionated distribution choice

Examples:

- defaults that specifically reflect a Portuguese-first deployment

Why it stays here:

- the underlying locale support may be upstreamable, but fork-specific defaults should
  remain local unless upstream wants them

## Review Checklist Before Opening Any Upstream PR

- Is the patch still useful if the reviewer does not care about this fork?
- Is the problem statement operationally concrete?
- Did we remove unrelated deployment/doc/branding changes?
- Did we avoid presenting a workflow preference as a universal requirement?
- Does the PR explain user impact, not just code movement?
- Is the scope small enough to review in one pass?

## Current Recommendation

Maintain this fork as the opinionated distribution for the target workflow.

Contribute back in slices, not in one merger attempt.

That gives the project the best chance of:

- keeping a usable independent fork
- staying rebase-friendly with upstream
- earning trust through focused upstream contributions
- avoiding unnecessary conflict over product direction

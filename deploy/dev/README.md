# Local Dev Stack

This stack is meant for validating the fork locally before publishing an image.

## What it gives you

- local build of the current `Dockerfile`
- Riven Backend + shared Postgres
- Riven Frontend
- Jellyfin
- Seerr
- Prowlarr
- Comet
- Zilean using its own database in the same Postgres instance

## Important note about FUSE on Docker Desktop

If you are running on Docker Desktop from a Windows path, backend and service integration tests are still useful, but full RivenVFS behavior can be flaky because FUSE + mount propagation are much more reliable on:

- a native Linux host
- or a WSL/Linux filesystem path instead of `C:\...`

If you want the most faithful FUSE test, run this same stack from a Linux machine or from a project checkout inside WSL.

## Suggested flow

1. Edit the root `.env`.
2. Copy the values from `.env.local.example` that you want to enable.
3. Paste your Real-Debrid API key in `RIVEN_DOWNLOADERS_REAL_DEBRID_API_KEY`.
4. If you want localized metadata in Riven, set `RIVEN_METADATA_LANGUAGE` and `RIVEN_METADATA_REGION` in the root `.env`.
5. If you want only Portuguese subtitles via post-processing, enable `RIVEN_POST_PROCESSING_SUBTITLE_*` in the root `.env`.
6. Start the stack.
7. Open the Riven Frontend and finish the configuration through the UI.
8. Open Seerr, Jellyfin, and Prowlarr to review or refine the generated local setup.

The dev compose enables Zilean and Comet scraping by default, and Comet is already wired to Prowlarr plus anime-oriented scrapers such as Nyaa, AnimeTosho, SeaDex, and NekoBT. The backend API key used by the frontend is pinned in the compose for local testing.

## Commands

Build the local image:

```bash
docker compose -f deploy/dev/docker-compose.local.yml build riven
```

Start the stack:

```bash
docker compose -f deploy/dev/docker-compose.local.yml up -d
```

Follow logs:

```bash
docker compose -f deploy/dev/docker-compose.local.yml logs -f riven
```

Stop everything:

```bash
docker compose -f deploy/dev/docker-compose.local.yml down
```

## URLs

- Riven: `http://localhost:8080`
- Riven Frontend: `http://localhost:3000`
- Jellyfin: `http://localhost:8096`
- Seerr: `http://localhost:5055`
- Prowlarr: `http://localhost:9696`
- Comet: `http://localhost:8000`
- Zilean: `http://localhost:8181`

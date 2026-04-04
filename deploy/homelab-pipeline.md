# Homelab Deploy Pipeline (Fork)

This fork publishes two GHCR images in your own account by default:

- `ghcr.io/<your-user>/riven-multi-debrid-queue-orchestrator-backend`
- `ghcr.io/<your-user>/riven-multi-debrid-queue-orchestrator-frontend`

## How the automation works

1. Backend repo (`riven-orchestrator`)
- `docker-build-dev.yml` publishes `dev`/`sha-*` tags from `main`.
- `docker-build.yml` publishes release tags (`vX.Y.Z`) and updates `stable`.

2. Frontend repo (`riven-frontend`)
- `docker-frontend-build-dev.yml` publishes `dev`/`sha-*` tags from active branches.
- `docker-frontend-build.yml` publishes release tags (`vX.Y.Z`) and updates `stable`.

3. Deploy (`deploy-production.yml` in backend repo)
- Triggered by release tag push or manual dispatch.
- Connects to your server by SSH.
- Pulls and restarts `riven` + `riven_frontend` in `docker-compose.prod.yml`.

## Required GitHub secrets (backend repo)

- `DEPLOY_HOST`
- `DEPLOY_USER`
- `DEPLOY_SSH_KEY`
- `DEPLOY_PATH`
- `GHCR_TOKEN` (PAT with `read:packages`)

The workflow now derives the GHCR username from the backend repository owner automatically.

## Optional: publish from the original repo into a different GHCR account

If you need to keep committing in one GitHub account/repository but publish container
images into another account temporarily, configure these in both repositories:

- Repository variable: `GHCR_OWNER=<target-ghcr-username>`
- Repository secret: `GHCR_PUBLISH_TOKEN=<PAT from the target account with write:packages>`

With those configured, build workflows push to:

- `ghcr.io/<GHCR_OWNER>/riven-multi-debrid-queue-orchestrator-backend`
- `ghcr.io/<GHCR_OWNER>/riven-multi-debrid-queue-orchestrator-frontend`

If `GHCR_OWNER` and `GHCR_PUBLISH_TOKEN` are not set, the workflows fall back to the
repository owner and `GITHUB_TOKEN`.

## Server-side setup

1. Clone your fork on the server.
2. Create `.env` from `.env.example`.
3. Configure:
- `RIVEN_IMAGE=ghcr.io/<your-user>/riven-multi-debrid-queue-orchestrator-backend:stable`
- `RIVEN_FRONTEND_IMAGE=ghcr.io/<your-user>/riven-multi-debrid-queue-orchestrator-frontend:stable`
- `RIVEN_FRONTEND_AUTH_SECRET=<long-random-secret>`

4. Start:

```bash
docker compose -f docker-compose.prod.yml up -d
```

## Manual publish (when you want to do it yourself)

From each repo, create and push a version tag:

```bash
git tag v1.0.0
git push origin v1.0.0
```

That tag triggers the release image workflow.  
To publish without tag, run the workflow manually from GitHub Actions.

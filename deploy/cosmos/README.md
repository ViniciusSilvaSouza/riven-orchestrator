# Cosmos Deployment

This folder contains a production-oriented stack for running this fork outside DUMB and importing it into Cosmos.

## Why this is safer than DUMB for RivenVFS

The official Riven container already prepares the FUSE runtime by:

- enabling `user_allow_other` in `/etc/fuse.conf`
- mounting `/dev/fuse`
- adding `SYS_ADMIN`
- expecting a dedicated `/mount` bind with mount propagation

That means the main FUSE risks move from "process manager mismatch" to normal host setup concerns:

- host mount propagation not configured
- `/dev/fuse` not available
- apparmor or host policy blocking FUSE
- stale old mountpoints after a crash

## Recommended architecture

- `riven` + `riven-postgres` in the same stack
- `jellyfin`, `seerr`, and `prowlarr` on the same Docker network
- `zilean` optional but colocated here for convenience because Riven can talk to it over the internal service name

Internal URLs used by Riven in this stack:

- `http://jellyfin:8096`
- `http://seerr:5055`
- `http://prowlarr:9696`
- `http://zilean:8181`

## 1. Publish your fork image

This repository now publishes images to:

- `ghcr.io/<your-github-user>/<your-repo>:main` on pushes to the default branch
- `ghcr.io/<your-github-user>/<your-repo>:vX.Y.Z` on git tags

Before deploying:

1. Push your branch to GitHub.
2. Let the GitHub Actions workflow finish.
3. Open the GitHub Packages page for the image and make it public if needed.

## 2. Prepare the host mount path

RivenVFS still needs the host mount path to be a shared bind mount. Without this, Jellyfin and other containers may not see the mounted filesystem correctly.

Run once:

```bash
sudo mkdir -p /srv/riven/mount
sudo mount --bind /srv/riven/mount /srv/riven/mount
sudo mount --make-rshared /srv/riven/mount
findmnt -T /srv/riven/mount -o TARGET,PROPAGATION
```

Expected propagation:

- `shared`
- or `rshared`

If you want this to survive reboot cleanly, copy `riven-mount.service.example` to `/etc/systemd/system/riven-mount.service`, adjust the path if needed, then run:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now riven-mount.service
```

## 3. Import into Cosmos

1. Copy `docker-compose.yml` into a new stack in Cosmos.
2. Copy the variables from `.env.example` into the stack environment editor.
3. Set `RIVEN_IMAGE` to your fork image, for example:

```bash
ghcr.io/viniciussilvasouza/riven-orchestrator:main
```

4. Point `RIVEN_MOUNT_DIR` to the same host path you prepared for shared propagation.

## 4. First boot checklist

1. Start the stack.
2. Open Seerr, Jellyfin, and Prowlarr and generate their API keys.
3. Add those keys to the Riven envs or directly in the Riven settings UI.
4. Enable the integrations only after the keys are present.
5. If using Zilean, expect the first sync to take time.

## 5. Future scrapers

For additional compatible scrapers, the cleanest rule is:

- put them on the same Docker network
- expose them by service name
- point Riven to the internal URL

Examples:

- `http://torrentio-hostname-or-url`
- `http://comet:8000`
- `http://mediafusion:8000`
- `http://jackett:9117`

## 6. If FUSE still fails

Check these first:

1. `findmnt -T /srv/riven/mount -o TARGET,PROPAGATION`
2. `docker exec -it riven sh -lc "cat /etc/fuse.conf"`
3. `docker exec -it riven sh -lc "ls -l /dev/fuse"`
4. `docker inspect riven --format '{{json .HostConfig.CapAdd}}'`

If the app crashes and leaves a stale mount behind, clear it with:

```bash
sudo fusermount -uz /srv/riven/mount || sudo umount -l /srv/riven/mount
```

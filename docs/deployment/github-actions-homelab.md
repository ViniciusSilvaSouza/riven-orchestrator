# Deploy automático (GitHub -> servidor privado)

Este fork já publica imagens no GHCR via workflows:

- `docker-build-dev.yml` (branch `main`, tag `dev`)
- `docker-build.yml` (tags `vX.Y.Z`)

Agora o deploy em produção é feito por:

- `.github/workflows/deploy-production.yml`

## Como funciona

1. Você cria uma tag `vX.Y.Z` e faz push no GitHub.
2. A imagem `ghcr.io/<owner>/<repo>:vX.Y.Z` é publicada.
3. O workflow de deploy conecta no seu servidor por SSH.
4. No servidor, ele roda:
   - `docker login ghcr.io`
   - `docker compose -f docker-compose.prod.yml pull riven`
   - `docker compose -f docker-compose.prod.yml up -d riven`

Também pode ser disparado manualmente por `workflow_dispatch` (com `image_tag`).

## Pré-requisitos no servidor

1. Docker + Docker Compose plugin instalados.
2. Diretório de deploy com:
   - `docker-compose.prod.yml`
   - `.env`
3. Banco e volumes persistentes configurados no `.env`.
4. Acesso SSH do GitHub Actions (chave privada no secret).

## Secrets no GitHub (Actions)

Crie os secrets do repositório:

- `DEPLOY_HOST`: IP ou domínio do servidor.
- `DEPLOY_USER`: usuário SSH.
- `DEPLOY_SSH_KEY`: chave privada SSH (formato OpenSSH).
- `DEPLOY_PATH`: caminho no servidor onde está o compose de produção.
- `GHCR_USER`: usuário que tem acesso ao pacote no GHCR.
- `GHCR_TOKEN`: token com permissão de leitura de pacote (`read:packages`).

## Primeiro bootstrap no servidor

No servidor, no diretório de deploy:

```bash
export RIVEN_IMAGE=ghcr.io/<owner>/<repo>:latest
docker compose -f docker-compose.prod.yml pull riven
docker compose -f docker-compose.prod.yml up -d
```

Depois disso, os próximos deploys ficam automáticos pelo GitHub Actions.

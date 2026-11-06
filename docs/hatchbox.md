# Hatchbox Docker deployment

App: [openrouter-webhooks, ID 14630](https://hatchbox.io/apps/14630-openrouter-webhooks).
Repository: `Looped-Sustainability/openrouter-webhooks`. Branch: `main`.

This repo follows Hatchbox's [Docker Compose deployment guide](https://hatchbox.io/docs/articles/19-deploying-with). It contains the image definition, Compose configuration, and a repo build hook. The text API still requires a webhook URL for every request.

## 1. Prepare one application server

Use one application server for this app. The queue lives on that server's Docker volume. Separate servers do not share it.

Docker Engine and the Compose v2 plugin must be available to the `deploy` user. If Docker is not installed, an administrator can run the following on the selected server as root. Review the installer before running it.

```sh
curl -fsSL https://get.docker.com -o /tmp/install-docker.sh
sh /tmp/install-docker.sh
usermod -aG docker deploy
systemctl enable --now docker.service containerd.service
```

Start a new `deploy` session so it receives the new group membership. Then check:

```sh
docker info >/dev/null
docker compose version
```

Docker group membership grants root-level host access. Use it only for the app's trusted deploy user. See Docker's [post-install steps](https://docs.docker.com/engine/install/linux-postinstall/).

These are operator commands. They do not run from the app container or the build hook.

## 2. Connect the repository

In the Hatchbox app's repository settings:

- Select `Looped-Sustainability/openrouter-webhooks`.
- Set the branch to `main`.
- Grant the Hatchbox GitHub App access to this repository if it cannot see it.

Push the deployment files to `main` before the first deployment.

## 3. Set the environment

Set `DATA_ENCRYPTION_KEY` once in the Hatchbox app environment. Generate a new value only for a new queue:

```sh
openssl rand -base64 32
```

If this app already has a key and queued jobs, retain that key. Replacing it prevents the service from decrypting queued caller tokens. Hatchbox does not return saved environment values through its API.

The other settings have defaults in `compose.yaml`. Optional settings include `MODEL_CONCURRENCY`, `WEBHOOK_CONCURRENCY`, `MODEL_TIMEOUT`, and `WEBHOOK_HOSTS`.

Do not add a shared `OPENROUTER_API_KEY` or proxy `API_TOKEN`. Callers supply their own OpenRouter tokens.

Hatchbox supplies the app's `PORT`. Leave it under Hatchbox's control. Compose binds `127.0.0.1:${PORT}:8080`, so Caddy can reach the container without exposing a second public port. Inside the container, `ADDR` is always `:8080` and `DATA_DIR` is always `/data`.

## 4. Use the repo build hook

Hatchbox detects `.hatchbox/build` in the repository. Its [build script documentation](https://hatchbox.io/docs/articles/18-build-scripts-and) describes this hook and the `REVISION` variable.

If the app has a custom build script field that replaces repo detection, set it to:

```sh
./.hatchbox/build
```

Remove Rails-specific commands from this app's other build hooks. This is a Go service in Docker. It has no Bundler, asset compilation, Rails migrations, or host Go build step.

The hook checks Docker access, validates Compose, and builds the image before it stops the old containers. It embeds Hatchbox's `REVISION` in the binary. It then runs `docker compose down --timeout 60` without deleting volumes. Hatchbox starts the web process for the new release.

## 5. Set the web process

Replace any default Rails web process with exactly one process assigned to the `web` role:

```sh
docker compose up --no-build --remove-orphans
```

Keep Compose attached. Do not add `-d`; Hatchbox must supervise this process. Do not add a separate worker process. The Go service runs the model and delivery workers inside this container.

Set the Hatchbox health check URI to:

```text
/healthz
```

The Compose project name is fixed as `openrouter-webhooks`. Its named volume is `openrouter-webhooks-data`. The container runs as UID 10001, and `/data` belongs to that user. Docker initializes the named volume from that directory on first use.

## 6. Deploy and verify

Deploy `main` from the app page. Wait for the deployment log to finish. Then check the public app origin and the full commit SHA:

```sh
bin/check-deploy https://YOUR-APP.hatchboxapp.com FULL_COMMIT_SHA
```

The check requires both `status: "ok"` and an exact matching `revision` from `/healthz`. A completed Hatchbox log alone does not prove that the intended image is serving requests.

Use the app's actual Hatchbox domain. The placeholder above is not a configured domain. Caddy handles HTTPS; the container serves HTTP on its private loopback binding.

After health and revision pass, use either SDK example in the README with the public `/api/v1` base URL and a webhook endpoint you control. Missing `webhook_url` must return HTTP 400. A supplied webhook queues a model call using the caller's OpenRouter account.

## Persistence and redeploys

Never run `docker compose down --volumes` or delete `openrouter-webhooks-data` while you need its jobs. Keep the same encryption key across releases. Back up the volume and protect its contents.

Redeployment stops active model calls. The service reports an interrupted call as `incomplete` and sends that result to its webhook. It does not repeat a call whose upstream outcome is unknown. Queued jobs and pending webhook deliveries resume with the same volume.

The build hook leaves the running app in place if image build fails. After it stops the old app, a later startup failure can still cause downtime. This deployment is a single-instance setup and does not promise uninterrupted model calls during a release.

## Local Docker check

Set a real encryption key in your private `.env`, then run:

```sh
docker compose up --build --detach --wait
curl --fail http://127.0.0.1:8080/healthz
docker compose down --timeout 60
```

Local builds report revision `development` unless you supply `REVISION` or `HATCHBOX_REVISION`. CI builds with the Git commit SHA and checks image health, revision, non-root execution, and volume write access.

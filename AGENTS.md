# Product goal

Build a webhook proxy for OpenRouter text generation.

- OpenRouter is the upstream provider. OpenAI already has background requests and webhooks. Use OpenAI only as a reference for client compatibility and webhook signatures.
- Callers replace the OpenRouter base URL with this service URL. Keep the standard SDK request shape. Do not introduce a separate job request wrapper.
- Each caller supplies their own OpenRouter bearer token. Do not require a shared provider key or a separate proxy token.
- Every text generation request must include a valid `webhook_url`, or the equivalent `X-Webhook-URL` header for SDKs that strip unknown fields. Reject missing URLs before calling OpenRouter.
- Text generation always runs asynchronously. There is no synchronous text mode or polling-only mode. `background: true` is optional. It does not enable an otherwise synchronous path.
- Send the final model result or error to the webhook. Persist accepted jobs and retry failed webhook deliveries. Do not repeat model calls after an unknown upstream outcome.
- Polling may inspect a webhook job. It does not replace webhook delivery.
- Test the actual OpenRouter and OpenAI SDKs. The tests must prove that neither SDK can bypass the required webhook.

# Verification

Install test dependencies with `npm ci --ignore-scripts --legacy-peer-deps`.
Run `go vet ./...`, `go test -race -count=1 -timeout=120s -v ./...`, and `go build ./...`.
The running service uses Go only. Node.js is needed for SDK tests.

# Deployment

- Run this service in Docker Compose. The Hatchbox app is 14630, openrouter-webhooks.
- Follow docs/hatchbox.md. The repo hook is .hatchbox/build. The attached web process is `docker compose up --no-build --remove-orphans`.
- Use one application server and the persistent openrouter-webhooks-data volume. Never remove that volume during a release.
- Preserve DATA_ENCRYPTION_KEY across releases. Caller tokens depend on it.
- Verify deployment with `bin/check-deploy APP_ORIGIN FULL_COMMIT_SHA`. Require healthy status and an exact matching revision before reporting success.

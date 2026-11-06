# OpenRouter webhook proxy

A Go service for asynchronous OpenRouter chat completions. It accepts a job, returns HTTP 202, calls OpenRouter, and sends a final event to your webhook.

The default worker pools allow 300 model calls and 50 webhook calls at once. Each call uses a Go goroutine. The service has no external Go dependencies.

## Run

Use Go 1.25 or later on Linux or macOS.

```sh
cp .env.example .env
# Set the keys and webhook host names in .env.
set -a
. ./.env
set +a
go run .
```

Generate separate random values for `API_TOKEN` and `WEBHOOK_SECRET`. Each value must have at least 32 characters. The OpenRouter key belongs to this service. Callers use the service token.

Keep `.env` private. The service does not load it on its own.

## Submit a job

```sh
curl -sS http://localhost:8080/v1/jobs \
  -H "Authorization: Bearer $API_TOKEN" \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: order-123-summary' \
  -d '{
    "webhook_url": "https://hooks.example.com/openrouter",
    "metadata": {"order_id": "123"},
    "request": {
      "model": "openrouter/auto",
      "messages": [{"role": "user", "content": "Write a short greeting."}],
      "max_tokens": 100
    }
  }'
```

The service returns only after it saves the job to disk.

```json
{
  "id": "f530e89e9bf7f9275a0398fb172c869e",
  "state": "queued",
  "status_url": "/v1/jobs/f530e89e9bf7f9275a0398fb172c869e"
}
```

The `request` object passes through to OpenRouter's chat completion endpoint. This includes tools, routing options, structured output, and model parameters. Set `stream` to false or omit it. This service delivers a final response rather than token events. See the [OpenRouter request reference](https://openrouter.ai/docs/api/api-reference/chat/send-chat-completion-request).

Use the same `Idempotency-Key` and body when you retry a submission. The service returns the original job. A different body with that key returns HTTP 409. This protection lasts until the job expires. Keys apply across all callers of this service. JSON key order and whitespace can affect the body comparison. Keep retry bodies unchanged.

## Receive the result

The webhook receives a JSON event. The `response` field contains OpenRouter's full JSON response.

```json
{
  "id": "5fa4a01f5b3e4b679e8dc336b634bfc8",
  "job_id": "f530e89e9bf7f9275a0398fb172c869e",
  "type": "job.succeeded",
  "created_at": "2026-11-05T22:00:00Z",
  "status_code": 200,
  "response": {
    "id": "gen-example",
    "choices": [{"message": {"role": "assistant", "content": "Hello!"}}]
  },
  "metadata": {"order_id": "123"}
}
```

Event types:

| Type | Meaning |
| --- | --- |
| `job.succeeded` | OpenRouter returned a successful response. |
| `job.failed` | OpenRouter returned an error, or the response could not be read. |
| `job.timed_out` | The model call exceeded its timeout. The upstream outcome is unknown. |
| `job.interrupted` | The process stopped during a model call. The upstream outcome is unknown. |

Error events include an `error` message. They include `response` and `status_code` when available. The service checks HTTP errors, top-level JSON errors, and completion error reasons. OpenRouter can return errors inside a response. See [OpenRouter error handling](https://github.com/OpenRouterTeam/docs/blob/main/api_reference/errors-and-debugging.mdx).

Verify these webhook headers before you process an event:

- `Webhook-Id` is the stable event ID.
- `Webhook-Timestamp` is the Unix time in seconds for this delivery attempt.
- `Webhook-Signature` is `sha256=<hex digest>`.

Compute HMAC-SHA256 with `WEBHOOK_SECRET`. Sign the timestamp, a literal period, and the exact raw request body bytes. Compare signatures in constant time. Reject timestamps more than five minutes from your current time. Keep your server clock in sync.

Return a 2xx status after you save the event. Deduplicate events by `Webhook-Id`. A lost HTTP response or a process stop can cause a second delivery. Do not require each timestamp to increase. Attempts can share the same second.

The service retries network errors and all non-2xx responses, including redirects. It uses exponential delays with jitter, from about two seconds to about 21 minutes. It makes at most 12 attempts by default. Model calls do not repeat automatically. A repeat could cause another charge. This also means a 429 response produces a final error event.

## Check a job or retry delivery

```sh
curl -sS "http://localhost:8080/v1/jobs/$JOB_ID" \
  -H "Authorization: Bearer $API_TOKEN"

curl -sS -X POST "http://localhost:8080/v1/jobs/$JOB_ID/redeliver" \
  -H "Authorization: Bearer $API_TOKEN"
```

Job states are `queued`, `running`, `delivering`, `delivered`, and `dead`. The job becomes `dead` when webhook attempts run out. The redelivery endpoint resets the delivery budget for a dead job. It sends the same event ID and does not call the model again.

`GET /healthz` needs no token. It returns HTTP 503 after a storage failure. The service then stops accepting and scheduling work. Repair the storage issue and restart the service. Active requests may still finish while storage is unavailable. An active model call whose result could not be saved will become an interrupted event after restart.

## Settings

| Variable | Default | Purpose |
| --- | --- | --- |
| `API_TOKEN` | Required | Bearer token for the job API. |
| `OPENROUTER_API_KEY` | Required | Key for model calls. |
| `WEBHOOK_SECRET` | Required | Shared webhook signing key. |
| `WEBHOOK_HOSTS` | Required | Comma-separated exact host names. |
| `ADDR` | `:8080` | HTTP listen address. |
| `DATA_DIR` | `./data` | Durable job storage. |
| `MODEL_CONCURRENCY` | `300` | Maximum active model calls. |
| `WEBHOOK_CONCURRENCY` | `50` | Maximum active webhook calls. |
| `MODEL_TIMEOUT` | `10m` | Timeout for each model call. |
| `WEBHOOK_TIMEOUT` | `15s` | Timeout for each delivery attempt. |
| `WEBHOOK_ATTEMPTS` | `12` | Delivery attempt limit. |
| `MAX_JOBS` | `10000` | Maximum stored jobs, including finished jobs. |
| `RETENTION` | `24h` | How long to retain delivered and dead jobs. |

The service limits request bodies to 1 MiB and model responses to 16 MiB. A full job store returns HTTP 503 with `Retry-After: 60`. Expired terminal jobs are deleted automatically. Pending jobs do not expire. Watch queue depth through your caller's job records and monitor disk space.

## Deploy

```sh
docker build -t openrouter-webhooks .
docker volume create openrouter-webhooks-data
docker run --rm --name openrouter-webhooks \
  --env-file .env \
  -e DATA_DIR=/data \
  -p 127.0.0.1:8080:8080 \
  -v openrouter-webhooks-data:/data \
  openrouter-webhooks
```

Put the API behind an HTTPS reverse proxy. Use a persistent local disk with atomic rename, file locks, and working `fsync`. Keep backups private. Jobs contain prompts, model responses, metadata, and webhook URLs. The service does not encrypt these files.

Run one service process per data directory. A file lock prevents a second process from opening the same directory. This version targets hundreds of concurrent calls on one server. It does not coordinate replicas. Use a shared durable queue before adding replicas. Do not use network file storage for this queue.

The queue keeps job records in memory and on disk. `MAX_JOBS` bounds the record count, not total bytes. Large outputs need more memory and disk space. Start with the default concurrency and measure your own workload. Your OpenRouter account and chosen providers can impose lower limits.

Webhook destinations must use HTTPS on port 443. The service requires an allowed host name. It also checks DNS addresses at connection time and blocks private and reserved networks. It does not follow redirects or use environment HTTP proxies. These checks reduce requests to internal services through a supplied webhook URL.

Use host names that you control. All API callers share one token and one signing secret. This service is intended for trusted callers within one organization.

On shutdown, the service stops admission, cancels active calls, and saves final events where possible. On restart, it resumes queued jobs and webhook deliveries. If a model call was in progress, it sends `job.interrupted` and does not repeat the model call.

## Verify

```sh
go vet ./...
go test -race -count=1 -timeout=60s -v ./...
```

The tests run HTTP and TLS over in-memory connections. They cover final events, signatures, timeouts, webhook retries, manual redelivery, admission limits, idempotency across restart, interrupted jobs, private network blocking, and 300 simultaneous model calls. They do not send paid OpenRouter requests.

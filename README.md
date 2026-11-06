# OpenRouter webhook proxy

Use this service in place of `https://openrouter.ai/api/v1`.

This service adds required webhooks to OpenRouter text requests. Every chat completion or Responses request must include `webhook_url`. The proxy saves the job, returns a queued response, calls OpenRouter, and sends the final result or error to that URL.

Each caller supplies their own OpenRouter token. There is no shared model account and no separate proxy API token. OpenRouter checks the token and charges that account. OpenAI and OpenRouter SDKs can call this proxy with an OpenRouter key.

OpenAI already has its own background requests and webhooks. It is only a reference for the client format and signing protocol. This service adds webhook delivery to OpenRouter text calls.

## Run

Use Go 1.25 or later on Linux or macOS.

```sh
cp .env.example .env
# Generate a key once. Save it as DATA_ENCRYPTION_KEY in .env.
openssl rand -base64 32

set -a
. ./.env
set +a
go run .
```

Keep the encryption key stable across restarts. The service uses it to encrypt caller tokens for queued jobs. The service removes a job's encrypted token from its record when the model call ends. It refuses to start if it cannot decrypt queued credentials.

The service does not load `.env` on its own. Keep that file private.

## Send a text request

Set the proxy base URL and add the required webhook destination. Keep the normal model request shape.

```js
import OpenAI from 'openai';

const client = new OpenAI({
  apiKey: process.env.OPENROUTER_API_KEY,
  baseURL: 'https://your-proxy.example/api/v1',
});

const completion = await client.chat.completions.create({
  model: 'openrouter/auto',
  messages: [{ role: 'user', content: 'Write a short greeting.' }],
  webhook_url: 'https://hooks.example.com/result',
  webhook_secret: process.env.MY_WEBHOOK_SECRET,
});
```

The OpenRouter SDK uses `serverURL` for the same setting.

```js
import { OpenRouter } from '@openrouter/sdk';

const client = new OpenRouter({
  apiKey: process.env.OPENROUTER_API_KEY,
  serverURL: 'https://your-proxy.example/api/v1',
});

const completion = await client.chat.send({
  chatRequest: {
    model: 'openrouter/auto',
    messages: [{ role: 'user', content: 'Write a short greeting.' }],
  },
}, {
  fetchOptions: {
    headers: {
      'X-Webhook-URL': 'https://hooks.example.com/result',
      'X-Webhook-Secret': process.env.MY_WEBHOOK_SECRET,
    },
  },
});
```

The `/v1` prefix is an alias for `/api/v1`. Text requests always run asynchronously. Missing, empty, or invalid webhook destinations return HTTP 400 before the service calls OpenRouter. `stream: true` and `background: false` also return HTTP 400.

## Required webhook delivery

Add `webhook_url` to every chat completion or Responses request. The proxy always runs the call in the background. `background: true` is optional and accepted for client compatibility. There is no synchronous text mode or polling-only mode.

```sh
curl -sS https://your-proxy.example/api/v1/responses \
  -H "Authorization: Bearer $OPENROUTER_API_KEY" \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: order-123-summary' \
  -d '{
    "model": "openrouter/auto",
    "input": "Write a short greeting.",
    "background": true,
    "webhook_url": "https://hooks.example.com/result"
  }'
```

The service returns HTTP 200 after it saves the job to disk. It returns a queued response in the endpoint's native format. This lets the strict OpenRouter SDK parse it.

```json
{
  "id": "resp_proxy_9c39b1d0c78c45df803e652359b72b02",
  "object": "response",
  "status": "queued",
  "background": true,
  "model": "openrouter/auto",
  "output": []
}
```

The response includes other native fields that the SDK requires. Chat completion jobs have a `chatcmpl_proxy_` ID, an empty `choices` array, and the same `status` extension.

The proxy handles `background`, `webhook_url`, and `webhook_secret`. It removes them before it calls OpenRouter. It sends the remaining model request to the same upstream endpoint in the foreground. It does not ask OpenRouter to create another background job.

Use the same body and `Idempotency-Key` when you retry a submission. A retry returns the original job. A changed body with that key returns HTTP 409. Keys apply within one caller token. Different tokens can use the same key. This protection lasts until the job expires.

For webhook jobs, supply your own `webhook_secret`, or save the generated secret from the initial response's `X-Webhook-Secret` header. A secret uses the Standard Webhooks format, `whsec_` followed by base64 for at least 32 random bytes.

## OpenAI client with a background webhook

The JavaScript SDK sends these proxy fields as part of the request.

```js
const response = await client.responses.create({
  model: 'openrouter/auto',
  input: 'Write a short greeting.',
  background: true,
  webhook_url: 'https://hooks.example.com/result',
  webhook_secret: process.env.MY_WEBHOOK_SECRET,
});

const current = await client.responses.retrieve(response.id);
```

In typed TypeScript, use the header options shown below for custom proxy fields. In the OpenAI Python SDK, use `extra_body`.

```python
from openai import OpenAI
import os

client = OpenAI(
    api_key=os.environ['OPENROUTER_API_KEY'],
    base_url='https://your-proxy.example/api/v1',
)
response = client.responses.create(
    model='openrouter/auto',
    input='Write a short greeting.',
    background=True,
    extra_body={
        'webhook_url': 'https://hooks.example.com/result',
        'webhook_secret': os.environ['MY_WEBHOOK_SECRET'],
    },
)
```

## OpenRouter client with a background webhook

The OpenRouter SDK validates requests and removes unknown fields. Use these headers through its supported request options.

```js
const response = await client.beta.responses.send(
  {
    responsesRequest: {
      model: 'openrouter/auto',
      input: 'Write a short greeting.',
    },
  },
  {
    fetchOptions: {
      headers: {
        'X-Background': 'true',
        'X-Webhook-URL': 'https://hooks.example.com/result',
        'X-Webhook-Secret': process.env.MY_WEBHOOK_SECRET,
      },
    },
  },
);
```

The same headers work with `client.chat.send({ chatRequest: ... }, options)` and with the OpenAI client's `headers` request option. They do not go to OpenRouter. They have the same meaning as the JSON proxy fields. Header values take precedence if both forms are present.

## Poll or retry a delivery

Use the same caller token to retrieve a job.

```sh
curl -sS "https://your-proxy.example/api/v1/responses/$RESPONSE_ID" \
  -H "Authorization: Bearer $OPENROUTER_API_KEY"
```

For chat completions, use `/api/v1/chat/completions/$RESPONSE_ID`. The OpenAI SDK supports `responses.retrieve(id)` and `chat.completions.retrieve(id)`.

The status starts at `queued`, changes to `in_progress`, and ends at `completed`, `failed`, `incomplete`, or `cancelled` if the provider returned that state. The final result keeps the proxy ID. Its `upstream_id` field holds OpenRouter's original ID. A caller with another token receives HTTP 404.

`X-Webhook-State` shows delivery state. It can be `queued`, `running`, `delivering`, `delivered`, or `dead`. A completed model response can still have a failed webhook delivery. The model result stays available for polling.

After delivery attempts run out, retry only the webhook with:

```sh
curl -sS -X POST \
  "https://your-proxy.example/api/v1/responses/$RESPONSE_ID/redeliver" \
  -H "Authorization: Bearer $OPENROUTER_API_KEY"
```

This sends the same event ID. It does not repeat the model call. The chat completion route supports the same `/redeliver` suffix.

## Verify a webhook

A webhook event follows the Standard Webhooks signing format. Unlike OpenAI's notification-only payload, its `data` field contains the full final result.

```json
{
  "object": "event",
  "id": "evt_7df1da47aebc47f5bdaab9c4637c6b72",
  "type": "response.completed",
  "created_at": 1794000000,
  "status_code": 200,
  "data": {
    "id": "resp_proxy_9c39b1d0c78c45df803e652359b72b02",
    "object": "response",
    "status": "completed",
    "output": []
  }
}
```

Responses events use `response.completed`, `response.failed`, `response.incomplete`, or `response.cancelled`. Chat completion events use the custom `chat.completion.` prefix with the same final states.

The official OpenAI SDK verifies these signatures.

```js
const event = await client.webhooks.unwrap(
  rawRequestBody,
  requestHeaders,
  process.env.MY_WEBHOOK_SECRET,
);
```

The proxy signs `webhook-id`, a period, `webhook-timestamp`, a period, and the exact body bytes with HMAC-SHA256. It sends the digest as `webhook-signature: v1,<base64>`. Keep the request body unchanged until you verify it. The OpenAI SDK also checks timestamp age.

Return a 2xx status after you save the event. Deduplicate events by `webhook-id`. A lost response or a process stop can cause another delivery.

The proxy retries network failures and all non-2xx responses. It does not follow redirects. It uses exponential delays with jitter and makes at most 12 attempts by default. It does not repeat model calls. This avoids a second charge after an unknown upstream outcome. Rate limit errors therefore produce a final error result.

## Scope

The proxy adds webhook delivery to OpenRouter text requests. It accepts OpenAI-compatible request shapes so both SDKs can call it. It does not add webhook support to OpenAI or reproduce OpenAI's webhook registry.

Polling checks a webhook job's status. It never replaces the required webhook. Background streaming, stream resume, and cancellation are not implemented. Model calls do not stream to the caller. Their final response goes to the supplied webhook URL.

## Settings

| Variable | Default | Purpose |
| --- | --- | --- |
| `DATA_ENCRYPTION_KEY` | Required | Base64-encoded 32-byte key for queued caller credentials. |
| `ADDR` | `:8080` | HTTP listen address. |
| `DATA_DIR` | `./data` | Durable job storage. |
| `WEBHOOK_HOSTS` | Empty | Optional comma-separated list of exact allowed host names. Empty allows any public HTTPS host. |
| `MODEL_CONCURRENCY` | `300` | Maximum active model calls. |
| `WEBHOOK_CONCURRENCY` | `50` | Maximum active delivery attempts. |
| `MODEL_TIMEOUT` | `10m` | Timeout for a model call. Foreground timeouts include time waiting for a slot. |
| `WEBHOOK_TIMEOUT` | `15s` | Timeout for each delivery attempt. |
| `WEBHOOK_ATTEMPTS` | `12` | Delivery attempt limit. |
| `MAX_JOBS` | `10000` | Maximum stored jobs, including finished jobs. |
| `RETENTION` | `24h` | Retention period after delivery ends or attempts run out. |

Text requests have a 1 MiB body limit. Model responses have a 16 MiB limit. Read-only OpenRouter routes pass through. Other submission routes return HTTP 404, so they cannot bypass the text webhook flow.

A full job store returns HTTP 503 with `Retry-After: 60`. Expired terminal jobs are deleted automatically. Pending jobs do not expire. The service holds job records in memory and on disk. The job count limit does not bound total bytes. Large outputs need more memory and disk space.

`GET /healthz` needs no token. It returns HTTP 503 after a storage failure. The service then stops text job admission and scheduling. Repair storage and restart the service.

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

Put the API behind an HTTPS reverse proxy. Keep the same data encryption key and persistent disk across restarts. Use local storage with atomic rename, file locks, and working `fsync`.

Run one process per data directory. A file lock prevents a second process from opening it. This version supports hundreds of concurrent calls on one server. It needs a shared queue before you add replicas.

The service resumes queued jobs and pending deliveries after restart. If a model call was in progress when the process stopped, it creates an `incomplete` result and does not repeat the call. The original upstream outcome may be unknown.

Webhook destinations must use HTTPS on port 443. The service resolves and checks the address when it connects. It blocks private and reserved networks. It does not follow redirects or use environment HTTP proxies. Caller tokens, webhook secrets, and response bodies never go into service logs.

Job files contain prompts, responses, webhook URLs, signing secrets, and caller token hashes. Only caller credentials use application encryption. Keep the disk and backups private. The service makes no claim that file updates erase old disk blocks.

The queue format is version 2. It cannot load version 1 job files from the earlier service-token prototype. Keep that old data separate if you need it. Do not point this version at an active version 1 queue.

## Verify

Use Node.js 22 or later for the SDK tests. The running Go service has no Node.js dependency.

```sh
npm ci --ignore-scripts --legacy-peer-deps
go vet ./...
go test -race -count=1 -timeout=120s -v ./...
go build ./...
```

The tests use the actual pinned `openai` and `@openrouter/sdk` packages. They send requests through the Go handler to local test providers. The SDK tests check that neither client can bypass the webhook requirement. They also cover asynchronous chat and Responses requests, polling, separate caller tokens, upstream errors, and OpenAI SDK webhook signature checks. A regression test checks that requests without valid webhook destinations never reach OpenRouter.

The queue tests cover 300 simultaneous model calls, delivery retries, restart recovery, encrypted credentials, token isolation, idempotency, and network address checks. They run HTTP and TLS over in-memory connections. No paid OpenRouter or OpenAI calls are part of these tests.

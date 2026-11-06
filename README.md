# OpenRouter webhook proxy

An OpenRouter proxy that adds required webhook delivery to text generation. It follows the OpenAI background-request workflow: submit a request, receive a queued response, and receive the final result through a webhook.

Replace `https://openrouter.ai/api/v1` with your proxy URL. Keep the normal model request body and your own OpenRouter bearer token. Add `webhook_url` to every text request.

**No webhook URL means HTTP 400.** The proxy rejects the request before it calls OpenRouter. Every accepted text request runs in the background. There is no synchronous or polling-only text mode.

## Request flow

1. The caller sends a chat completion or Responses request with a webhook URL.
2. The proxy saves the job and returns HTTP 200 with an ID and `status: "queued"`.
3. A worker sends the model request to OpenRouter with the caller's token.
4. The proxy sends the final result or error to the webhook. It retries failed delivery.

The caller can also retrieve the job's status. Polling does not replace webhook delivery.

## Run

Use Go 1.25 or later on Linux or macOS.

```sh
cp .env.example .env
openssl rand -base64 32
```

Save the generated value as `DATA_ENCRYPTION_KEY` in `.env`. Generate this key once. Keep it across restarts.

```sh
set -a
. ./.env
set +a
go run .
```

The service listens on port 8080 by default. It does not load `.env` on its own.

The server needs no shared OpenRouter key and no separate proxy API token. Each caller supplies an OpenRouter key. OpenRouter checks that key and charges its account. The OpenAI SDK can use an OpenRouter key with this proxy; an OpenAI API key does not grant OpenRouter access.

## Submit a request

```sh
curl -sS http://localhost:8080/api/v1/chat/completions \
  -H "Authorization: Bearer $OPENROUTER_API_KEY" \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: greeting-123' \
  -d '{
    "model": "openrouter/auto",
    "messages": [{"role": "user", "content": "Write a short greeting."}],
    "background": true,
    "webhook_url": "https://hooks.example.com/result"
  }'
```

The first response contains a queued chat completion. This example shows its main fields:

```json
{
  "id": "chatcmpl_proxy_9c39b1d0c78c45df803e652359b72b02",
  "object": "chat.completion",
  "model": "openrouter/auto",
  "status": "queued",
  "background": true,
  "choices": []
}
```

The response also includes the fields that the SDK requires. The `Location` header gives the retrieval path. Responses requests use `object: "response"`, an empty `output` array, and a `resp_proxy_` ID.

| Field | Behavior |
| --- | --- |
| `webhook_url` | Required. HTTPS on port 443, without URL credentials or a fragment. |
| `background` | Optional. Omit it or set it to `true`. `false` is rejected. |
| `stream` | Omit it or set it to `false`. `true` is rejected. |
| `webhook_secret` | Optional signing secret. Supply one or save the generated secret from the response header. |

The proxy removes these background and webhook options before it sends the model request to OpenRouter. The remaining model body goes to the matching upstream endpoint. There is no separate job request wrapper.

Without `webhook_url` or its header equivalent, the proxy returns:

```json
{
  "error": {
    "message": "webhook_url is required for every text request",
    "type": "proxy_error",
    "code": 400
  }
}
```

URL checks at submission cover format, port, and the optional host list. DNS and public address checks run when the proxy connects for delivery. A supplied URL can therefore pass submission and later fail delivery.

## OpenAI client

This JavaScript example uses the official `openai` package with an OpenRouter key.

```js
import OpenAI from 'openai';

const client = new OpenAI({
  apiKey: process.env.OPENROUTER_API_KEY,
  baseURL: 'https://your-proxy.example/api/v1',
});

const queued = await client.responses.create({
  model: 'openrouter/auto',
  input: 'Write a short greeting.',
  background: true,
  webhook_url: 'https://hooks.example.com/result',
  webhook_secret: process.env.MY_WEBHOOK_SECRET,
});

console.log(queued.id, queued.status);
```

The same options work with `client.chat.completions.create()` and a normal `messages` array. These webhook fields are proxy extensions. In typed TypeScript, pass them through the SDK's request `headers` option as `X-Webhook-URL` and `X-Webhook-Secret`.

## OpenRouter client

The official `@openrouter/sdk` package removes unknown body fields. Pass the webhook URL through its supported header options.

```js
import { OpenRouter } from '@openrouter/sdk';

const client = new OpenRouter({
  apiKey: process.env.OPENROUTER_API_KEY,
  serverURL: 'https://your-proxy.example/api/v1',
});

const queued = await client.chat.send(
  {
    chatRequest: {
      model: 'openrouter/auto',
      messages: [{ role: 'user', content: 'Write a short greeting.' }],
    },
  },
  {
    fetchOptions: {
      headers: {
        'X-Webhook-URL': 'https://hooks.example.com/result',
        'X-Webhook-Secret': process.env.MY_WEBHOOK_SECRET,
      },
    },
  },
);

console.log(queued.id);
```

For Responses, use `client.beta.responses.send({ responsesRequest: { model, input } }, options)` with the same header options. The proxy accepts `X-Background: true`, but it is optional. Text calls always run asynchronously.

A nonempty webhook header overrides its matching JSON field. The proxy removes these headers before it calls OpenRouter. The OpenRouter SDK removes unknown response fields too, so use the retrieval endpoint to inspect the added chat completion `status` field.

## Receive and verify the webhook

The webhook receives an event with the full final result in `data`. A chat completion event looks like this; the result here is shortened:

```json
{
  "object": "event",
  "id": "evt_7df1da47aebc47f5bdaab9c4637c6b72",
  "type": "chat.completion.completed",
  "created_at": 1794000000,
  "status_code": 200,
  "data": {
    "id": "chatcmpl_proxy_9c39b1d0c78c45df803e652359b72b02",
    "upstream_id": "gen-example",
    "object": "chat.completion",
    "status": "completed",
    "background": true,
    "choices": [{"index": 0, "message": {"role": "assistant", "content": "Hello!"}, "finish_reason": "stop"}]
  }
}
```

The result keeps the proxy job ID. `upstream_id` holds OpenRouter's original ID when available. Model output, usage, and upstream error details remain in the result. `status_code` records the upstream HTTP status when available; transport failures may have none.

| Final status | Responses event | Chat completion event |
| --- | --- | --- |
| `completed` | `response.completed` | `chat.completion.completed` |
| `failed` | `response.failed` | `chat.completion.failed` |
| `incomplete` | `response.incomplete` | `chat.completion.incomplete` |
| `cancelled` | `response.cancelled` | `chat.completion.cancelled` |

A model timeout produces `failed`. A process stop during a model call produces `incomplete`. The proxy can preserve a provider's `cancelled` result, but it has no caller cancellation operation.

Use a signing secret with at least 32 random bytes encoded as base64. The usual format is `whsec_` followed by that value. A bare base64 value also works. If you omit `webhook_secret`, save the generated value from the initial response's `X-Webhook-Secret` header.

The official OpenAI SDK verifies the webhook signature:

```js
const event = await client.webhooks.unwrap(
  rawRequestBody,
  requestHeaders,
  process.env.MY_WEBHOOK_SECRET,
);
```

Pass the exact raw body before JSON parsing. The proxy uses Standard Webhooks headers: `webhook-id`, `webhook-timestamp`, and `webhook-signature`. It signs `id.timestamp.body` with HMAC-SHA256 and sends `v1,<base64 digest>`. The SDK checks the signature and timestamp age.

Save the event, then return a 2xx status. Deduplicate by `webhook-id`. The same event can arrive more than once if an HTTP response is lost or a process stops during delivery.

## Status and delivery retries

Use the same caller token to retrieve a job. Both base URL prefixes work.

| Method | Path under `/api/v1` or `/v1` | Purpose |
| --- | --- | --- |
| `POST` | `/chat/completions` | Queue a chat completion with a required webhook. |
| `POST` | `/responses` | Queue a Responses request with a required webhook. |
| `GET` | `/chat/completions/{id}` | Retrieve a chat completion job. |
| `GET` | `/responses/{id}` | Retrieve a Responses job. |
| `POST` | `/chat/completions/{id}/redeliver` | Retry an exhausted webhook delivery. |
| `POST` | `/responses/{id}/redeliver` | Retry an exhausted webhook delivery. |

The OpenAI SDK supports `client.responses.retrieve(id)` and `client.chat.completions.retrieve(id)`. Status moves from `queued` to `in_progress`, then to a final status. Another caller's token receives HTTP 404.

The retrieval response's `X-Webhook-State` header reports delivery state: `queued`, `running`, `delivering`, `delivered`, or `dead`. Model completion and webhook delivery are separate. A completed result can still await delivery.

The proxy retries network failures and every non-2xx webhook response. It uses exponential delays with jitter and does not follow redirects. After `WEBHOOK_ATTEMPTS` attempts, delivery becomes `dead`. Call the matching `/redeliver` endpoint to reset its delivery budget. Redelivery keeps the event ID and does not call the model again.

Model calls do not retry automatically. An upstream error, including an invalid key or rate limit, becomes a failed result and webhook event. The queued acceptance response does not prove that OpenRouter accepted the token or model request.

Use `Idempotency-Key` to retry submission without creating another job. Within one caller token, the same endpoint, model body, webhook URL, and supplied signing secret return the existing job. Changed values with the same key return HTTP 409. Keep the same key and request on retries. This protection ends when the job expires.

## OpenAI background compatibility

The supported workflow follows OpenAI's [background requests](https://developers.openai.com/api/docs/guides/background) and [signed webhooks](https://developers.openai.com/api/docs/guides/webhooks): queued responses, status retrieval, terminal events, and OpenAI SDK signature verification.

This proxy requires a per-request webhook URL and includes the final result in the event. OpenAI configures webhook destinations separately. Chat completion background jobs and `chat.completion.*` events are proxy extensions.

The current implementation does not support background streaming, stream resume, caller cancellation, or OpenAI's webhook registration API. It uses its own retention setting. These limits mean it does not implement every OpenAI background API operation.

## Configuration

| Variable | Default | Purpose |
| --- | --- | --- |
| `DATA_ENCRYPTION_KEY` | Required | Base64-encoded 32-byte key for queued caller credentials. |
| `ADDR` | `:8080` | HTTP listen address. |
| `DATA_DIR` | `./data` | Durable job directory. |
| `WEBHOOK_HOSTS` | Empty | Optional comma-separated exact host list. Empty allows any public HTTPS host. |
| `MODEL_CONCURRENCY` | `300` | Shared concurrency limit for model calls and read-only upstream requests. |
| `WEBHOOK_CONCURRENCY` | `50` | Maximum concurrent delivery attempts. |
| `MODEL_TIMEOUT` | `10m` | Timeout for each upstream HTTP call. Queue wait is separate for text jobs. |
| `WEBHOOK_TIMEOUT` | `15s` | Timeout for each delivery attempt. |
| `WEBHOOK_ATTEMPTS` | `12` | Delivery attempt budget. |
| `MAX_JOBS` | `10000` | Maximum stored jobs, including finished jobs. |
| `RETENTION` | `24h` | Retention after delivery succeeds or attempts run out. |

Text request bodies have a 1 MiB limit. Model responses have a 16 MiB limit. A full store returns HTTP 503 with `Retry-After: 60`. Pending jobs do not expire. Terminal job cleanup also removes its result and idempotency record.

Read-only upstream routes pass through with the caller's token. Other submission routes return HTTP 404. `GET /healthz` needs no token. It returns HTTP 503 after a storage failure; the proxy then stops text admission and scheduling until restart.

The proxy requires public HTTPS webhook destinations on port 443. It checks DNS addresses when it connects and blocks private and reserved networks. It does not use environment HTTP proxies.

## Docker and Hatchbox deployment

The app runs in Docker. `compose.yaml` builds the Go image, starts one non-root container, and stores jobs in the persistent `openrouter-webhooks-data` volume.

With a valid encryption key in your private `.env`, run:

```sh
docker compose up --build --detach --wait
curl --fail http://127.0.0.1:8080/healthz
docker compose down --timeout 60
```

The server binds to `127.0.0.1:8080` by default. On Hatchbox, Compose uses the app's assigned `PORT` for that host binding. The container always listens on port 8080.

Follow [the Hatchbox setup instructions](docs/hatchbox.md) for [app 14630](https://hatchbox.io/apps/14630-openrouter-webhooks). The repo includes `.hatchbox/build`. The Hatchbox web process runs `docker compose up --no-build --remove-orphans` without detached mode.

Put the public app behind Hatchbox's HTTPS proxy. Verify the deployed commit with:

```sh
bin/check-deploy https://YOUR-APP.hatchboxapp.com FULL_COMMIT_SHA
```

`/healthz` returns `status` and the image's compiled `revision`. Local builds use `development` unless you set a build revision. CI builds and checks the Docker image with the commit SHA.

Keep the same encryption key and persistent local volume across releases. Run one process on one application server. This queue does not coordinate replicas. Never delete its volume during redeployment.

Queued jobs and webhook deliveries resume after restart. A model call that was running becomes `incomplete`; the proxy does not repeat it because the upstream outcome may be unknown. It refuses to start if it cannot decrypt queued caller tokens.

Caller tokens use application encryption on disk. The service clears the encrypted credential from the job record after the model call ends. Prompts, outputs, webhook URLs, signing secrets, and token hashes also live in the job directory; keep storage and backups private. Job records stay in memory too. `MAX_JOBS` limits record count, not total bytes.

The queue uses format version 2. It cannot load version 1 records from the original shared-token prototype.

## Verify

Use Node.js 22 or later for the SDK tests. The running service needs Go only.

```sh
npm ci --ignore-scripts --legacy-peer-deps
go vet ./...
go test -race -count=1 -timeout=120s -v ./...
go build ./...
```

Tests use the pinned official `openai` and `@openrouter/sdk` packages. They cover required webhook rejection, asynchronous chat and Responses calls, token isolation, status retrieval, errors, and signature verification. Queue tests also cover delivery retries, restart recovery, encrypted credentials, and 300 simultaneous model calls.

The tests use local providers and HTTP/TLS connections in memory. They make no paid OpenRouter or OpenAI calls. The concurrency test proves local proxy behavior; production capacity also depends on the server and OpenRouter account limits.

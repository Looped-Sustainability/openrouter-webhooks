import assert from 'node:assert/strict';
import { createInterface } from 'node:readline';
import OpenAI from 'openai';
import { OpenRouter, HTTPClient } from '@openrouter/sdk';

// Only the transport crosses this pipe. Both SDKs use their real serializers,
// response parsers, error handling, and webhook verification.
const input = createInterface({ input: process.stdin });
const pending = new Map();
let nextID = 0;
input.on('line', (line) => {
  const message = JSON.parse(line);
  const callback = pending.get(message.id);
  pending.delete(message.id);
  callback(message);
});
function rpc(message) {
  const id = ++nextID;
  return new Promise((resolve) => {
    pending.set(id, resolve);
    process.stdout.write(`${JSON.stringify({ ...message, id })}\n`);
  });
}
async function transportFetch(input, init) {
  const request = new Request(input, init);
  const result = await rpc({
    operation: 'request', method: request.method, url: request.url,
    headers: Object.fromEntries(request.headers), body: await request.text(),
  });
  return new Response(result.body, { status: result.status, headers: result.headers });
}
const secret = `whsec_${Buffer.alloc(32, 9).toString('base64')}`;
const openai = new OpenAI({ apiKey: 'caller-a', baseURL: 'https://proxy.test/api/v1', fetch: transportFetch, maxRetries: 0 });
const router = new OpenRouter({
  apiKey: 'caller-b', serverURL: 'https://proxy.test/api/v1',
  httpClient: new HTTPClient({ fetcher: transportFetch }),
  retryConfig: { strategy: 'none' },
});
const routerPolling = new OpenAI({ apiKey: 'caller-b', baseURL: 'https://proxy.test/v1', fetch: transportFetch, maxRetries: 0 });
const messages = [{ role: 'user', content: 'hello' }];
const model = 'test/model';
async function poll(client, id) {
  const responses = id.startsWith('resp_');
  for (let i = 0; i < 150; i++) {
    const response = responses ? await client.responses.retrieve(id) : await client.chat.completions.retrieve(id);
    if (!['queued', 'in_progress'].includes(response.status)) return response;
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
  throw new Error('background request did not finish');
}
async function verifyWebhook(client, id, type) {
  const callback = await rpc({ operation: 'webhook' });
  const event = await client.webhooks.unwrap(callback.body, callback.headers, secret);
  assert.equal(event.object, 'event');
  assert.equal(event.type, type);
  assert.equal(event.data.id, id);
  assert.equal(event.data.status, type.split('.').at(-1));
  await assert.rejects(client.webhooks.unwrap(`${callback.body} `, callback.headers, secret));
  await assert.rejects(client.webhooks.unwrap(callback.body, callback.headers, `whsec_${Buffer.alloc(32, 8).toString('base64')}`));
}
try {
  // Neither client can bypass the webhook requirement with a normal text call.
  await assert.rejects(openai.chat.completions.create({ model, messages }), (error) => error.status === 400);
  await assert.rejects(openai.responses.create({ model, input: 'hello' }), (error) => error.status === 400);
  await assert.rejects(router.chat.send({ chatRequest: { model, messages } }));
  await assert.rejects(router.beta.responses.send({ responsesRequest: { model, input: 'hello' } }));
  await assert.rejects(openai.chat.completions.create({ model, messages, stream: true, webhook_url: 'https://example.com/result' }), (error) => error.status === 400);
  await assert.rejects(router.chat.send({ chatRequest: { model, messages, stream: true } }, { fetchOptions: { headers: { 'X-Webhook-URL': 'https://example.com/result' } } }));

  const queued = await openai.responses.create({
    model, input: 'hello',
    webhook_url: 'https://example.com/result', webhook_secret: secret,
  });
  assert.equal(queued.status, 'queued');
  assert.ok(queued.id.startsWith('resp_proxy_'));
  const finished = await poll(openai, queued.id);
  assert.equal(finished.status, 'completed');
  assert.equal(finished.id, queued.id);
  assert.equal(finished.output_text, 'Hello');
  assert.equal(finished.upstream_id, 'upstream-response');
  await verifyWebhook(openai, queued.id, 'response.completed');
  await assert.rejects(routerPolling.responses.retrieve(queued.id), (error) => error.status === 404);

  const headers = { 'X-Webhook-URL': 'https://example.com/result', 'X-Webhook-Secret': secret };
  const routerQueued = await router.beta.responses.send(
    { responsesRequest: { model, input: 'hello' } }, { fetchOptions: { headers } },
  );
  assert.equal(routerQueued.status, 'queued');
  assert.ok(routerQueued.id.startsWith('resp_proxy_'));
  const routerFinished = await poll(routerPolling, routerQueued.id);
  assert.equal(routerFinished.output_text, 'Hello');
  await verifyWebhook(routerPolling, routerQueued.id, 'response.completed');

  const chatQueued = await router.chat.send(
    { chatRequest: { model, messages } }, { fetchOptions: { headers } },
  );
  assert.ok(chatQueued.id.startsWith('chatcmpl_proxy_'));
  const chatFinished = await poll(routerPolling, chatQueued.id);
  assert.equal(chatFinished.choices[0].message.content, 'Hello');
  await verifyWebhook(routerPolling, chatQueued.id, 'chat.completion.completed');

  const chatA = await openai.chat.completions.create({ model, messages, background: true, webhook_url: 'https://example.com/result', webhook_secret: secret });
  assert.equal(chatA.status, 'queued');
  assert.equal((await poll(openai, chatA.id)).choices[0].message.content, 'Hello');
  await verifyWebhook(openai, chatA.id, 'chat.completion.completed');

  const invalid = new OpenAI({ apiKey: 'invalid', baseURL: 'https://proxy.test/v1', fetch: transportFetch, maxRetries: 0 });
  await assert.rejects(invalid.chat.completions.create({ model, messages }), (error) => error.status === 400);
  const routerInvalid = new OpenRouter({ apiKey: 'invalid', serverURL: 'https://proxy.test/v1', httpClient: new HTTPClient({ fetcher: transportFetch }), retryConfig: { strategy: 'none' } });
  await assert.rejects(routerInvalid.chat.send({ chatRequest: { model, messages } }));
  const failed = await invalid.responses.create({ model, input: 'hello', background: true, webhook_url: 'https://example.com/result', webhook_secret: secret });
  const failedResult = await poll(invalid, failed.id);
  assert.equal(failedResult.status, 'failed');
  assert.equal(failedResult.error.message, 'Invalid upstream key');
  await verifyWebhook(invalid, failed.id, 'response.failed');
  console.error('Both SDKs passed: mandatory webhooks, asynchronous chat and Responses, polling, token isolation, upstream errors, and signed webhook delivery.');
} catch (error) {
  console.error(error);
  process.exitCode = 1;
} finally {
  input.close();
}

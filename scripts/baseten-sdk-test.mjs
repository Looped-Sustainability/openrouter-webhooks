import assert from 'node:assert/strict';
import { createInterface } from 'node:readline';
import OpenAI from 'openai';
import Anthropic from '@anthropic-ai/sdk';

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
const openai = new OpenAI({ apiKey: 'caller-a', baseURL: 'https://proxy.test/baseten/v1', fetch: transportFetch, maxRetries: 0 });
const anthropic = new Anthropic({
  apiKey: 'caller-c',
  baseURL: 'https://proxy.test/baseten',
  defaultHeaders: { Authorization: 'Bearer caller-c' },
  fetch: transportFetch,
  maxRetries: 0,
});
const messages = [{ role: 'user', content: 'hello' }];
const model = 'test/model';
async function verifyWebhook(client, id, type) {
  const callback = await rpc({ operation: 'webhook' });
  const event = await client.webhooks.unwrap(callback.body, callback.headers, secret);
  assert.equal(event.object, 'event');
  assert.equal(event.type, type);
  assert.equal(event.data.id, id);
  assert.equal(event.data.status, type.split('.').at(-1));
  await assert.rejects(client.webhooks.unwrap(`${callback.body} `, callback.headers, secret));
  return event;
}
try {
  await assert.rejects(openai.chat.completions.create({ model, messages }), (error) => error.status === 400);
  await assert.rejects(openai.responses.create({ model, input: 'hello' }), (error) => error.status === 400);

  const chatQueued = await openai.chat.completions.create({
    model, messages, webhook_url: 'https://example.com/result', webhook_secret: secret,
  });
  assert.equal(chatQueued.status, 'queued');
  assert.ok(chatQueued.id.startsWith('chatcmpl_proxy_'));
  await verifyWebhook(openai, chatQueued.id, 'chat.completion.completed');

  const responseQueued = await openai.responses.create({
    model, input: 'hello', webhook_url: 'https://example.com/result', webhook_secret: secret,
  });
  assert.equal(responseQueued.status, 'queued');
  assert.ok(responseQueued.id.startsWith('resp_proxy_'));
  await verifyWebhook(openai, responseQueued.id, 'response.completed');

  await assert.rejects(anthropic.messages.create({ model, max_tokens: 16, messages }), (error) => error.status === 400);
  const messageQueued = await anthropic.messages.create(
    { model, max_tokens: 16, messages },
    { headers: { 'X-Webhook-URL': 'https://example.com/result', 'X-Webhook-Secret': secret } },
  );
  assert.ok(messageQueued.id.startsWith('msg_proxy_'));
  assert.deepEqual(messageQueued.content, []);
  const raw = await transportFetch(`https://proxy.test/baseten/v1/messages/${messageQueued.id}`, {
    headers: { Authorization: 'Bearer caller-c' },
  });
  const rawBody = JSON.parse(await raw.text());
  assert.ok(rawBody.status === 'queued' || rawBody.status === 'in_progress' || rawBody.status === 'completed');
  const event = await verifyWebhook(openai, messageQueued.id, 'message.completed');
  assert.equal(event.data.content.find((block) => block.type === 'text').text, 'Hello');
  console.error('Baseten SDKs passed: mandatory webhooks, OpenAI chat and Responses, Anthropic Messages, and signed webhook delivery.');
} catch (error) {
  console.error(error);
  process.exitCode = 1;
} finally {
  input.close();
}

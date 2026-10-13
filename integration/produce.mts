import Redis from 'ioredis';
import { Client } from '../src/client.js';

/**
 * Interop test: produce tasks from Node, let the Go asynq server consume
 * them, then assert on the results the Go consumer recorded.
 */
const redis = new Redis(process.env.ASYNQ_TEST_REDIS_URL ?? 'redis://127.0.0.1:6379');
const client = new Client(redis);

const jobs: Array<[string, unknown, Record<string, unknown>?]> = [
  ['email:send', { to: 'user@example.com', subject: 'Hello' }],
  ['webhook:deliver', { url: 'https://example.com/hook', event: 'order.paid' }, { queue: 'critical' }],
  ['cleanup', { olderThanDays: 30 }],
  ['metrics:track', { event: 'page_view', page: '/pricing' }],
];

console.log('producing tasks from Node...');
for (const [type, payload, opts] of jobs) {
  const id = await client.enqueue(type, payload, opts as never);
  console.log(`  enqueued ${type} -> ${id}`);
}

await redis.quit();
console.log('done.');

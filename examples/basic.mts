import Redis from 'ioredis';
import { Client, ErrDuplicateTask } from '../src/client.js';

/**
 * Minimal example: enqueue a few tasks with different options.
 * Run with:  ./node_modules/.bin/tsx examples/basic.mts
 */
const redis = new Redis(process.env.REDIS_URL ?? 'redis://127.0.0.1:6379');
const client = new Client(redis);

// Immediate task on the default queue
const id1 = await client.enqueue('email:send', { to: 'user@example.com' });
console.log('enqueued immediate:', id1);

// Scheduled one hour from now
const id2 = await client.enqueue('report:generate', { userId: 42 }, { processIn: 3600 });
console.log('enqueued scheduled:', id2);

// Unique for 5 minutes
try {
  await client.enqueue('sync:account', { accountId: 'acct_1' }, { uniqueTTL: 300 });
  await client.enqueue('sync:account', { accountId: 'acct_1' }, { uniqueTTL: 300 });
} catch (err) {
  if (err instanceof ErrDuplicateTask) {
    console.log('duplicate rejected as expected');
  }
}

await redis.quit();

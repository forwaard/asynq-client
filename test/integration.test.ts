import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import Redis from 'ioredis';
import { Client, ErrDuplicateTask, ErrTaskIdConflict } from '../src/client.js';
import { decodeMessage } from '../src/message.js';
import { pendingKey, scheduledKey, taskKey } from '../src/keys.js';

const REDIS_URL = process.env.ASYNQ_TEST_REDIS_URL ?? 'redis://127.0.0.1:6379';

describe('Client (integration with real Redis)', () => {
  let redis: Redis;
  let client: Client;

  beforeAll(async () => {
    redis = new Redis(REDIS_URL, { lazyConnect: true });
    await redis.connect();
    client = new Client(redis);
  });

  afterAll(async () => {
    await redis.flushdb();
    redis.disconnect();
  });

  it('enqueues an immediate task in asynq wire format', async () => {
    const id = await client.enqueue('email:send', { to: 'user@example.com' });

    // Task hash exists with pending state.
    const hash = await redis.hgetallBuffer(taskKey('default', id));
    expect(hash.state?.toString()).toBe('pending');
    expect(hash.pending_since).toBeTruthy();

    // Payload in msg field is decodable protobuf.
    const msg = decodeMessage(hash.msg as Buffer);
    expect(msg.type).toBe('email:send');
    expect(msg.id).toBe(id);
    expect(msg.queue).toBe('default');
    expect(msg.retry).toBe(25);
    expect(JSON.parse(msg.payload.toString())).toEqual({ to: 'user@example.com' });

    // Task ID is in the pending list.
    const pending = await redis.lrange(pendingKey('default'), 0, -1);
    expect(pending).toContain(id);

    // Queue is registered in the global set.
    expect(await redis.sismember('asynq:queues', 'default')).toBe(1);
  });

  it('rejects duplicate task IDs', async () => {
    const id = await client.enqueue('a', {}, { taskId: 'fixed-id' });
    expect(id).toBe('fixed-id');
    await expect(client.enqueue('b', {}, { taskId: 'fixed-id' })).rejects.toThrow(
      ErrTaskIdConflict,
    );
  });

  it('enforces uniqueness within TTL', async () => {
    await client.enqueue('sync:data', { source: 'api' }, { uniqueTTL: 60 });
    await expect(
      client.enqueue('sync:data', { source: 'api' }, { uniqueTTL: 60 }),
    ).rejects.toThrow(ErrDuplicateTask);
    // Different payload => different unique key => allowed.
    await client.enqueue('sync:data', { source: 'webhook' }, { uniqueTTL: 60 });
  });

  it('schedules a task into the scheduled zset', async () => {
    const at = new Date(Date.now() + 3600_000);
    const id = await client.enqueue('report:generate', { userId: 1 }, { processAt: at });

    const hash = await redis.hgetallBuffer(taskKey('default', id));
    expect(hash.state?.toString()).toBe('scheduled');

    const score = await redis.zscore(scheduledKey('default'), id);
    expect(Number(score)).toBe(Math.floor(at.getTime() / 1000));

    const msg = decodeMessage(hash.msg as Buffer);
    expect(msg.type).toBe('report:generate');
  });

  it('supports processIn delay option', async () => {
    const before = Math.floor(Date.now() / 1000);
    const id = await client.enqueue('cleanup', {}, { processIn: 300 });
    const score = Number(await redis.zscore(scheduledKey('default'), id));
    expect(score).toBeGreaterThanOrEqual(before + 299);
    expect(score).toBeLessThanOrEqual(before + 301);
  });

  it('writes maxRetry/timeout/deadline/retention/headers into the message', async () => {
    const deadline = new Date(Date.now() + 7200_000);
    const id = await client.enqueue(
      'image:resize',
      { path: '/tmp/a.jpg' },
      {
        maxRetry: 3,
        timeout: 30,
        deadline,
        retention: 600,
        headers: { 'x-request-id': 'r1' },
      },
    );
    const hash = await redis.hgetallBuffer(taskKey('default', id));
    const msg = decodeMessage(hash.msg as Buffer);
    expect(msg.retry).toBe(3);
    expect(msg.timeout).toBe(30);
    expect(msg.deadline).toBe(Math.floor(deadline.getTime() / 1000));
    expect(msg.retention).toBe(600);
    expect(msg.headers).toEqual({ 'x-request-id': 'r1' });
  });

  it('supports custom queues', async () => {
    const id = await client.enqueue('webhook:deliver', { url: 'https://x' }, { queue: 'critical' });
    const pending = await redis.lrange(pendingKey('critical'), 0, -1);
    expect(pending).toContain(id);
    expect(await redis.sismember('asynq:queues', 'critical')).toBe(1);
  });

  it('adds tasks to an aggregation group', async () => {
    const id = await client.enqueue('metrics:track', { event: 'click' }, { group: 'clicks' });
    const hash = await redis.hgetallBuffer(taskKey('default', id));
    expect(hash.state?.toString()).toBe('aggregating');
    expect(hash.group?.toString()).toBe('clicks');
    // Group key is a ZSET scored by enqueue time (matches Go's addToGroupCmd).
    expect(await redis.zscore('asynq:{default}:g:clicks', id)).not.toBeNull();
    expect(await redis.sismember('asynq:{default}:groups', 'clicks')).toBe(1);
  });
});

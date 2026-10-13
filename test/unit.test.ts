import { describe, expect, it } from 'vitest';
import { decodeMessage, encodeMessage } from '../src/message.js';
import {
  pendingKey,
  queueKeyPrefix,
  scheduledKey,
  taskKey,
  uniqueKey,
  validateQueueName,
} from '../src/keys.js';

describe('keys', () => {
  it('builds keys with asynq hash-tag layout', () => {
    expect(queueKeyPrefix('default')).toBe('asynq:{default}:');
    expect(taskKey('default', 'abc')).toBe('asynq:{default}:t:abc');
    expect(pendingKey('critical')).toBe('asynq:{critical}:pending');
    expect(scheduledKey('low')).toBe('asynq:{low}:scheduled');
  });

  it('builds unique key with md5 of payload', () => {
    const k = uniqueKey('default', 'email:send', Buffer.from('{"to":"a@b.c"}'));
    expect(k).toMatch(/^asynq:\{default\}:unique:email:send:[0-9a-f]{32}$/);
  });

  it('rejects blank queue names', () => {
    expect(() => validateQueueName('')).toThrow();
    expect(() => validateQueueName('   ')).toThrow();
  });
});

describe('message codec', () => {
  it('round-trips a TaskMessage', () => {
    const msg = {
      type: 'email:send',
      payload: Buffer.from('{"to":"a@b.c"}'),
      id: 'task-123',
      queue: 'default',
      retry: 25,
      retried: 0,
      errorMsg: '',
      lastFailedAt: 0,
      timeout: 30,
      deadline: 0,
      uniqueKey: '',
      groupKey: '',
      retention: 3600,
      completedAt: 0,
      headers: { 'x-trace': 'abc' },
    };
    const encoded = encodeMessage(msg);
    const decoded = decodeMessage(encoded);
    expect(decoded.type).toBe(msg.type);
    expect(decoded.payload.toString()).toBe(msg.payload.toString());
    expect(decoded.id).toBe(msg.id);
    expect(decoded.queue).toBe(msg.queue);
    expect(decoded.retry).toBe(msg.retry);
    expect(decoded.timeout).toBe(msg.timeout);
    expect(decoded.retention).toBe(msg.retention);
    expect(decoded.headers).toEqual(msg.headers);
  });
});

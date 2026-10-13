import { createHash } from 'node:crypto';

/**
 * Redis key helpers, mirroring asynq's internal/base/base.go key layout.
 *
 * All keys use hash tags (`{queue}`) so that keys belonging to the same
 * queue land on the same Redis Cluster slot.
 */

/** Global set of all known queue names. */
export const ALL_QUEUES = 'asynq:queues';

/** Key prefix for all keys of a queue: `asynq:{<queue>}:` */
export function queueKeyPrefix(queue: string): string {
  return `asynq:{${queue}}:`;
}

/** Key of the hash storing a task message: `asynq:{<queue>}:t:<id>` */
export function taskKey(queue: string, id: string): string {
  return `${queueKeyPrefix(queue)}t:${id}`;
}

/** Key of the pending list: `asynq:{<queue>}:pending` */
export function pendingKey(queue: string): string {
  return `${queueKeyPrefix(queue)}pending`;
}

/** Key of the scheduled sorted set: `asynq:{<queue>}:scheduled` */
export function scheduledKey(queue: string): string {
  return `${queueKeyPrefix(queue)}scheduled`;
}

/** Key of the group set used for task aggregation: `asynq:{<queue>}:g:<group>` */
export function groupKey(queue: string, group: string): string {
  return `${queueKeyPrefix(queue)}g:${group}`;
}

/**
 * Key of the uniqueness lock for a task.
 *
 * Mirrors base.UniqueKey: md5(payload) hex digest appended to
 * `asynq:{<queue>}:unique:<type>:`.
 */
export function uniqueKey(queue: string, taskType: string, payload: Buffer): string {
  const digest = createHash('md5').update(payload).digest('hex');
  return `${queueKeyPrefix(queue)}unique:${taskType}:${digest}`;
}

/** Validate a queue name (must be non-blank), mirroring base.ValidateQueueName. */
export function validateQueueName(name: string): void {
  if (name.trim().length === 0) {
    throw new Error('queue name must contain one or more characters');
  }
}

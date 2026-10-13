import { randomUUID } from 'node:crypto';
import type { Redis } from 'ioredis';
import {
  ADD_TO_GROUP_SCRIPT,
  ADD_TO_GROUP_UNIQUE_SCRIPT,
  ENQUEUE_SCRIPT,
  ENQUEUE_UNIQUE_SCRIPT,
  SCHEDULE_SCRIPT,
  SCHEDULE_UNIQUE_SCRIPT,
} from './scripts.js';
import { encodeMessage, type TaskMessage } from './message.js';
import {
  ALL_QUEUES,
  groupKey as buildGroupKey,
  pendingKey,
  scheduledKey,
  taskKey,
  uniqueKey as buildUniqueKey,
  validateQueueName,
} from './keys.js';

/** Options accepted by {@link Client.enqueue}. */
export interface EnqueueOptions {
  /** Queue name. Defaults to `"default"`. */
  queue?: string;
  /** Max retry count. Negative is treated as zero. Defaults to 25 (asynq default). */
  maxRetry?: number;
  /** Custom task ID. Defaults to a random UUID. */
  taskId?: string;
  /** Task timeout in seconds. Zero means no limit. */
  timeout?: number;
  /** Absolute deadline for the task. */
  deadline?: Date;
  /**
   * Uniqueness lock TTL in seconds. When set, a second task with the same
   * type+payload within the TTL is rejected with {@link ErrDuplicateTask}.
   */
  uniqueTTL?: number;
  /** Absolute time at which the task should be processed. */
  processAt?: Date;
  /** Delay in seconds after which the task should be processed. */
  processIn?: number;
  /** Seconds to keep the task after successful completion. Zero deletes it. */
  retention?: number;
  /**
   * Group key for task aggregation. When set, the task goes into the
   * aggregation group instead of the pending list (requires the Go server
   * to be configured with a GroupAggregator).
   */
  group?: string;
  /** Extra headers attached to the task message. */
  headers?: Record<string, string>;
}

/** Thrown when a task with the same ID already exists in Redis. */
export class ErrTaskIdConflict extends Error {
  constructor(message = 'task ID conflicts with another task') {
    super(message);
    this.name = 'ErrTaskIdConflict';
  }
}

/** Thrown when a task with the same uniqueness lock already exists. */
export class ErrDuplicateTask extends Error {
  constructor(message = 'task already exists (unique lock held)') {
    super(message);
    this.name = 'ErrDuplicateTask';
  }
}

/** Default max retry, matching asynq's client default. */
export const DEFAULT_MAX_RETRY = 25;
/** Default queue name, matching asynq's base.DefaultQueueName. */
export const DEFAULT_QUEUE = 'default';
/**
 * Default timeout in seconds applied when neither timeout nor deadline is
 * given, matching asynq's client-side defaultTimeout (30 minutes).
 */
export const DEFAULT_TIMEOUT_SECONDS = 30 * 60;

/**
 * Client is an insert-only asynq producer. It writes task messages to Redis
 * in the exact format the Go asynq server consumes, so a Go worker fleet can
 * process tasks enqueued from Node.js without any change.
 *
 * The client is safe for concurrent use.
 */
export class Client {
  private readonly redis: Redis;
  private readonly publishedQueues = new Set<string>();

  /**
   * @param redis - A connected ioredis client. The client does NOT take
   *   ownership: it never closes the connection.
   */
  constructor(redis: Redis) {
    this.redis = redis;
  }

  /**
   * Enqueue a task. Depending on options it lands in the pending list
   * (immediate), the scheduled set (future), or an aggregation group.
   *
   * @param taskType - Task type string, e.g. `"email:send"`. Must match the
   *   type registered on the Go side (`mux.Handle("email:send", ...)`).
   * @param payload - Payload object, JSON-encoded into `TaskMessage.payload`.
   * @param opts - Processing options.
   * @returns The task ID.
   */
  async enqueue(
    taskType: string,
    payload: unknown,
    opts: EnqueueOptions = {},
  ): Promise<string> {
    const queue = opts.queue ?? DEFAULT_QUEUE;
    validateQueueName(queue);

    const id = opts.taskId ?? randomUUID();
    const payloadBuf = Buffer.from(JSON.stringify(payload ?? null));

    const unique =
      opts.uniqueTTL !== undefined && opts.uniqueTTL > 0
        ? buildUniqueKey(queue, taskType, payloadBuf)
        : '';

    const deadlineSec = opts.deadline ? Math.floor(opts.deadline.getTime() / 1000) : 0;
    let timeoutSec = opts.timeout ?? 0;
    if (deadlineSec === 0 && timeoutSec === 0) {
      // Mirror the Go client: use the default timeout when neither is set.
      timeoutSec = DEFAULT_TIMEOUT_SECONDS;
    }

    const msg: TaskMessage = {
      type: taskType,
      payload: payloadBuf,
      id,
      queue,
      retry: Math.max(0, opts.maxRetry ?? DEFAULT_MAX_RETRY),
      retried: 0,
      errorMsg: '',
      lastFailedAt: 0,
      timeout: timeoutSec,
      deadline: deadlineSec,
      uniqueKey: unique,
      groupKey: opts.group ?? '',
      retention: opts.retention ?? 0,
      completedAt: 0,
      headers: opts.headers ?? {},
    };

    const encoded = encodeMessage(msg);

    await this.publishQueue(queue);

    const processAt = resolveProcessAt(opts);

    if (opts.group) {
      return this.enqueueToGroup(queue, id, encoded, opts, unique);
    }
    if (processAt) {
      return this.schedule(queue, id, encoded, processAt, opts, unique);
    }
    return this.enqueueNow(queue, id, encoded, opts, unique);
  }

  /** Enqueue for immediate processing. */
  private async enqueueNow(
    queue: string,
    id: string,
    encoded: Buffer,
    opts: EnqueueOptions,
    uniqueKey: string,
  ): Promise<string> {
    if (opts.uniqueTTL) {
      const result = (await this.redis.eval(
        ENQUEUE_UNIQUE_SCRIPT,
        3,
        uniqueKey,
        taskKey(queue, id),
        pendingKey(queue),
        id,
        opts.uniqueTTL,
        encoded,
        nowNano(),
      )) as number;
      this.checkResult(result);
      return id;
    }
    const result = (await this.redis.eval(
      ENQUEUE_SCRIPT,
      2,
      taskKey(queue, id),
      pendingKey(queue),
      encoded,
      id,
      nowNano(),
    )) as number;
    this.checkResult(result);
    return id;
  }

  /** Schedule for future processing. */
  private async schedule(
    queue: string,
    id: string,
    encoded: Buffer,
    processAt: Date,
    opts: EnqueueOptions,
    uniqueKey: string,
  ): Promise<string> {
    const atSec = Math.floor(processAt.getTime() / 1000);
    if (opts.uniqueTTL) {
      const result = (await this.redis.eval(
        SCHEDULE_UNIQUE_SCRIPT,
        3,
        uniqueKey,
        taskKey(queue, id),
        scheduledKey(queue),
        id,
        opts.uniqueTTL,
        atSec,
        encoded,
      )) as number;
      this.checkResult(result);
      return id;
    }
    const result = (await this.redis.eval(
      SCHEDULE_SCRIPT,
      2,
      taskKey(queue, id),
      scheduledKey(queue),
      encoded,
      atSec,
      id,
    )) as number;
    this.checkResult(result);
    return id;
  }

  /** Add to an aggregation group. */
  private async enqueueToGroup(
    queue: string,
    id: string,
    encoded: Buffer,
    opts: EnqueueOptions,
    uniqueKey: string,
  ): Promise<string> {
    const gkey = buildGroupKey(queue, opts.group!);
    const groupsSet = `asynq:{${queue}}:groups`;
    const nowSec = Math.floor(Date.now() / 1000);
    if (opts.uniqueTTL) {
      const result = (await this.redis.eval(
        ADD_TO_GROUP_UNIQUE_SCRIPT,
        4,
        taskKey(queue, id),
        gkey,
        groupsSet,
        uniqueKey,
        encoded,
        id,
        nowSec,
        opts.group!,
        opts.uniqueTTL,
      )) as number;
      this.checkResult(result);
      return id;
    }
    const result = (await this.redis.eval(
      ADD_TO_GROUP_SCRIPT,
      3,
      taskKey(queue, id),
      gkey,
      groupsSet,
      encoded,
      id,
      nowSec,
      opts.group!,
    )) as number;
    this.checkResult(result);
    return id;
  }

  /** Register the queue in the global queue set (once per process). */
  private async publishQueue(queue: string): Promise<void> {
    if (!this.publishedQueues.has(queue)) {
      await this.redis.sadd(ALL_QUEUES, queue);
      this.publishedQueues.add(queue);
    }
  }

  private checkResult(n: number): void {
    if (n === -1) throw new ErrDuplicateTask();
    if (n === 0) throw new ErrTaskIdConflict();
  }
}

function resolveProcessAt(opts: EnqueueOptions): Date | null {
  if (opts.processAt) return opts.processAt;
  if (opts.processIn !== undefined && opts.processIn > 0) {
    return new Date(Date.now() + opts.processIn * 1000);
  }
  return null;
}

function nowNano(): string {
  // UnixNano fits in a 64-bit int; ioredis passes numbers/strings to Lua.
  // Use BigInt to avoid float precision loss, then string for transport.
  return (BigInt(Date.now()) * 1_000_000n).toString();
}

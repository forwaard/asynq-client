import protobuf from 'protobufjs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

/**
 * Internal task message representation, mirroring asynq's base.TaskMessage.
 * Serialized with protobuf and stored in the `msg` field of the task hash.
 */
export interface TaskMessage {
  /** Task type, e.g. "email:send". */
  type: string;
  /** Raw payload bytes (usually JSON-encoded). */
  payload: Buffer;
  /** Unique task ID. */
  id: string;
  /** Queue name. */
  queue: string;
  /** Max number of retries. */
  retry: number;
  /** Number of times retried so far. */
  retried: number;
  /** Error message from the last failure. */
  errorMsg: string;
  /** Time of last failure (unix seconds). Zero means no last failure. */
  lastFailedAt: number;
  /** Timeout in seconds. Zero means no timeout. */
  timeout: number;
  /** Deadline (unix seconds). Zero means no deadline. */
  deadline: number;
  /** Redis key of the uniqueness lock, or empty. */
  uniqueKey: string;
  /** Group key for task aggregation, or empty. */
  groupKey: string;
  /** Seconds to retain the task after completion. Zero means delete on done. */
  retention: number;
  /** Completion time (unix seconds). Zero means not completed. */
  completedAt: number;
  /** Additional metadata headers. */
  headers: Record<string, string>;
}

const protoPath = join(dirname(fileURLToPath(import.meta.url)), '..', 'proto', 'asynq.proto');
const root = protobuf.loadSync(protoPath);
const PbTaskMessage = root.lookupType('asynq.TaskMessage');

/**
 * Encode a TaskMessage to protobuf bytes, compatible with Go's
 * base.EncodeMessage (proto.Marshal of pb.TaskMessage).
 */
export function encodeMessage(msg: TaskMessage): Buffer {
  const err = PbTaskMessage.verify(msg);
  if (err) {
    throw new Error(`cannot encode message: ${err}`);
  }
  const m = PbTaskMessage.create(msg);
  return Buffer.from(PbTaskMessage.encode(m).finish());
}

/**
 * Decode protobuf bytes to a TaskMessage. Used by tests and tooling to
 * verify wire compatibility with the Go implementation.
 */
export function decodeMessage(data: Buffer): TaskMessage {
  const m = PbTaskMessage.decode(data);
  const obj = PbTaskMessage.toObject(m, {
    longs: Number,
    bytes: Buffer,
    defaults: true,
  }) as Record<string, unknown>;
  return {
    type: obj.type as string,
    payload: obj.payload as Buffer,
    id: obj.id as string,
    queue: obj.queue as string,
    retry: obj.retry as number,
    retried: obj.retried as number,
    errorMsg: obj.errorMsg as string,
    lastFailedAt: obj.lastFailedAt as number,
    timeout: obj.timeout as number,
    deadline: obj.deadline as number,
    uniqueKey: obj.uniqueKey as string,
    groupKey: obj.groupKey as string,
    retention: obj.retention as number,
    completedAt: obj.completedAt as number,
    headers: (obj.headers as Record<string, string>) ?? {},
  };
}

/**
 * Lua scripts copied verbatim from asynq's internal/rdb/rdb.go.
 *
 * Keeping them byte-identical to the Go implementation is what guarantees
 * the Go consumer cannot tell a Node-produced task apart from a Go-produced
 * one. Do not "improve" formatting — byte equality matters for EVALSHA.
 */

/**
 * Enqueue a task for immediate processing.
 *
 * KEYS[1] -> asynq:{<qname>}:t:<task_id>
 * KEYS[2] -> asynq:{<qname>}:pending
 * ARGV[1] -> task message data (protobuf)
 * ARGV[2] -> task ID
 * ARGV[3] -> current unix time in nsec
 *
 * Returns 1 if enqueued, 0 if task ID already exists.
 */
export const ENQUEUE_SCRIPT = `if redis.call("EXISTS", KEYS[1]) == 1 then
	return 0
end
redis.call("HSET", KEYS[1],
           "msg", ARGV[1],
           "state", "pending",
           "pending_since", ARGV[3])
redis.call("LPUSH", KEYS[2], ARGV[2])
return 1
`;

/**
 * Enqueue a task if its uniqueness lock can be acquired.
 *
 * KEYS[1] -> unique key
 * KEYS[2] -> asynq:{<qname>}:t:<taskid>
 * KEYS[3] -> asynq:{<qname>}:pending
 * ARGV[1] -> task ID
 * ARGV[2] -> uniqueness lock TTL (seconds)
 * ARGV[3] -> task message data
 * ARGV[4] -> current unix time in nsec
 *
 * Returns 1 if enqueued, 0 if task ID conflicts, -1 if duplicate task.
 */
export const ENQUEUE_UNIQUE_SCRIPT = `local ok = redis.call("SET", KEYS[1], ARGV[1], "NX", "EX", ARGV[2])
if not ok then
  return -1
end
if redis.call("EXISTS", KEYS[2]) == 1 then
  return 0
end
redis.call("HSET", KEYS[2],
           "msg", ARGV[3],
           "state", "pending",
           "pending_since", ARGV[4],
           "unique_key", KEYS[1])
redis.call("LPUSH", KEYS[3], ARGV[1])
return 1
`;

/**
 * Schedule a task to be processed in the future.
 *
 * KEYS[1] -> asynq:{<qname>}:t:<task_id>
 * KEYS[2] -> asynq:{<qname>}:scheduled
 * ARGV[1] -> task message data
 * ARGV[2] -> process_at unix timestamp
 * ARGV[3] -> task ID
 *
 * Returns 1 if scheduled, 0 if task ID already exists.
 */
export const SCHEDULE_SCRIPT = `if redis.call("EXISTS", KEYS[1]) == 1 then
	return 0
end
redis.call("HSET", KEYS[1],
           "msg", ARGV[1],
           "state", "scheduled")
redis.call("ZADD", KEYS[2], ARGV[2], ARGV[3])
return 1
`;

/**
 * Schedule a unique task to be processed in the future.
 *
 * KEYS[1] -> unique key
 * KEYS[2] -> asynq:{<qname>}:t:<task_id>
 * KEYS[3] -> asynq:{<qname>}:scheduled
 * ARGV[1] -> task ID
 * ARGV[2] -> uniqueness lock TTL (seconds)
 * ARGV[3] -> process_at unix timestamp
 * ARGV[4] -> task message data
 *
 * Returns 1 if scheduled, 0 if task ID conflicts, -1 if duplicate task.
 */
export const SCHEDULE_UNIQUE_SCRIPT = `local ok = redis.call("SET", KEYS[1], ARGV[1], "NX", "EX", ARGV[2])
if not ok then
  return -1
end
if redis.call("EXISTS", KEYS[2]) == 1 then
  return 0
end
redis.call("HSET", KEYS[2],
           "msg", ARGV[4],
           "state", "scheduled",
           "unique_key", KEYS[1])
redis.call("ZADD", KEYS[3], ARGV[3], ARGV[1])
return 1
`;

/**
 * Enqueue a task into an aggregation group (mirrors addToGroupCmd in rdb.go).
 *
 * KEYS[1] -> asynq:{<qname>}:t:<task_id>
 * KEYS[2] -> asynq:{<qname>}:g:<group_key>
 * KEYS[3] -> asynq:{<qname>}:groups
 * ARGV[1] -> task message data
 * ARGV[2] -> task ID
 * ARGV[3] -> current time in unix seconds
 * ARGV[4] -> group key
 *
 * Returns 1 if enqueued, 0 if task ID already exists.
 */
export const ADD_TO_GROUP_SCRIPT = `if redis.call("EXISTS", KEYS[1]) == 1 then
	return 0
end
redis.call("HSET", KEYS[1],
           "msg", ARGV[1],
           "state", "aggregating",
	       "group", ARGV[4])
redis.call("ZADD", KEYS[2], ARGV[3], ARGV[2])
redis.call("SADD", KEYS[3], ARGV[4])
return 1
`;

/**
 * Enqueue a unique task into an aggregation group (mirrors addToGroupUniqueCmd).
 *
 * KEYS[1] -> asynq:{<qname>}:t:<task_id>
 * KEYS[2] -> asynq:{<qname>}:g:<group_key>
 * KEYS[3] -> asynq:{<qname>}:groups
 * KEYS[4] -> unique key
 * ARGV[1] -> task message data
 * ARGV[2] -> task ID
 * ARGV[3] -> current time in unix seconds
 * ARGV[4] -> group key
 * ARGV[5] -> uniqueness lock TTL (seconds)
 *
 * Returns 1 if enqueued, 0 if task ID exists, -1 if duplicate task.
 */
export const ADD_TO_GROUP_UNIQUE_SCRIPT = `local ok = redis.call("SET", KEYS[4], ARGV[2], "NX", "EX", ARGV[5])
if not ok then
  return -1
end
if redis.call("EXISTS", KEYS[1]) == 1 then
  return 0
end
redis.call("HSET", KEYS[1],
           "msg", ARGV[1],
           "state", "aggregating",
	       "group", ARGV[4])
redis.call("ZADD", KEYS[2], ARGV[3], ARGV[2])
redis.call("SADD", KEYS[3], ARGV[4])
return 1
`;

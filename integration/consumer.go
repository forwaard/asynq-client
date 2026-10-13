// Go consumer for the end-to-end interop test.
//
// It registers handlers for the task types produced by the Node producer,
// then writes each consumed payload to a result file so the test harness can
// verify that the Go side processed exactly what Node enqueued.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/hibiken/asynq"
)

func main() {
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "127.0.0.1:6379"
	}
	resultFile := os.Getenv("RESULT_FILE")
	if resultFile == "" {
		resultFile = "/tmp/asynq-interop-results.jsonl"
	}

	srv := asynq.NewServer(
		asynq.RedisClientOpt{Addr: redisAddr},
		asynq.Config{
			Concurrency: 4,
			Queues:      map[string]int{"default": 3, "critical": 6},
		},
	)

	mux := asynq.NewServeMux()

	handle := func(taskType string) func(context.Context, *asynq.Task) error {
		return func(ctx context.Context, t *asynq.Task) error {
			var payload map[string]any
			if err := json.Unmarshal(t.Payload(), &payload); err != nil {
				return fmt.Errorf("unmarshal payload: %w", err)
			}
			record := map[string]any{
				"type":      taskType,
				"payload":   payload,
				"queue":     "default",
				"timestamp": time.Now().Unix(),
			}
			// Read queue from context if available.
			if q, ok := asynq.GetQueueName(ctx); ok {
				record["queue"] = q
			}
			data, _ := json.Marshal(record)
			f, err := os.OpenFile(resultFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
			if err != nil {
				return err
			}
			defer f.Close()
			if _, err := f.Write(append(data, '\n')); err != nil {
				return err
			}
			log.Printf("processed %s: %s", taskType, data)
			return nil
		}
	}

	mux.HandleFunc("email:send", handle("email:send"))
	mux.HandleFunc("webhook:deliver", handle("webhook:deliver"))
	mux.HandleFunc("report:generate", handle("report:generate"))
	mux.HandleFunc("cleanup", handle("cleanup"))
	mux.HandleFunc("metrics:track", handle("metrics:track"))

	log.Printf("starting asynq server on %s", redisAddr)
	if err := srv.Run(mux); err != nil {
		log.Fatalf("server error: %v", err)
	}
}

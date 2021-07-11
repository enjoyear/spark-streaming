package main

import (
	"context"
	"fmt"
	"time"
)

const (
	key string = "key1"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	valueCtx := context.WithValue(ctx, key, "val1")

	go func() {
		error := work(valueCtx, "1")
		fmt.Printf("Error returned from worker 1 is: %s\n", error)
	}()

	go work(valueCtx, "2")
	go work(valueCtx, "3")

	time.Sleep(5 * time.Second)
	fmt.Println("Main thread cancelling routines...")
	// Send cancellation to the routines
	cancel()

	time.Sleep(500 * time.Millisecond)
	fmt.Println("Done")
}

func work(ctx context.Context, workerId string) error {
	for {
		select {
		case <-ctx.Done():
			fmt.Printf("Context value: %s. Routine %s exiting...\n", ctx.Value(key), workerId)
			return ctx.Err()
		default:
			fmt.Printf("Context value: %s. Route %s in progress...\n", ctx.Value(key), workerId)
			time.Sleep(500 * time.Millisecond)
		}
	}
}

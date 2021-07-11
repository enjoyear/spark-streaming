package main

import (
	"context"
	"fmt"
	"time"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())

	go work(ctx, "1")
	go work(ctx, "2")
	go work(ctx, "3")

	time.Sleep(5 * time.Second)
	fmt.Println("Main thread cancelling routines...")
	// Send cancellation to the routines
	cancel()

	time.Sleep(500 * time.Millisecond)
	fmt.Println("Done")
}

func work(ctx context.Context, workerId string) {
	for {
		select {
		case <-ctx.Done():
			fmt.Printf("Routine %s exiting...\n", workerId)
			return
		default:
			fmt.Printf("Route %s in progress...\n", workerId)
			time.Sleep(500 * time.Millisecond)
		}
	}
}

package main

import (
	"context"
	"fmt"
	"time"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Timeout is larger than processingTime
	go handle(ctx, 1*time.Second)

	select {
	case <-ctx.Done():
		fmt.Println("Got cancellation signal in main thread:", ctx.Err())
	}

	time.Sleep(100 * time.Millisecond)
	fmt.Println("Done")
}

func handle(ctx context.Context, processingTime time.Duration) {
	for {
		select {
		case <-ctx.Done():
			fmt.Println("Got cancellation signal in the routine: ", ctx.Err())
			return
		case <-time.After(processingTime):
			//This will be printed every processingTime
			fmt.Println("Process the request for", processingTime)
		}
	}
}

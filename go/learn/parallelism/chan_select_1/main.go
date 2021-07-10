package main

import (
	"fmt"
	"time"
)

func main() {
	stop := make(chan bool)

	go func() {
		for {
			select {
			case <-stop:
				fmt.Println("Routine received exit signal. Exiting...")
				return
			default:
				fmt.Println("Work in progress in the routine...")
				time.Sleep(500 * time.Millisecond)
			}
		}
	}()

	time.Sleep(5 * time.Second)
	fmt.Println("Ready to exit routine")
	stop <- true

	time.Sleep(500 * time.Millisecond)
	fmt.Println("Done")
}

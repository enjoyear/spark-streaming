package main

import (
	"fmt"
	"sync"
	"time"
)

// Use WaitGroup to let main thread wait for multiple goRoutines
func main() {
	var wg sync.WaitGroup

	wg.Add(2)
	go func() {
		time.Sleep(3 * time.Second)
		fmt.Println("first routine done")
		wg.Done()
	}()
	go func() {
		time.Sleep(1 * time.Second)
		fmt.Println("second routine done")
		wg.Done()
	}()

	wg.Wait()
	time.Sleep(500 * time.Millisecond)
	fmt.Println("All done")
}

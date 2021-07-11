package main

import (
	"fmt"
	"time"
)

func main() {
	//The current time will be written to the channel after 2 seconds.
	//Implementation is the same as NewTimer.
	//Reuse NewTimer if worry about efficiency because the underlying Timer is not collected by the garbage collector
	//until the timer fires.
	timeChan := time.After(2 * time.Second)
	fmt.Println(time.Now())

	select {
	case w := <-timeChan:
		fmt.Printf("Got written time: %s\n", w)
		//default:
		//	fmt.Println("In default")
	}

	//deadlock if timeChan is reused
	//1. <-timeChan
	//2.
	//select {
	//case w := <-timeChan:
	//	fmt.Printf("Got written2 time: %s\n", w)
	//	//default:
	//	//	fmt.Println("In default")
	//}

	fmt.Println("Done")
}

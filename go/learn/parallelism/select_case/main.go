package main

import (
	"fmt"
	"time"
)

func main() {
	intChan := make(chan int, 10)
	for i := 0; i < 10; i++ {
		intChan <- i
	}

	stringChan := make(chan string, 5)
	for i := 0; i < 5; i++ {
		stringChan <- fmt.Sprintf("String %d", i)
	}

	//avoid deadlock when not closing the channel
label:
	for {
		select {
		//First two cases will be executed in any order
		case v := <-intChan: //not blocking if channel is empty. go to the next case if it happens
			fmt.Printf("Read %d from intChan.\n", v)
			sleep()
		case v := <-stringChan:
			fmt.Printf("Read %s from stringChan.\n", v)
			sleep()
		default:
			fmt.Printf("Both channels are empty.\n")
			sleep()
			// "break" will be useless here. Normally we use "return" to exit current routine
			// break
			break label
		}
	}

	fmt.Println("The end")
}

func sleep() {
	time.Sleep(200 * time.Millisecond)
}

package main

import (
	"fmt"
	"time"
)

func main() {
	go sayHello()
	go shouldNotAffectOtherGoroutines()

	for i := 0; i < 5; i++ {
		time.Sleep(time.Second)
		fmt.Println("Main alive...")
	}
	fmt.Println("The end")
}

func sayHello() {
	for i := 0; i < 10; i++ {
		time.Sleep(100 * time.Millisecond)
		fmt.Println("Hello World")
	}
}

func shouldNotAffectOtherGoroutines() {
	/**
	Without recover, panic in this goroutine will affect other goroutines
	*/
	defer func() {
		if err := recover(); err != nil {
			fmt.Printf("Panic captured in shouldNotAffectOtherGoroutines: %s\n", err)
		}
	}()

	var myMap map[int]string
	// it will panic here because myMap is nil
	myMap[0] = "abc"
}

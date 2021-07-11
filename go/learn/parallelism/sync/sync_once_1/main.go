package main

import (
	"fmt"
	"sync"
)

func init() {
	fmt.Println("Init called at the package loading time")
}

func main() {
	for i := 0; i < 5; i++ {
		instance := GetInstance()
		fmt.Printf("instance %p\n", instance)
	}
}

type singleton struct {
}

var instance *singleton
var once sync.Once

func GetInstance() *singleton {
	// sync.Once guarantees that the code will be executed exactly once.
	// sync.Once is similar to init, however, init is executed at the package loading time
	once.Do(func() {
		fmt.Println("This code should run only once")
		instance = new(singleton)
	})
	return instance
}

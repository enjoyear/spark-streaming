package main

import (
	"fmt"
	"sync"
)

func main() {
	panicDo()
}

func panicDo() {
	once := &sync.Once{}
	defer func() {
		if err := recover(); err != nil {
			once.Do(func() {
				fmt.Println("run in recover")
			})
		}
	}()
	once.Do(func() {
		fmt.Println("panic i=0")
		panic("panic i=0")
	})

}

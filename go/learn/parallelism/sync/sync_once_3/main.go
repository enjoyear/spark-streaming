package main

import (
	"fmt"
	"sync"
)

func main() {
	nestedDo()
}

func nestedDo() {
	once := &sync.Once{}
	once.Do(func() {
		//Check the source code, and this will cause a deadlock at "o.m.Lock()" in o.doSlow()
		once.Do(func() {
			fmt.Println("test nestedDo")
		})
	})
}

package main

import "fmt"
import "sync"
import "time"

type s struct{
	lck sync.Mutex
}

type st struct {
	a int
	b int
}

func f(s *string) {
	fmt.Println(s)
}

func main() {
	a := s{}
	a.lck.Lock()
	fmt.Println("Hello, playground")
	a.lck.Unlock()


	timer := time.NewTimer(time.Duration(200 * time.Millisecond))

	<-timer.C
	fmt.Println("Hello, playground")

	timer.Reset(time.Duration(200 * time.Millisecond))
	<-timer.C
	fmt.Println("got second signal")

	f(nil)
}
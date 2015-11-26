package main

import (
	"fmt"
	"time"
)

func main() {
	nano := time.Now().UnixNano()
	fmt.Printf("%v | %v\n", nano/1000, int(nano/1000))
}

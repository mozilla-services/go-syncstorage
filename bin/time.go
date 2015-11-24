package main

import (
	"fmt"
	"time"
)

func main() {
	fmt.Printf("%f", float64(time.Now().UnixNano())/1000000)
}

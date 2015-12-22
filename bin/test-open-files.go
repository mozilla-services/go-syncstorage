package main

import (
	"fmt"
	"syscall"
)

func main() {

	var rLimit syscall.Rlimit

	err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rLimit)

	if err != nil {
		fmt.Println("Error Getting Rlimit ", err)
	}
	fmt.Println("Open File limit: ", rLimit.Cur)

}

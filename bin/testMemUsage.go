package main

import (
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"strconv"

	"github.com/mostlygeek/go-syncstorage/syncstorage"
)

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func RandStringBytesRmndr(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Int63()%int64(len(letterBytes))]
	}
	return string(b)
}

func GetMemAllocated() uint64 {
	mem := &runtime.MemStats{}
	runtime.ReadMemStats(mem)
	return mem.TotalAlloc
}

func main() {

	numDBs := 2500

	before := GetMemAllocated()

	dbList := make([]*syncstorage.DB, numDBs)

	for i := 0; i < numDBs; i++ {
		userId := "memtest_" + strconv.Itoa(i)
		db, err := syncstorage.NewDB("./db_work/"+userId+".db", userId)

		if err != nil {
			fmt.Println(i, "Error: ", err)
			os.Exit(1)
		}

		dbList[i] = db
	}

	after := GetMemAllocated()
	diff := after - before

	fmt.Println(numDBs, "DBs. about", (diff / uint64(numDBs)), "RAM bytes each")

}

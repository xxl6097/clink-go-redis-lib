package main

import (
	"fmt"
	"github.com/xxl6097/clink-go-redis-lib/redis"
)

func main() {
	fmt.Println("hello main..")
	addr := "10.6.14.8:28407"
	pawd := "het@redis"
	dabe := 6
	redis.Init(addr, pawd, dabe)
	redis.Redis.Get("clink:")
}

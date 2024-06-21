package redis

import (
	"context"
	_redis "github.com/redis/go-redis/v9"
	"github.com/xxl6097/go-glog/glog"
	"time"
)

var client *_redis.Client

func Init(addr, password string, database int) {
	glog.Debug("redis init...")
	opt := &_redis.Options{
		Addr:             addr,
		Password:         password,
		DB:               database,
		DisableIndentity: true,
	}
	client = _redis.NewClient(opt)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		res, err := client.Ping(ctx).Result()
		glog.Debug("client.Ping ", res)
		if err != nil {
			glog.Error("initRedis client.Ping err: ", err)
			time.Sleep(time.Second * 5)
		} else {
			glog.Info("redis connect sucess...")
			break
		}
	}
}

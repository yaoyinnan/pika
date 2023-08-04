package pika_integration

import (
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type TimeValue struct {
	time.Time
}

func (t *TimeValue) ScanRedis(s string) (err error) {
	t.Time, err = time.Parse(time.RFC3339Nano, s)
	return
}

const (
	masterIP   = "127.0.0.1"
	masterPort = "9221"
	slaveIP    = "127.0.0.1"
	slavePort  = "9231"
)

func pikaOptions1() *redis.Options {
	return &redis.Options{
		Addr:         fmt.Sprintf("%s:%s", masterIP, masterPort),
		DB:           0,
		DialTimeout:  10 * time.Second,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		MaxRetries:   -1,
		PoolSize:     10,
		PoolTimeout:  30 * time.Second,
	}
}

func pikaOptions2() *redis.Options {
	return &redis.Options{
		Addr:         fmt.Sprintf("%s:%s", slaveIP, slavePort),
		DB:           0,
		DialTimeout:  10 * time.Second,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		MaxRetries:   -1,
		PoolSize:     10,
		PoolTimeout:  30 * time.Second,
	}
}

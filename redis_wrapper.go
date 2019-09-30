package rmq

import (
	"log"
	"time"

	"github.com/go-redis/redis"
)

type RedisWrapper struct {
	rawClient *redis.Client
}

func (wrapper RedisWrapper) Set(key string, value string, expiration time.Duration) bool {
	return checkNoErr(wrapper.rawClient.Set(key, value, expiration).Err())
}

func (wrapper RedisWrapper) Del(key string) (affected int, ok bool) {
	n, err := wrapper.rawClient.Del(key).Result()
	ok = checkNoErr(err)
	if !ok {
		return 0, false
	}
	return int(n), ok
}

func (wrapper RedisWrapper) TTL(key string) (ttl time.Duration, ok bool) {
	ttl, err := wrapper.rawClient.TTL(key).Result()
	ok = checkNoErr(err)
	if !ok {
		return 0, false
	}
	return ttl, ok
}

func (wrapper RedisWrapper) LPush(key, value string) bool {
	return checkNoErr(wrapper.rawClient.LPush(key, value).Err())
}

func (wrapper RedisWrapper) LLen(key string) (affected int, ok bool) {
	n, err := wrapper.rawClient.LLen(key).Result()
	ok = checkNoErr(err)
	if !ok {
		return 0, false
	}
	return int(n), ok
}

func (wrapper RedisWrapper) LRem(key string, count int, value string) (affected int, ok bool) {
	n, err := wrapper.rawClient.LRem(key, int64(count), value).Result()
	return int(n), checkNoErr(err)
}

func (wrapper RedisWrapper) LTrim(key string, start, stop int) bool {
	return checkNoErr(wrapper.rawClient.LTrim(key, int64(start), int64(stop)).Err())
}

func (wrapper RedisWrapper) RPopLPush(source, destination string) (value string, ok bool) {
	value, err := wrapper.rawClient.RPopLPush(source, destination).Result()
	return value, checkNoErr(err)
}

func (wrapper RedisWrapper) SAdd(key, value string) bool {
	return checkNoErr(wrapper.rawClient.SAdd(key, value).Err())
}

func (wrapper RedisWrapper) SMembers(key string) []string {
	members, err := wrapper.rawClient.SMembers(key).Result()
	if ok := checkNoErr(err); !ok {
		return []string{}
	}
	return members
}

func (wrapper RedisWrapper) SRem(key, value string) (affected int, ok bool) {
	n, err := wrapper.rawClient.SRem(key, value).Result()
	ok = checkNoErr(err)
	if !ok {
		return 0, false
	}
	return int(n), ok
}

func (wrapper RedisWrapper) FlushDb() {
	wrapper.rawClient.FlushDb()
}

// checkNoErr returns true if there is no error, false if the result error is nil or if there's another error
func checkNoErr(err error) (ok bool) {
	switch err {
	case nil:
		return true
	case redis.Nil:
		return false
	default:
		log.Printf("rmq redis error is not nil %s", err)
		return false
	}
}

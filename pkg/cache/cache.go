package cache

import (
	"encoding/json"
	"time"

	"github.com/cozy/cozy-stack/pkg/config"
	"github.com/cozy/cozy-stack/pkg/logger"
	"github.com/go-redis/redis"
)

var cacheLog = logger.WithNamespace("cache")

// Cache is an interface for a structure susceptible of caching
type Cache interface {
	Get(string, interface{}) bool
	Set(string, interface{})
	Del(string)
}

type noCache struct{}

func (*noCache) Get(string, interface{}) bool { return false }
func (*noCache) Set(string, interface{})      {}
func (*noCache) Del(string)                   {}

type subRedisInterface interface {
	Get(string) *redis.StringCmd
	Set(string, interface{}, time.Duration) *redis.StatusCmd
	Del(...string) *redis.IntCmd
}

type jsonCache struct {
	namespace  string
	expiration time.Duration
	client     subRedisInterface
}

func (c *jsonCache) Get(d string, out interface{}) bool {
	bytes, err := c.client.Get(c.namespace + "-" + d).Bytes()
	if err != nil {
		return false
	}
	if err := json.Unmarshal(bytes, out); err != nil {
		cacheLog.Error("bad value in redis", err)
		return false
	}
	return true
}

func (c *jsonCache) Set(d string, i interface{}) {
	bytes, err := json.Marshal(i)
	if err != nil {
		cacheLog.Errorf("unable to marshal instance %q: %s", c.namespace+d, err)
		return
	}
	err = c.client.Set(c.namespace+"-"+d, bytes, c.expiration).Err()
	if err != nil {
		cacheLog.Errorf("unable to cache value %q: %s", c.namespace+d, err)
	}
}

func (c *jsonCache) Del(d string) {
	err := c.client.Del(c.namespace + "-" + d).Err()
	if err != nil {
		cacheLog.Error("unable to revoke cache", err)
	}
}

// Create creates a cache
func Create(namespace string, expiration time.Duration) Cache {
	cli := config.GetConfig().Cache.Client()
	if cli == nil {
		return &noCache{}
	}
	return &jsonCache{
		namespace:  namespace,
		expiration: expiration,
		client:     cli,
	}
}

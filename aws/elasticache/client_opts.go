package elasticache

import (
	"fmt"

	"github.com/redis/go-redis/v9"
)

type clientOpt func(*Client) error

// WithRedisClient sets the redis client.
func WithRedisClient(redisClient *redis.Client) clientOpt {
	return func(c *Client) error {
		if c.redisClient != nil {
			return fmt.Errorf("redis client already set")
		}
		if redisClient == nil {
			return fmt.Errorf("redis client cannot be nil")
		}
		c.redisClient = redisClient
		c.redisURI = []string{redisClient.Options().Addr}
		return nil
	}
}

// WithRedisClusterClient sets the redis cluster client.
func WithRedisClusterClient(redisClusterClient *redis.ClusterClient) clientOpt {
	return func(c *Client) error {
		if c.redisClient != nil {
			return fmt.Errorf("redis client already set")
		}
		if redisClusterClient == nil {
			return fmt.Errorf("redis cluster client cannot be nil")
		}
		c.redisClient = redisClusterClient
		c.redisURI = redisClusterClient.Options().Addrs
		c.clusterMode = true
		return nil
	}
}

// WithDefaultRedisClient sets the default redis client.
func WithDefaultRedisClient(redisURI string) clientOpt {
	return func(c *Client) error {
		if c.redisClient != nil {
			return fmt.Errorf("redis client already set")
		}
		redisClient, err := c.newDefaultRedisClient(redisURI)
		if err != nil {
			return err
		}
		c.redisClient = redisClient
		c.redisURI = []string{redisURI}
		return nil
	}
}

func WithDefaultRedisClusterClient(redisURI []string) clientOpt {
	return func(c *Client) error {
		if c.redisClient != nil {
			return fmt.Errorf("redis client already set")
		}
		redisClient, err := c.newDefaultRedisClusterClient(redisURI)
		if err != nil {
			return err
		}
		c.redisClient = redisClient
		c.redisURI = redisURI
		c.clusterMode = true
		return nil
	}
}

package elasticache

import (
	"fmt"
	"strings"

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

// WithDialHost overrides the TCP host for WithDefaultRedisClient (e.g.
// "127.0.0.1" for an SSM port-forward). Apply before WithDefaultRedisClient.
// IAM auth still uses the replication group id; TLS ServerName stays the
// canonical host from REDIS_URI.
func WithDialHost(host string) clientOpt {
	return func(c *Client) error {
		c.dialHost = strings.TrimSpace(host)
		return nil
	}
}

// WithDialPort overrides the TCP port for WithDefaultRedisClient (e.g. local
// forward port). Apply before WithDefaultRedisClient.
func WithDialPort(port int) clientOpt {
	return func(c *Client) error {
		c.dialPort = port
		return nil
	}
}

// WithDefaultRedisClient sets the default redis client.
//
// redisURI is the canonical ElastiCache endpoint (host:port). When WithDialHost
// / WithDialPort were applied earlier, TCP dials the override while TLS
// ServerName remains the canonical hostname.
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

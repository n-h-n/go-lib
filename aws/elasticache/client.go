package elasticache

import (
	"context"
	"crypto/tls"
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/n-h-n/go-lib/aws/iam"
	"github.com/n-h-n/go-lib/log"
)

type Client struct {
	redisURI           []string
	redisClient        redis.UniversalClient
	iamClient          iam.IAMClient
	replicationGroupID string
	token              string
	clusterMode        bool
	verboseMode        bool
	ctx                context.Context
}

func NewClient(ctx context.Context, replicationGroupID string, verboseMode bool, clientOptions ...clientOpt) (*Client, error) {
	c := &Client{
		replicationGroupID: replicationGroupID,
		ctx:                ctx,
		verboseMode:        verboseMode,
	}

	iamClient, err := iam.NewIAMClient(ctx, iam.WithVerboseMode(verboseMode), iam.WithSessionDuration(15*time.Minute))
	if err != nil {
		return nil, err
	}
	c.iamClient = iamClient

	for _, option := range clientOptions {
		err := option(c)
		if err != nil {
			return nil, err
		}
	}

	if c.redisClient == nil {
		return nil, fmt.Errorf("redis client cannot be nil, please provide a redis client")
	}

	if err := c.writeTokenToLocalFile(); err != nil {
		log.Log.Errorf(c.ctx, "failed to write elasticache token to local file: %v", err)
	}

	go c.runPeriodicRefresh()

	return c, nil
}

func (c *Client) newDefaultRedisClient(redisURI string) (redis.UniversalClient, error) {
	token, err := c.generateIAMAuthToken()
	c.token = token
	if err != nil {
		return nil, err
	}

	redisClient := redis.NewClient(&redis.Options{
		Addr:      redisURI,
		Username:  c.iamClient.GetServiceName(),
		Password:  token,
		TLSConfig: &tls.Config{},
	})

	_, err = redisClient.Ping(context.Background()).Result()
	if err != nil {
		return nil, err
	}
	return redisClient, nil
}

func (c *Client) newDefaultRedisClusterClient(redisURI []string) (redis.UniversalClient, error) {
	token, err := c.generateIAMAuthToken()
	c.token = token
	if err != nil {
		return nil, err
	}
	redisClient := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:     redisURI,
		Username:  c.iamClient.GetServiceName(),
		Password:  token,
		TLSConfig: &tls.Config{},
	})

	_, err = redisClient.Ping(context.Background()).Result()
	if err != nil {
		return nil, err
	}

	return redisClient, nil
}

func (c *Client) refreshRedisClient() error {
	if c.iamClient.GetSessionTimeRemaining().Seconds() > c.iamClient.GetSessionDuration().Seconds()*c.iamClient.GetRefreshPercentage() {
		if c.verboseMode {
			log.Log.Debugf(c.ctx, "redis client: session time remaining is %v, refreshing at threshold in %v",
				c.iamClient.GetSessionTimeRemaining(),
				c.iamClient.GetSessionTimeRemaining()-time.Duration(c.iamClient.GetSessionDuration().Seconds()*c.iamClient.GetRefreshPercentage())*time.Second,
			)
		}
		return nil
	}

	if c.verboseMode {
		log.Log.Debugf(c.ctx, "refreshing redis client....")
	}

	if err := c.iamClient.RefreshAWSCreds(c.ctx); err != nil {
		return err
	}

	if c.clusterMode {
		newClient, newClientErr := c.newDefaultRedisClusterClient(c.redisURI)

		if newClientErr != nil {
			log.Log.Errorf(c.ctx, "while refreshing redis creds, failed to create new redis cluster client: %v", newClientErr)
			return newClientErr
		}

		c.redisClient = newClient
	} else {
		newClient, newClientErr := c.newDefaultRedisClient(c.redisURI[0])

		if newClientErr != nil {
			log.Log.Errorf(c.ctx, "while refreshing redis creds, failed to create new redis client: %v", newClientErr)
			return newClientErr
		}

		c.redisClient = newClient
	}

	pingResp, err := c.Ping(c.ctx).Result()
	if err != nil {
		log.Log.Errorf(c.ctx, "while refreshing redis creds, failed to ping redis client: %v, retrying....", err)
		go c.refreshRedisClient()
		return err
	}

	// }
	if c.verboseMode {
		log.Log.Debugf(c.ctx, "ping resp: %v", pingResp)
	}

	if c.verboseMode {
		log.Log.Debugf(c.ctx, "successfully refreshed redis client")
	}

	if err := c.writeTokenToLocalFile(); err != nil {
		log.Log.Errorf(c.ctx, "failed to write elasticache token to local file: %v", err)
	}

	return nil
}

func (c *Client) runPeriodicRefresh() {
	minMilliseconds := 58000
	maxMilliSeconds := 62000
	interval := rand.Intn(maxMilliSeconds-minMilliseconds) + minMilliseconds

	ticker := time.NewTicker(time.Duration(interval) * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := c.refreshRedisClient(); err != nil {
				log.Log.Errorf(c.ctx, "failed to refresh redis client: %v", err)
			}
		case <-c.ctx.Done():
			return
		}
	}
}

func (c *Client) generateIAMAuthToken() (string, error) {
	tokenRequest := IAMAuthTokenRequest{
		ctx:                c.ctx,
		userID:             c.iamClient.GetServiceName(),
		replicationGroupID: c.replicationGroupID,
		region:             c.iamClient.GetAWSRegion(),
	}

	return tokenRequest.toSignedRequestURI(aws.Credentials{
		AccessKeyID:     *c.iamClient.GetAssumedRole().Credentials.AccessKeyId,
		SecretAccessKey: *c.iamClient.GetAssumedRole().Credentials.SecretAccessKey,
		SessionToken:    *c.iamClient.GetAssumedRole().Credentials.SessionToken,
	})
}

func (c *Client) writeTokenToLocalFile() error {
	tokenFile, err := os.Create("./elasticache-token.txt")
	if err != nil {
		return err
	}
	defer tokenFile.Close()

	_, err = tokenFile.WriteString(c.token)
	if err != nil {
		return err
	}
	return nil
}

func (c *Client) Auth(ctx context.Context, password string) (string, error) {
	return c.redisClient.Pipeline().Auth(ctx, password).Result()
}

func (c *Client) Close() error {
	return c.redisClient.Close()
}

func (c *Client) Del(ctx context.Context, keys ...string) *redis.IntCmd {
	return c.redisClient.Del(ctx, keys...)
}

func (c *Client) Expire(ctx context.Context, key string, expiration time.Duration) *redis.BoolCmd {
	return c.redisClient.Expire(ctx, key, expiration)
}

func (c *Client) Get(ctx context.Context, key string) *redis.StringCmd {
	return c.redisClient.Get(ctx, key)
}

func (c *Client) GetRedisClient() redis.UniversalClient {
	return c.redisClient
}

func (c *Client) HDel(ctx context.Context, key string, fields ...string) *redis.IntCmd {
	return c.redisClient.HDel(ctx, key, fields...)
}

func (c *Client) HGet(ctx context.Context, key, field string) *redis.StringCmd {
	return c.redisClient.HGet(ctx, key, field)
}

func (c *Client) HGetAll(ctx context.Context, key string) *redis.MapStringStringCmd {
	return c.redisClient.HGetAll(ctx, key)
}

func (c *Client) HIncrBy(ctx context.Context, key, field string, incr int64) *redis.IntCmd {
	return c.redisClient.HIncrBy(ctx, key, field, incr)
}

func (c *Client) HKeys(ctx context.Context, key string) *redis.StringSliceCmd {
	return c.redisClient.HKeys(ctx, key)
}

func (c *Client) HLen(ctx context.Context, key string) *redis.IntCmd {
	return c.redisClient.HLen(ctx, key)
}

func (c *Client) HMGet(ctx context.Context, key string, fields ...string) *redis.SliceCmd {
	return c.redisClient.HMGet(ctx, key, fields...)
}

func (c *Client) HSet(ctx context.Context, key string, pairs ...interface{}) *redis.IntCmd {
	return c.redisClient.HSet(ctx, key, pairs...)
}

func (c *Client) LLen(ctx context.Context, key string) *redis.IntCmd {
	return c.redisClient.LLen(ctx, key)
}

func (c *Client) LPop(ctx context.Context, key string) *redis.StringCmd {
	return c.redisClient.LPop(ctx, key)
}

func (c *Client) LPush(ctx context.Context, key string, values ...interface{}) *redis.IntCmd {
	return c.redisClient.LPush(ctx, key, values...)
}

func (c *Client) LRange(ctx context.Context, key string, start, stop int64) *redis.StringSliceCmd {
	return c.redisClient.LRange(ctx, key, start, stop)
}

func (c *Client) LTrim(ctx context.Context, key string, start, stop int64) *redis.StatusCmd {
	return c.redisClient.LTrim(ctx, key, start, stop)
}

func (c *Client) Pipeline() redis.Pipeliner {
	return c.redisClient.Pipeline()
}

func (c *Client) Ping(ctx context.Context) *redis.StatusCmd {
	return c.redisClient.Ping(ctx)
}

func (c *Client) RPop(ctx context.Context, key string) *redis.StringCmd {
	return c.redisClient.RPop(ctx, key)
}

func (c *Client) RPush(ctx context.Context, key string, values ...interface{}) *redis.IntCmd {
	return c.redisClient.RPush(ctx, key, values...)
}

func (c *Client) SAdd(ctx context.Context, key string, members ...interface{}) *redis.IntCmd {
	return c.redisClient.SAdd(ctx, key, members...)
}

func (c *Client) SCard(ctx context.Context, key string) *redis.IntCmd {
	return c.redisClient.SCard(ctx, key)
}

func (c *Client) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.StatusCmd {
	return c.redisClient.Set(ctx, key, value, expiration)
}

func (c *Client) SIsMember(ctx context.Context, key string, member interface{}) *redis.BoolCmd {
	return c.redisClient.SIsMember(ctx, key, member)
}

func (c *Client) SMembers(ctx context.Context, key string) *redis.StringSliceCmd {
	return c.redisClient.SMembers(ctx, key)
}

func (c *Client) SRem(ctx context.Context, key string, members ...interface{}) *redis.IntCmd {
	return c.redisClient.SRem(ctx, key, members...)
}

func (c *Client) TTL(ctx context.Context, key string) *redis.DurationCmd {
	return c.redisClient.TTL(ctx, key)
}

func (c *Client) ZAdd(ctx context.Context, key string, members ...redis.Z) *redis.IntCmd {
	return c.redisClient.ZAdd(ctx, key, members...)
}

func (c *Client) ZCard(ctx context.Context, key string) *redis.IntCmd {
	return c.redisClient.ZCard(ctx, key)
}

func (c *Client) ZCount(ctx context.Context, key, min, max string) *redis.IntCmd {
	return c.redisClient.ZCount(ctx, key, min, max)
}

func (c *Client) ZIncrBy(ctx context.Context, key string, increment float64, member string) *redis.FloatCmd {
	return c.redisClient.ZIncrBy(ctx, key, increment, member)
}

func (c *Client) ZInterStore(ctx context.Context, dest string, store *redis.ZStore) *redis.IntCmd {
	return c.redisClient.ZInterStore(ctx, dest, store)
}

func (c *Client) ZRange(ctx context.Context, key string, start, stop int64) *redis.StringSliceCmd {
	return c.redisClient.ZRange(ctx, key, start, stop)
}

func (c *Client) ZRangeByScore(ctx context.Context, key string, opt *redis.ZRangeBy) *redis.StringSliceCmd {
	return c.redisClient.ZRangeByScore(ctx, key, opt)
}

func (c *Client) ZRank(ctx context.Context, key, member string) *redis.IntCmd {
	return c.redisClient.ZRank(ctx, key, member)
}

func (c *Client) ZRem(ctx context.Context, key string, members ...interface{}) *redis.IntCmd {
	return c.redisClient.ZRem(ctx, key, members...)
}

func (c *Client) ZRemRangeByRank(ctx context.Context, key string, start, stop int64) *redis.IntCmd {
	return c.redisClient.ZRemRangeByRank(ctx, key, start, stop)
}

func (c *Client) ZRemRangeByScore(ctx context.Context, key, min, max string) *redis.IntCmd {
	return c.redisClient.ZRemRangeByScore(ctx, key, min, max)
}

func (c *Client) ZRevRange(ctx context.Context, key string, start, stop int64) *redis.StringSliceCmd {
	return c.redisClient.ZRevRange(ctx, key, start, stop)
}

func (c *Client) ZRevRangeByScore(ctx context.Context, key string, opt *redis.ZRangeBy) *redis.StringSliceCmd {
	return c.redisClient.ZRevRangeByScore(ctx, key, opt)
}

func (c *Client) ZRevRank(ctx context.Context, key, member string) *redis.IntCmd {
	return c.redisClient.ZRevRank(ctx, key, member)
}

func (c *Client) ZScore(ctx context.Context, key, member string) *redis.FloatCmd {
	return c.redisClient.ZScore(ctx, key, member)
}

func (c *Client) ZUnionStore(ctx context.Context, dest string, store *redis.ZStore) *redis.IntCmd {
	return c.redisClient.ZUnionStore(ctx, dest, store)
}

func (c *Client) RandomTestingSet() error {
	id := uuid.New().String()

	status, err := c.Set(c.ctx, "test", id, 2*time.Minute).Result()
	if err != nil {
		return err
	}

	log.Log.Debugf(c.ctx, "successfully set test to %v: %v", id, status)

	return nil
}

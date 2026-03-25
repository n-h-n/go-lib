package elasticache

import (
	"context"
	"crypto/tls"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"sync"
	"time"

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
	mu                 sync.RWMutex
	refreshMu          sync.Mutex
}

// ElastiCache IAM auth tokens expire after ~15 minutes. Recycle pooled Redis
// connections ahead of that window so reconnects always fetch fresh IAM auth.
const proactiveConnectionRotationAfter = 12 * time.Minute

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
	redisClient := redis.NewClient(&redis.Options{
		Addr:                       redisURI,
		CredentialsProviderContext: c.redisCredentialsProvider,
		ConnMaxLifetime:            proactiveConnectionRotationAfter,
		TLSConfig:                  &tls.Config{},
	})

	_, err := redisClient.Ping(context.Background()).Result()
	if err != nil {
		return nil, err
	}
	return redisClient, nil
}

func (c *Client) newDefaultRedisClusterClient(redisURI []string) (redis.UniversalClient, error) {
	redisClient := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:                      redisURI,
		CredentialsProviderContext: c.redisCredentialsProvider,
		ConnMaxLifetime:            proactiveConnectionRotationAfter,
		TLSConfig:                  &tls.Config{},
	})

	_, err := redisClient.Ping(context.Background()).Result()
	if err != nil {
		return nil, err
	}

	return redisClient, nil
}

func (c *Client) refreshRedisClient(force bool) error {
	if c.verboseMode {
		log.Log.Debugf(c.ctx, "refreshing redis client....")
	}

	if _, err := c.prepareAuthToken(c.ctx, force); err != nil {
		return err
	}

	pingResp, err := c.Ping(c.ctx).Result()
	if err != nil {
		log.Log.Errorf(c.ctx, "while refreshing redis creds, failed to ping redis client: %v, retrying....", err)
		go func() {
			if retryErr := c.refreshRedisClient(true); retryErr != nil {
				log.Log.Errorf(c.ctx, "retrying redis credential refresh failed: %v", retryErr)
			}
		}()
		return err
	}

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
			if err := c.refreshRedisClient(false); err != nil {
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

	// Use original credentials (pod identity) for ElastiCache IAM auth
	// ElastiCache IAM authentication requires credentials that match the ElastiCache user
	// When a role assumes itself, the assumed role credentials don't map correctly to ElastiCache users
	// The original credentials are refreshed alongside the assumed role credentials in RefreshAWSCreds
	// See: https://docs.aws.amazon.com/AmazonElastiCache/latest/dg/auth-iam.html
	creds := c.iamClient.GetOriginalCredentials()

	return tokenRequest.toSignedRequestURI(creds)
}

func (c *Client) prepareAuthToken(ctx context.Context, force bool) (string, error) {
	if ctx == nil {
		ctx = c.ctx
	}

	c.refreshMu.Lock()
	defer c.refreshMu.Unlock()

	refreshOpts := []func(*iam.RefreshOpts){}
	if force {
		refreshOpts = append(refreshOpts, iam.WithForceRefresh(true))
	}
	if err := c.iamClient.RefreshAWSCreds(ctx, refreshOpts...); err != nil {
		return "", err
	}

	token, err := c.generateIAMAuthToken()
	if err != nil {
		return "", err
	}

	c.mu.Lock()
	c.token = token
	c.mu.Unlock()

	return token, nil
}

func (c *Client) redisCredentialsProvider(ctx context.Context) (string, string, error) {
	token, err := c.prepareAuthToken(ctx, false)
	if err != nil {
		return "", "", err
	}
	return c.iamClient.GetServiceName(), token, nil
}

func (c *Client) writeTokenToLocalFile() error {
	c.mu.RLock()
	token := c.token
	c.mu.RUnlock()

	tokenFile, err := os.Create("./elasticache-token.txt")
	if err != nil {
		return err
	}
	defer tokenFile.Close()

	_, err = tokenFile.WriteString(token)
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
	cmd := c.redisClient.Del(ctx, keys...)
	if c.refreshOnAuthError(ctx, cmd.Err()) {
		cmd = c.redisClient.Del(ctx, keys...)
	}
	return cmd
}

func (c *Client) Expire(ctx context.Context, key string, expiration time.Duration) *redis.BoolCmd {
	cmd := c.redisClient.Expire(ctx, key, expiration)
	if c.refreshOnAuthError(ctx, cmd.Err()) {
		cmd = c.redisClient.Expire(ctx, key, expiration)
	}
	return cmd
}

func (c *Client) Get(ctx context.Context, key string) *redis.StringCmd {
	cmd := c.redisClient.Get(ctx, key)
	if c.refreshOnAuthError(ctx, cmd.Err()) {
		cmd = c.redisClient.Get(ctx, key)
	}
	return cmd
}

func (c *Client) GetRedisClient() redis.UniversalClient {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.redisClient
}

// RefreshAndGetRedisClient forces credential refresh and returns the latest
// redis client. It is intended for callers that received auth errors and need
// to recover immediately rather than waiting for periodic refresh.
func (c *Client) RefreshAndGetRedisClient() redis.UniversalClient {
	if err := c.refreshRedisClient(true); err != nil {
		log.Log.Errorf(c.ctx, "failed to force refresh redis client: %v", err)
	}
	return c.redisClient
}

func isAuthError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "WRONGPASS") || strings.Contains(msg, "NOAUTH")
}

func (c *Client) refreshOnAuthError(ctx context.Context, err error) bool {
	if !isAuthError(err) {
		return false
	}
	log.Log.Warnf(ctx, "redis auth failure detected, forcing elasticache client refresh")
	if refreshErr := c.refreshRedisClient(true); refreshErr != nil {
		log.Log.Errorf(ctx, "failed refreshing elasticache client after auth error: %v", refreshErr)
		return false
	}
	return true
}

func (c *Client) HDel(ctx context.Context, key string, fields ...string) *redis.IntCmd {
	cmd := c.redisClient.HDel(ctx, key, fields...)
	if c.refreshOnAuthError(ctx, cmd.Err()) {
		cmd = c.redisClient.HDel(ctx, key, fields...)
	}
	return cmd
}

func (c *Client) HGet(ctx context.Context, key, field string) *redis.StringCmd {
	cmd := c.redisClient.HGet(ctx, key, field)
	if c.refreshOnAuthError(ctx, cmd.Err()) {
		cmd = c.redisClient.HGet(ctx, key, field)
	}
	return cmd
}

func (c *Client) HGetAll(ctx context.Context, key string) *redis.MapStringStringCmd {
	return c.redisClient.HGetAll(ctx, key)
}

func (c *Client) HIncrBy(ctx context.Context, key, field string, incr int64) *redis.IntCmd {
	return c.redisClient.HIncrBy(ctx, key, field, incr)
}

func (c *Client) HKeys(ctx context.Context, key string) *redis.StringSliceCmd {
	cmd := c.redisClient.HKeys(ctx, key)
	if c.refreshOnAuthError(ctx, cmd.Err()) {
		cmd = c.redisClient.HKeys(ctx, key)
	}
	return cmd
}

func (c *Client) HLen(ctx context.Context, key string) *redis.IntCmd {
	cmd := c.redisClient.HLen(ctx, key)
	if c.refreshOnAuthError(ctx, cmd.Err()) {
		cmd = c.redisClient.HLen(ctx, key)
	}
	return cmd
}

func (c *Client) HMGet(ctx context.Context, key string, fields ...string) *redis.SliceCmd {
	cmd := c.redisClient.HMGet(ctx, key, fields...)
	if c.refreshOnAuthError(ctx, cmd.Err()) {
		cmd = c.redisClient.HMGet(ctx, key, fields...)
	}
	return cmd
}

func (c *Client) HSet(ctx context.Context, key string, pairs ...interface{}) *redis.IntCmd {
	cmd := c.redisClient.HSet(ctx, key, pairs...)
	if c.refreshOnAuthError(ctx, cmd.Err()) {
		cmd = c.redisClient.HSet(ctx, key, pairs...)
	}
	return cmd
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
	cmd := c.redisClient.SAdd(ctx, key, members...)
	if c.refreshOnAuthError(ctx, cmd.Err()) {
		cmd = c.redisClient.SAdd(ctx, key, members...)
	}
	return cmd
}

func (c *Client) SCard(ctx context.Context, key string) *redis.IntCmd {
	return c.redisClient.SCard(ctx, key)
}

func (c *Client) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.StatusCmd {
	cmd := c.redisClient.Set(ctx, key, value, expiration)
	if c.refreshOnAuthError(ctx, cmd.Err()) {
		cmd = c.redisClient.Set(ctx, key, value, expiration)
	}
	return cmd
}

func (c *Client) SIsMember(ctx context.Context, key string, member interface{}) *redis.BoolCmd {
	return c.redisClient.SIsMember(ctx, key, member)
}

func (c *Client) SMembers(ctx context.Context, key string) *redis.StringSliceCmd {
	cmd := c.redisClient.SMembers(ctx, key)
	if c.refreshOnAuthError(ctx, cmd.Err()) {
		cmd = c.redisClient.SMembers(ctx, key)
	}
	return cmd
}

func (c *Client) SRem(ctx context.Context, key string, members ...interface{}) *redis.IntCmd {
	cmd := c.redisClient.SRem(ctx, key, members...)
	if c.refreshOnAuthError(ctx, cmd.Err()) {
		cmd = c.redisClient.SRem(ctx, key, members...)
	}
	return cmd
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

package leadership

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

var ErrLeaseNotFound = errors.New("lease not found")
var ErrLeaseUnknown = errors.New("lease state is unknown")

var redisAcquireScript = redis.NewScript(`
local function now_ms()
  local now = redis.call('TIME')
  return now[1] * 1000 + math.floor(now[2] / 1000)
end
local current = redis.call('HGETALL', KEYS[1])
if #current == 0 then
  local durable = redis.call('GET', KEYS[2])
  if durable and (not tonumber(durable) or tonumber(durable) <= 0) then
    return {2}
  end
  local epoch = redis.call('INCR', KEYS[2])
  local issued = now_ms()
  local expires = issued + tonumber(ARGV[4])
  redis.call('HSET', KEYS[1], 'cluster', ARGV[1], 'holder', ARGV[2], 'token', ARGV[3], 'epoch', epoch, 'issued', issued)
  redis.call('PEXPIRE', KEYS[1], ARGV[4])
  return {1, ARGV[2], ARGV[3], epoch, issued, expires}
end
local ttl = redis.call('PTTL', KEYS[1])
if ttl <= 0 then
  return {2, redis.call('HGET', KEYS[1], 'holder'), redis.call('HGET', KEYS[1], 'token'), redis.call('HGET', KEYS[1], 'epoch'), redis.call('HGET', KEYS[1], 'issued'), 0}
end
if redis.call('HGET', KEYS[1], 'cluster') ~= ARGV[1] then
  return {2, redis.call('HGET', KEYS[1], 'holder'), redis.call('HGET', KEYS[1], 'token'), redis.call('HGET', KEYS[1], 'epoch'), redis.call('HGET', KEYS[1], 'issued'), 0}
end
local current_epoch = tonumber(redis.call('HGET', KEYS[1], 'epoch'))
local durable_epoch = tonumber(redis.call('GET', KEYS[2]))
if not current_epoch or current_epoch <= 0 or not durable_epoch or durable_epoch < current_epoch then
  return {2, redis.call('HGET', KEYS[1], 'holder'), redis.call('HGET', KEYS[1], 'token'), redis.call('HGET', KEYS[1], 'epoch'), redis.call('HGET', KEYS[1], 'issued'), 0}
end
local issued = redis.call('HGET', KEYS[1], 'issued')
return {0, redis.call('HGET', KEYS[1], 'holder'), redis.call('HGET', KEYS[1], 'token'), redis.call('HGET', KEYS[1], 'epoch'), issued, now_ms() + ttl}
`)

var redisRenewScript = redis.NewScript(`
local function now_ms()
  local now = redis.call('TIME')
  return now[1] * 1000 + math.floor(now[2] / 1000)
end
local current_epoch = tonumber(redis.call('HGET', KEYS[1], 'epoch'))
if not current_epoch or current_epoch <= 0 then
  return {2}
end
if redis.call('HGET', KEYS[1], 'cluster') ~= ARGV[1] or redis.call('HGET', KEYS[1], 'holder') ~= ARGV[2] or redis.call('HGET', KEYS[1], 'token') ~= ARGV[3] or redis.call('HGET', KEYS[1], 'epoch') ~= ARGV[4] then
  return {0}
end
local ttl = redis.call('PTTL', KEYS[1])
if ttl == -1 or ttl == 0 then
  return {2}
end
if ttl < 0 then
  return {0}
end
local issued = redis.call('HGET', KEYS[1], 'issued')
local epoch = redis.call('HGET', KEYS[1], 'epoch')
local now = now_ms()
redis.call('PEXPIRE', KEYS[1], ARGV[5])
return {1, ARGV[2], ARGV[3], epoch, issued, now + tonumber(ARGV[5])}
`)

var redisReleaseScript = redis.NewScript(`
local current_epoch = tonumber(redis.call('HGET', KEYS[1], 'epoch'))
if not current_epoch or current_epoch <= 0 then
  return 2
end
if redis.call('HGET', KEYS[1], 'cluster') == ARGV[1] and redis.call('HGET', KEYS[1], 'holder') == ARGV[2] and redis.call('HGET', KEYS[1], 'token') == ARGV[3] and redis.call('HGET', KEYS[1], 'epoch') == ARGV[4] then
  local ttl = redis.call('PTTL', KEYS[1])
  if ttl == -1 or ttl == 0 then
    return 2
  end
  if ttl < 0 then
    return 0
  end
  redis.call('DEL', KEYS[1])
  return 1
end
return 0
`)

var redisGetScript = redis.NewScript(`
local current = redis.call('HGETALL', KEYS[1])
if #current == 0 then
  return {0}
end
local ttl = redis.call('PTTL', KEYS[1])
if ttl == -1 or ttl == 0 then
  return {2, redis.call('HGET', KEYS[1], 'holder'), redis.call('HGET', KEYS[1], 'token'), redis.call('HGET', KEYS[1], 'epoch'), redis.call('HGET', KEYS[1], 'issued'), 0}
end
if ttl < 0 then
  return {0}
end
local now = redis.call('TIME')
local nowms = now[1] * 1000 + math.floor(now[2] / 1000)
return {1, redis.call('HGET', KEYS[1], 'holder'), redis.call('HGET', KEYS[1], 'token'), redis.call('HGET', KEYS[1], 'epoch'), redis.call('HGET', KEYS[1], 'issued'), nowms + ttl}
`)

type RedisStore struct {
	client redis.UniversalClient
	prefix string
}

func NewRedisStore(client redis.UniversalClient, prefix string) *RedisStore {
	return &RedisStore{client: client, prefix: strings.TrimSuffix(prefix, ":")}
}

func (s *RedisStore) Acquire(ctx context.Context, clusterID, holderID string, ttl time.Duration) (LeaseRecord, bool, error) {
	if err := validateLeaseRequest(clusterID, holderID, ttl); err != nil {
		return LeaseRecord{}, false, err
	}
	token, err := newLeaseToken()
	if err != nil {
		return LeaseRecord{}, false, err
	}
	result, err := redisAcquireScript.Run(ctx, s.client, s.keys(clusterID), clusterID, holderID, token, ttl.Milliseconds()).Result()
	if err != nil {
		return LeaseRecord{}, false, err
	}
	return parseRedisAcquireResult(clusterID, result)
}

func (s *RedisStore) Renew(ctx context.Context, lease LeaseRecord, ttl time.Duration) (LeaseRecord, bool, error) {
	if err := validateLeaseRequest(lease.ClusterID, lease.HolderID, ttl); err != nil {
		return LeaseRecord{}, false, err
	}
	if err := validateLeaseIdentity(lease); err != nil {
		return LeaseRecord{}, false, err
	}
	result, err := redisRenewScript.Run(ctx, s.client, []string{s.leaseKey(lease.ClusterID)}, lease.ClusterID, lease.HolderID, lease.Token, lease.Epoch, ttl.Milliseconds()).Result()
	if err != nil {
		return LeaseRecord{}, false, err
	}
	return parseRedisRenewResult(lease.ClusterID, result)
}

func (s *RedisStore) Release(ctx context.Context, lease LeaseRecord) error {
	if err := validateLeaseIdentity(lease); err != nil {
		return err
	}
	result, err := redisReleaseScript.Run(ctx, s.client, []string{s.leaseKey(lease.ClusterID)}, lease.ClusterID, lease.HolderID, lease.Token, lease.Epoch).Result()
	if err != nil {
		return err
	}
	state, err := redisInt(result)
	if err != nil {
		return err
	}
	switch state {
	case 1:
		return nil
	case 2:
		return ErrLeaseUnknown
	default:
		return ErrStaleLease
	}
}

func (s *RedisStore) Get(ctx context.Context, clusterID string) (LeaseRecord, error) {
	if strings.TrimSpace(clusterID) == "" {
		return LeaseRecord{}, fmt.Errorf("cluster ID is required")
	}
	result, err := redisGetScript.Run(ctx, s.client, []string{s.leaseKey(clusterID)}).Result()
	if err != nil {
		return LeaseRecord{}, err
	}
	record, found, err := parseRedisGetResult(clusterID, result)
	if err != nil {
		return LeaseRecord{}, err
	}
	if !found {
		return LeaseRecord{}, ErrLeaseNotFound
	}
	return record, nil
}

func (s *RedisStore) keys(clusterID string) []string {
	return []string{s.leaseKey(clusterID), s.epochKey(clusterID)}
}

func (s *RedisStore) leaseKey(clusterID string) string { return s.prefix + ":lease:" + clusterID }
func (s *RedisStore) epochKey(clusterID string) string { return s.prefix + ":epoch:" + clusterID }

func parseRedisAcquireResult(clusterID string, result interface{}) (LeaseRecord, bool, error) {
	values, state, err := redisScriptValues(result)
	if err != nil {
		return LeaseRecord{}, false, err
	}
	if state == 2 {
		return LeaseRecord{}, false, ErrLeaseUnknown
	}
	if state != 0 && state != 1 {
		return LeaseRecord{}, false, fmt.Errorf("unexpected Redis acquire state %d", state)
	}
	record, err := parseRedisLeaseValues(clusterID, values)
	return record, state == 1, err
}

func parseRedisRenewResult(clusterID string, result interface{}) (LeaseRecord, bool, error) {
	values, state, err := redisScriptValues(result)
	if err != nil {
		return LeaseRecord{}, false, err
	}
	if state == 0 {
		return LeaseRecord{}, false, nil
	}
	if state == 2 {
		return LeaseRecord{}, false, ErrLeaseUnknown
	}
	if state != 1 {
		return LeaseRecord{}, false, fmt.Errorf("unexpected Redis renew state %d", state)
	}
	record, err := parseRedisLeaseValues(clusterID, values)
	return record, true, err
}

func parseRedisGetResult(clusterID string, result interface{}) (LeaseRecord, bool, error) {
	values, state, err := redisScriptValues(result)
	if err != nil {
		return LeaseRecord{}, false, err
	}
	if state == 0 {
		return LeaseRecord{}, false, nil
	}
	if state == 2 {
		return LeaseRecord{}, false, ErrLeaseUnknown
	}
	if state != 1 {
		return LeaseRecord{}, false, fmt.Errorf("unexpected Redis get state %d", state)
	}
	record, err := parseRedisLeaseValues(clusterID, values)
	return record, true, err
}

func redisScriptValues(result interface{}) ([]interface{}, int64, error) {
	values, ok := result.([]interface{})
	if !ok || len(values) == 0 {
		return nil, 0, fmt.Errorf("unexpected Redis lease script response %T", result)
	}
	state, err := redisInt(values[0])
	if err != nil {
		return nil, 0, err
	}
	return values, state, nil
}

func parseRedisLeaseValues(clusterID string, values []interface{}) (LeaseRecord, error) {
	if len(values) != 6 {
		return LeaseRecord{}, fmt.Errorf("unexpected Redis lease record length %d", len(values))
	}
	epoch, err := redisInt(values[3])
	if err != nil {
		return LeaseRecord{}, err
	}
	issued, err := redisInt(values[4])
	if err != nil {
		return LeaseRecord{}, err
	}
	expires, err := redisInt(values[5])
	if err != nil {
		return LeaseRecord{}, err
	}
	if epoch <= 0 || strings.TrimSpace(fmt.Sprint(values[1])) == "" || strings.TrimSpace(fmt.Sprint(values[2])) == "" || expires <= issued {
		return LeaseRecord{}, ErrLeaseUnknown
	}
	return LeaseRecord{ClusterID: clusterID, HolderID: fmt.Sprint(values[1]), Token: fmt.Sprint(values[2]), Epoch: epoch, IssuedAt: time.UnixMilli(issued), ExpiresAt: time.UnixMilli(expires)}, nil
}

func redisInt(value interface{}) (int64, error) {
	switch value := value.(type) {
	case int64:
		return value, nil
	case string:
		return strconv.ParseInt(value, 10, 64)
	case []byte:
		return strconv.ParseInt(string(value), 10, 64)
	default:
		return 0, fmt.Errorf("unexpected Redis integer type %T", value)
	}
}

func validateLeaseRequest(clusterID, holderID string, ttl time.Duration) error {
	if strings.TrimSpace(clusterID) == "" || strings.TrimSpace(holderID) == "" {
		return fmt.Errorf("cluster ID and holder ID are required")
	}
	if ttl <= 0 || ttl.Milliseconds() <= 0 {
		return fmt.Errorf("lease TTL must be at least one millisecond")
	}
	return nil
}

func validateLeaseIdentity(lease LeaseRecord) error {
	if strings.TrimSpace(lease.ClusterID) == "" || strings.TrimSpace(lease.HolderID) == "" || strings.TrimSpace(lease.Token) == "" {
		return fmt.Errorf("cluster ID, holder ID, and token are required")
	}
	if lease.Epoch <= 0 {
		return fmt.Errorf("lease epoch must be positive")
	}
	return nil
}

func newLeaseToken() (string, error) {
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}

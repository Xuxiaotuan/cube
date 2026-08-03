package main

import "testing"

func TestNewRedisClientUsesSecretPasswordWithoutChangingURL(t *testing.T) {
	client, err := newRedisClient("redis://redis.example:6379/0", "redis-secret")
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	options := client.Options()
	if options.Password != "redis-secret" {
		t.Fatalf("Redis password = %q, want Secret-provided password", options.Password)
	}
	if options.Addr != "redis.example:6379" {
		t.Fatalf("Redis address = %q, want redis.example:6379", options.Addr)
	}
}

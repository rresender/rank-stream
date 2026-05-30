//go:build integration
// +build integration

package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"rank-stream/queue"

	"github.com/go-redis/redis/v7"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/require"
)

var redisPool *dockertest.Pool
var redisResource *dockertest.Resource

func TestMain(m *testing.M) {
	var err error
	redisPool, err = dockertest.NewPool("")
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create redis docker pool: %v\n", err)
		os.Exit(1)
	}

	redisResource, err = redisPool.RunWithOptions(&dockertest.RunOptions{
		Repository: "redis",
		Tag:        "6-alpine",
	}, func(hostConfig *docker.HostConfig) {
		hostConfig.AutoRemove = true
		hostConfig.RestartPolicy = docker.RestartPolicy{Name: "no"}
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to start redis container: %v\n", err)
		os.Exit(1)
	}

	addr := fmt.Sprintf("localhost:%s", redisResource.GetPort("6379/tcp"))
	os.Setenv("REDIS_ADDR", addr)
	os.Setenv("REDIS_TLS", "false")

	if err := redisPool.Retry(func() error {
		client := redis.NewClient(&redis.Options{Addr: addr})
		return client.Ping().Err()
	}); err != nil {
		redisPool.Purge(redisResource)
		fmt.Fprintf(os.Stderr, "failed to connect to redis: %v\n", err)
		os.Exit(1)
	}

	code := m.Run()

	if err := redisPool.Purge(redisResource); err != nil {
		fmt.Fprintf(os.Stderr, "failed to purge redis container: %v\n", err)
	}

	os.Exit(code)
}

func newIntegrationServer(t *testing.T) (*httptest.Server, *redis.Client, func()) {
	addr := os.Getenv("REDIS_ADDR")
	require.NotEmpty(t, addr, "REDIS_ADDR must be configured")

	seed := redis.NewClient(&redis.Options{Addr: addr})
	require.NoError(t, seed.FlushDB().Err())
	seed.Close()

	h, client, _ := BootStrap()
	testServer := httptest.NewServer(h)

	return testServer, client, func() {
		testServer.Close()
		client.Close()
	}
}

func doRequest(t *testing.T, client *http.Client, req *http.Request, out interface{}) {
	resp, err := client.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, json.NewDecoder(resp.Body).Decode(out))
}

func TestIntegrationHTTPQueueLifecycle(t *testing.T) {
	server, _, cleanup := newIntegrationServer(t)
	defer cleanup()

	httpClient := &http.Client{Timeout: 5 * time.Second}
	tenant := "tenant1"
	queueName := "orders"
	item := "item123"

	url := fmt.Sprintf("%s/queue-manager/api/v1/queue/%s/%s/%s", server.URL, tenant, queueName, item)
	req, err := http.NewRequest(http.MethodPut, url, nil)
	require.NoError(t, err)

	var response map[string]int64
	doRequest(t, httpClient, req, &response)
	require.Equal(t, int64(1), response["position"])

	url = fmt.Sprintf("%s/queue-manager/api/v1/queue/length/%s/%s", server.URL, tenant, queueName)
	req, err = http.NewRequest(http.MethodGet, url, nil)
	require.NoError(t, err)

	var lengthResp map[string]int64
	doRequest(t, httpClient, req, &lengthResp)
	require.Equal(t, int64(1), lengthResp["length"])

	url = fmt.Sprintf("%s/queue-manager/api/v1/queue/index/%s/%s/%s", server.URL, tenant, queueName, item)
	req, err = http.NewRequest(http.MethodGet, url, nil)
	require.NoError(t, err)

	var indexResp map[string]int64
	doRequest(t, httpClient, req, &indexResp)
	require.Equal(t, int64(1), indexResp["position"])

	url = fmt.Sprintf("%s/queue-manager/api/v1/queue/%s/%s", server.URL, tenant, queueName)
	req, err = http.NewRequest(http.MethodDelete, url, nil)
	require.NoError(t, err)

	var pullResp map[string]string
	doRequest(t, httpClient, req, &pullResp)
	require.Equal(t, item, pullResp["item"])

	url = fmt.Sprintf("%s/queue-manager/api/v1/queue/length/%s/%s", server.URL, tenant, queueName)
	req, err = http.NewRequest(http.MethodGet, url, nil)
	require.NoError(t, err)

	var emptyLengthResp map[string]int64
	doRequest(t, httpClient, req, &emptyLengthResp)
	require.Equal(t, int64(0), emptyLengthResp["length"])
}

func TestIntegrationStreamEventProcessing(t *testing.T) {
	_, client, cleanup := newIntegrationServer(t)
	defer cleanup()

	name := queue.Name{Tenant: "tenant2", Queue: "events"}
	message := map[string]interface{}{
		"event":    eQueuePushEvent,
		"token":    "token123",
		"room":     fmt.Sprintf("%s#%s", name.Tenant, name.Queue),
		"socketId": "provider123",
		"ttl":      "10",
	}
	payload, err := json.Marshal(message)
	require.NoError(t, err)

	_, err = client.XAdd(&redis.XAddArgs{
		Stream: queueStream,
		ID:     "*",
		Values: map[string]interface{}{"data": string(payload)},
	}).Result()
	require.NoError(t, err)

	q := queue.New(client)
	require.Eventually(t, func() bool {
		return q.Exists(name, "token123")
	}, 5*time.Second, 100*time.Millisecond)
}

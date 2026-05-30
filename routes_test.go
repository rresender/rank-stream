package main

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"rank-stream/emitter"
	"rank-stream/queue"
	"strings"
	"testing"

	"github.com/alicebob/miniredis"
	"github.com/go-redis/redis/v7"
	"github.com/gorilla/mux"
	"github.com/stretchr/testify/assert"
)

func validateBasics(resp *http.Response, err error, t *testing.T) []byte {
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("Received non-200 response: %d", resp.StatusCode)
	}
	defer resp.Body.Close()
	actual, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return actual
}

func health(server *httptest.Server, t *testing.T) {
	resp, err := http.Get(server.URL + "/health")
	actual := validateBasics(resp, err, t)
	m := make(map[string]interface{})
	json.Unmarshal(actual, &m)
	assert.Equal(t, "ping: PONG", m["status"].(string))
}

func queuesPerTenant(queuePool *queue.Queue, server *httptest.Server, t *testing.T) {

	name := queue.Name{
		Tenant: "local",
		Queue:  "default",
	}

	queuePool.Push(name, "item")

	resp, err := http.Get(server.URL + "/queue-manager/api/v1/queue/local")
	actual := validateBasics(resp, err, t)

	m := make(map[string][]string)
	json.Unmarshal(actual, &m)

	assert.Equal(t, []string{"local#default"}, m["queues"])

	queuePool.Pull(name)
}

func queues(queuePool *queue.Queue, server *httptest.Server, t *testing.T) {

	name := queue.Name{
		Tenant: "local",
		Queue:  "default",
	}

	queuePool.Push(name, "item")

	name1 := queue.Name{
		Tenant: "local1",
		Queue:  "default",
	}
	queuePool.Push(name1, "item")

	resp, err := http.Get(server.URL + "/queue-manager/api/v1/queue")
	actual := validateBasics(resp, err, t)

	m := make(map[string][]string)
	json.Unmarshal(actual, &m)

	assert.Equal(t, []string{"local#default", "local1#default"}, m["queues"])

	queuePool.Pull(name)
	queuePool.Pull(name1)
}

func push(queuePool *queue.Queue, server *httptest.Server, t *testing.T) {
	client := &http.Client{}
	req, _ := http.NewRequest(http.MethodPut, server.URL+"/queue-manager/api/v1/queue/local/default/xpto", nil)
	resp, err := client.Do(req)
	actual := validateBasics(resp, err, t)

	m := make(map[string]int64)
	json.Unmarshal(actual, &m)

	assert.Equal(t, int64(1), m["position"])

	name := queue.Name{
		Tenant: "local",
		Queue:  "default",
	}
	queuePool.Pull(name)
}

func pull(queuePool *queue.Queue, server *httptest.Server, t *testing.T) {

	name := queue.Name{
		Tenant: "local",
		Queue:  "default",
	}
	queuePool.Push(name, "xpto")

	client := &http.Client{}
	req, _ := http.NewRequest(http.MethodDelete, server.URL+"/queue-manager/api/v1/queue/local/default", nil)
	resp, err := client.Do(req)
	actual := validateBasics(resp, err, t)

	m := make(map[string]string)
	json.Unmarshal(actual, &m)

	assert.Equal(t, "xpto", m["item"])

	resp, err = client.Do(req)
	actual = validateBasics(resp, err, t)

	m1 := make(map[string]bool)
	json.Unmarshal(actual, &m1)

	assert.Equal(t, true, m1["empty"])
}

func length(queuePool *queue.Queue, server *httptest.Server, t *testing.T) {

	name := queue.Name{
		Tenant: "local",
		Queue:  "default",
	}

	queuePool.Push(name, "item")

	resp, err := http.Get(server.URL + "/queue-manager/api/v1/queue/length/local/default")
	actual := validateBasics(resp, err, t)

	m := make(map[string]int64)
	json.Unmarshal(actual, &m)

	assert.Equal(t, int64(1), m["length"])

	queuePool.Pull(name)

	resp, err = http.Get(server.URL + "/queue-manager/api/v1/queue/length/local/default")
	actual = validateBasics(resp, err, t)

	json.Unmarshal(actual, &m)

	assert.Equal(t, int64(0), m["length"])
}

func indexOf(queuePool *queue.Queue, server *httptest.Server, t *testing.T) {

	name := queue.Name{
		Tenant: "local",
		Queue:  "default",
	}

	queuePool.Push(name, "xpto")

	resp, err := http.Get(server.URL + "/queue-manager/api/v1/queue/index/local/default/xpto")
	actual := validateBasics(resp, err, t)

	m := make(map[string]int64)
	json.Unmarshal(actual, &m)

	assert.Equal(t, int64(1), m["position"])

	queuePool.Pull(name)

	resp, err = http.Get(server.URL + "/queue-manager/api/v1/queue/index/local/default/xpto")
	actual = validateBasics(resp, err, t)

	json.Unmarshal(actual, &m)

	assert.Equal(t, int64(-1), m["position"])
}

func TestHandlers(t *testing.T) {

	local, _ := miniredis.Run()
	os.Setenv("REDIS_ADDR", local.Addr())

	handler, redis, _ := BootStrap()
	server := httptest.NewServer(handler)
	queuePool := queue.New(redis)

	defer redis.Close()
	defer server.Close()

	health(server, t)
	queuesPerTenant(queuePool, server, t)
	queues(queuePool, server, t)
	push(queuePool, server, t)
	pull(queuePool, server, t)
	length(queuePool, server, t)
	indexOf(queuePool, server, t)
}

func TestController(t *testing.T) {

	local, _ := miniredis.Run()

	rdb := redis.NewClient(&redis.Options{
		Addr: local.Addr(),
	})
	defer rdb.Close()

	router := mux.NewRouter()
	queuePool := queue.New(rdb)

	broadcaster := emitter.NewEmitter(&emitter.Options{
		Redis: rdb,
		Key:   "maindb",
	})
	defer broadcaster.Close()

	ctl := NewController(router, queuePool, broadcaster)

	name := queue.Name{
		Tenant: "local",
		Queue:  "default",
	}

	queuePool.Push(name, "test")
	ctl.BroadcastQueueInfo(name)
	ctl.BroadcastQueuePosition(name)
	queuePool.Push(name, "pull")
}

func TestAvgQueue(t *testing.T) {

	local, _ := miniredis.Run()

	rdb := redis.NewClient(&redis.Options{
		Addr: local.Addr(),
	})
	defer rdb.Close()

	router := mux.NewRouter()
	queuePool := queue.New(rdb)

	broadcaster := emitter.NewEmitter(&emitter.Options{
		Redis: rdb,
		Key:   "maindb",
	})
	defer broadcaster.Close()

	ctl := NewController(router, queuePool, broadcaster)

	name := queue.Name{
		Tenant: "local",
		Queue:  "default",
	}

	queuePool.SetWithExpire("realtime:avg-queue:local:default", "1131.5", -1)
	avg := ctl.GetAvgQueueTime(name)
	assert.Equal(t, float64(1131), avg)

	queuePool.SetWithExpire("realtime:avg-queue:local:default", "", -1)
	avg = ctl.GetAvgQueueTime(name)
	assert.Equal(t, float64(0), avg)

	queuePool.SetWithExpire("realtime:avg-queue:local:default", "0.0", -1)
	avg = ctl.GetAvgQueueTime(name)
	assert.Equal(t, float64(0), avg)

	queuePool.SetWithExpire("realtime:avg-queue:local:default", "-1.7", -1)
	avg = ctl.GetAvgQueueTime(name)
	assert.Equal(t, float64(0), avg)

	queuePool.SetWithExpire("realtime:avg-queue:local:default", "1.7", -1)
	avg = ctl.GetAvgQueueTime(name)
	assert.Equal(t, float64(1), avg)
}

func TestListerners(t *testing.T) {

	local, _ := miniredis.Run()

	rdb := redis.NewClient(&redis.Options{
		Addr: local.Addr(),
	})
	defer rdb.Close()

	router := mux.NewRouter()
	queuePool := queue.New(rdb)

	broadcaster := emitter.NewEmitter(&emitter.Options{
		Redis: rdb,
		Key:   "maindb",
	})
	defer broadcaster.Close()

	ctl := NewController(router, queuePool, broadcaster)

	payload, _ := json.Marshal(map[string]interface{}{
		"event":     "queue_push_at",
		"namespace": "/customer",
		"token":     "3v6D91",
		"room":      "localhost#Support",
		"socketId":  "/provider#bmDXut6YI99_6M9hBBBB",
		"score":     1620218948297561000,
	})
	ctl.ProcessQueueMessages(string(payload), "398409823042")
	name := queue.Name{
		Tenant: "localhost",
		Queue:  "Support",
	}
	assert.True(t, queuePool.Exists(name, "3v6D91"))

	payload, _ = json.Marshal(map[string]interface{}{
		"event":     "queue_pull",
		"namespace": "/customer",
		"room":      "localhost#Support",
		"socketId":  "/provider#bmDXut6YI99_6M9hBBBB",
	})
	ctl.ProcessQueueMessages(string(payload), "398409823042")
	assert.False(t, queuePool.Exists(name, "3v6D91"))

	payload, _ = json.Marshal(map[string]interface{}{
		"event":     "queue_push",
		"namespace": "/customer",
		"token":     "3v6D92",
		"room":      "localhost#Support",
		"socketId":  "/provider#bmDXut6YI99_6M9hBBBB",
	})
	ctl.ProcessQueueMessages(string(payload), "398409823042")
	assert.True(t, queuePool.Exists(name, "3v6D92"))

	payload, _ = json.Marshal(map[string]interface{}{
		"event":     "queue_push",
		"namespace": "/customer",
		"token":     "3v6D93",
		"room":      "localhost#Support",
		"socketId":  "/provider#bmDXut6YI99_6M9hBBBB",
		"expired":   true,
	})
	ctl.ProcessQueueMessages(string(payload), "398409823042")
	assert.False(t, queuePool.Exists(name, "3v6D93"))

	payload, _ = json.Marshal(map[string]interface{}{
		"event":     "queue_pull",
		"namespace": "/customer",
		"room":      "localhost#Fake",
		"socketId":  "/provider#bmDXut6YI99_6M9hBBBB",
	})
	ctl.ProcessQueueMessages(string(payload), "398409823042")

	payload, _ = json.Marshal(map[string]interface{}{
		"event":     "queue_get",
		"namespace": "/customer",
		"token":     "3v6D92",
		"room":      "localhost#Support",
		"socketId":  "/provider#bmDXut6YI99_6M9hBBBB",
	})
	ctl.ProcessQueueMessages(string(payload), "398409823042")
	assert.False(t, queuePool.Exists(name, "3v6D92"))

	payload, _ = json.Marshal(map[string]interface{}{
		"event":     "queue_get",
		"namespace": "/customer",
		"token":     "xxxxx",
		"room":      "localhost#Support",
		"socketId":  "/provider#bmDXut6YI99_6M9hBBBB",
	})
	ctl.ProcessQueueMessages(string(payload), "398409823042")

	payload, _ = json.Marshal(map[string]interface{}{
		"event":     "queue_disconnect",
		"namespace": "/customer",
		"token":     "zzzzzz",
		"room":      "localhost#Support",
		"socketId":  "/provider#bmDXut6YI99_6M9hBBBB",
	})
	ctl.ProcessQueueMessages(string(payload), "398409823042")

	assert.Equal(t, int64(0), rdb.Exists(getDisconnectedKey(name, "zzzzzz")).Val())

	payload, _ = json.Marshal(map[string]interface{}{
		"event":     "queue_push",
		"namespace": "/customer",
		"token":     "3v6D88",
		"room":      "localhost#Support",
		"socketId":  "/provider#bmDXut6YI99_6M9hBBBB",
	})
	ctl.ProcessQueueMessages(string(payload), "398409823042")
	assert.True(t, queuePool.Exists(name, "3v6D88"))

	payload, _ = json.Marshal(map[string]interface{}{
		"event":     "queue_disconnect",
		"namespace": "/customer",
		"token":     "3v6D88",
		"room":      "localhost#Support",
		"socketId":  "/provider#bmDXut6YI99_6M9hBBBB",
	})
	ctl.ProcessQueueMessages(string(payload), "398409823042")

	assert.Equal(t, int64(1), rdb.Exists(getDisconnectedKey(name, "3v6D88")).Val())

	ctl.AckAndDeleteMessage("streamName", "queueGroup", "398409823042")
	ctl.ProcessQueueExpired(strings.Split("__keyspace@0__:disconnected|localhost#Sales|2v6D90", "|"))

	payload, _ = json.Marshal(map[string]interface{}{
		"event":     "queue_push",
		"namespace": "/customer",
		"token":     "2v6D91",
		"room":      "localhost#Support",
		"socketId":  "/provider#bmDXut6YI99_6M9hBBBB",
	})

	ctl.ProcessQueueMessages(string(payload), "398409823042")
	assert.True(t, queuePool.Exists(name, "2v6D91"))

	ctl.ProcessQueueExpired(strings.Split("__keyspace@0__:disconnected|localhost#Support|2v6D91", "|"))
	assert.False(t, queuePool.Exists(name, "2v6D91"))
}

package main

import (
	"time"

	"rank-stream/queue"

	"github.com/go-redis/redis/v7"
)

// QueueStore abstracts ranked-queue and Redis operations used by the controller.
// *queue.Queue satisfies this interface.
type QueueStore interface {
	Status() string
	GetQueues(tenant string) []string
	GetAllQueues() []string
	GetQueueItems(name queue.Name) []string
	Push(name queue.Name, item string) int64
	PushWithScore(name queue.Name, entry queue.Entry) int64
	Pull(name queue.Name) (*queue.Entry, bool)
	Del(name queue.Name, item string) (*queue.Entry, bool)
	Len(name queue.Name) int64
	IndexOf(name queue.Name, item string) int64
	Exists(name queue.Name, item string) bool
	IsPresent(key string) bool
	Get(key string) string
	Remove(key string) bool
	SetWithExpire(key string, value string, ttl time.Duration) string
	Subscribe(topic string) *redis.PubSub
	PushStream(streamName string, message map[string]interface{}) (string, error)
	ReadStream(streamName string, consumerGroup string, start string) ([]redis.XStream, error)
	AckStream(streamName string, consumerGroup string, ID string) (int64, error)
	DelStream(streamName string, ID string) (int64, error)
}

package queue

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/go-redis/redis/v7"
	uuid "github.com/satori/go.uuid"
)

var consumerName string = uuid.NewV4().String()

// Queue -
type Queue struct {
	db *redis.Client
}

// Name -
type Name struct {
	Tenant string
	Queue  string
}

// Entry -
type Entry struct {
	Member string
	Score  float64
}

// Key -
func (n Name) Key() string {
	return fmt.Sprintf("%s#%s", n.Tenant, n.Queue)
}

// NewName -
func NewName(v string) Name {
	name := Name{}
	values := strings.Split(v, "#")
	if len(values) == 2 {
		name.Tenant = values[0]
		name.Queue = values[1]
	}
	return name
}

// New -
func New(cluster *redis.Client) *Queue {
	return &Queue{db: cluster}
}

// Status -
func (q *Queue) Status() string {
	return q.db.Ping().String()
}

// get -
func (q *Queue) Get(key string) string {
	return q.db.Get(key).Val()
}

func scanKeys(db *redis.Client, pattern string) []string {
	keys := make([]string, 0)
	iter := db.Scan(0, pattern, 0).Iterator()
	for iter.Next() {
		keys = append(keys, iter.Val())
	}
	return keys
}

// GetQueues - Per Tenant
func (q *Queue) GetQueues(tenant string) []string {
	keys := scanKeys(q.db, tenant+"#*")
	sort.Strings(keys)
	return keys
}

// GetAllQueues -
func (q *Queue) GetAllQueues() []string {
	keys := scanKeys(q.db, "*")
	sort.Strings(keys)
	return keys
}

// GetQueueItems -
func (q *Queue) GetQueueItems(name Name) []string {
	return q.db.ZRange(name.Key(), 0, -1).Val()
}

// PushWithScore -
func (q *Queue) PushWithScore(name Name, entry Entry) int64 {
	if q.Exists(name, entry.Member) {
		return q.IndexOf(name, entry.Member)
	}
	key := name.Key()
	q.db.ZAdd(key, &redis.Z{
		Score:  entry.Score,
		Member: entry.Member,
	}).Val()
	return q.IndexOf(name, entry.Member)
}

// Push -
func (q *Queue) Push(name Name, item string) int64 {
	return q.PushWithScore(name, Entry{
		Member: item,
		Score:  float64(q.db.Time().Val().UnixNano()),
	})
}

// Exists reports whether item is a member of the queue's sorted set.
func (q *Queue) Exists(name Name, item string) bool {
	key := name.Key()
	if q.db.Exists(key).Val() == 0 {
		return false
	}
	if err := q.db.ZScore(key, item).Err(); err == redis.Nil {
		return false
	} else if err != nil {
		return false
	}
	return true
}

// IsPresent reports whether a plain Redis key exists.
func (q *Queue) IsPresent(key string) bool {
	if result := q.db.Exists(key); result.Val() == 0 {
		return false
	}
	return true
}

// IndexOf -
func (q *Queue) IndexOf(name Name, item string) int64 {
	if q.Exists(name, item) {
		return q.db.ZRank(name.Key(), item).Val() + 1
	}
	return -1
}

// Len -
func (q *Queue) Len(name Name) int64 {
	return q.db.ZCount(name.Key(), "-inf", "+inf").Val()
}

// Pull -
func (q *Queue) Pull(name Name) (*Entry, bool) {
	items := q.db.ZPopMin(name.Key()).Val()
	if len(items) == 0 {
		return nil, false
	}
	item := items[0]
	return &Entry{
		Member: item.Member.(string),
		Score:  item.Score,
	}, true
}

// Del - Deletes an item
func (q *Queue) Del(name Name, item string) (*Entry, bool) {
	score := q.db.ZScore(name.Key(), item)
	ret := q.db.ZRem(name.Key(), item).Val()
	return &Entry{
		Member: item,
		Score:  score.Val(),
	}, ret == 1
}

// Subscribe -
func (q *Queue) Subscribe(topic string) *redis.PubSub {
	return q.db.PSubscribe(topic)
}

// SetWithExpire -
func (q *Queue) SetWithExpire(key string, value string, ttl time.Duration) string {
	cmd := q.db.Set(key, value, ttl)
	return cmd.Val()
}

// Remove -
func (q *Queue) Remove(key string) bool {
	if q.db.Exists(key).Val() == 1 {
		cmd := q.db.Del(key)
		return cmd.Val() == 1
	}
	return false
}

// ReadStream -
func (q *Queue) PushStream(streamName string, message map[string]interface{}) (string, error) {
	//StringCmd
	return q.db.XAdd(&redis.XAddArgs{
		Stream: streamName,
		ID:     "*",
		Values: message,
	}).Result()
}

// ReadStream -
func (q *Queue) ReadStream(streamName string, consumerGroup string, start string) ([]redis.XStream, error) {
	return q.db.XReadGroup(&redis.XReadGroupArgs{
		Streams:  []string{streamName, start},
		Group:    consumerGroup,
		Consumer: consumerName,
		Count:    1,
		Block:    0,
	}).Result()
}

// AckStream -
func (q *Queue) AckStream(streamName string, consumerGroup string, ID string) (int64, error) {
	return q.db.XAck(streamName, consumerGroup, ID).Result()
}

// DelStream -
func (q *Queue) DelStream(streamName string, ID string) (int64, error) {
	return q.db.XDel(streamName, ID).Result()
}

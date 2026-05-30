package queue

import (
	"testing"

	"github.com/alicebob/miniredis"
	"github.com/go-redis/redis/v7"
	"github.com/stretchr/testify/assert"
)

// TestKey -
func TestKey(t *testing.T) {
	name := Name{
		Tenant: "a",
		Queue:  "b",
	}
	key := name.Key()
	assert.Equal(t, key, "a#b")
}

func TestNewQueue(t *testing.T) {
	r := redis.NewClient(&redis.Options{})
	defer r.Close()
	q := New(r)
	assert.Equal(t, q.db, r)
}

func TestPushToHead(t *testing.T) {
	local, _ := miniredis.Run()
	db := redis.NewClient(&redis.Options{
		Addr: local.Addr(),
	})
	defer db.Close()
	q := New(db)
	assert.Equal(t, q.Status(), "ping: PONG")

	name := Name{
		Tenant: "a",
		Queue:  "b",
	}

	assert.Equal(t, q.Push(name, "item1"), int64(1))
	assert.Equal(t, q.Push(name, "item2"), int64(2))
	assert.Equal(t, q.Push(name, "item3"), int64(3))

	assert.Equal(t, q.PushWithScore(name, Entry{
		Member: "itemHead",
		Score:  float64(-1),
	}), int64(1))

	assert.Equal(t, q.IndexOf(name, "item1"), int64(2))
	assert.Equal(t, q.IndexOf(name, "item2"), int64(3))
	assert.Equal(t, q.IndexOf(name, "item3"), int64(4))

	item, found := q.Del(name, "itemHead")
	assert.True(t, found)
	assert.Equal(t, float64(-1), item.Score)
	assert.Equal(t, "itemHead", item.Member)

	assert.Equal(t, q.IndexOf(name, "item1"), int64(1))
	assert.Equal(t, q.IndexOf(name, "item2"), int64(2))
	assert.Equal(t, q.IndexOf(name, "item3"), int64(3))
}

func TestOps(t *testing.T) {
	local, _ := miniredis.Run()
	db := redis.NewClient(&redis.Options{
		Addr: local.Addr(),
	})
	defer db.Close()
	q := New(db)
	assert.Equal(t, q.Status(), "ping: PONG")

	name := Name{
		Tenant: "a",
		Queue:  "b",
	}

	assert.Equal(t, NewName("a#b").Key(), name.Key())

	assert.Equal(t, q.Len(name), int64(0))
	assert.False(t, q.Exists(name, "item"))

	assert.Equal(t, q.Push(name, "item"), int64(1))
	assert.True(t, q.Exists(name, "item"))
	assert.Equal(t, q.Len(name), int64(1))
	assert.Equal(t, q.Push(name, "item"), int64(1))

	assert.False(t, q.Exists(name, "noitem"))
	assert.Equal(t, q.Push(name, "noitem"), int64(2))
	assert.Equal(t, q.Len(name), int64(2))
	assert.Equal(t, []string{"item", "noitem"}, q.GetQueueItems(name))

	assert.Equal(t, q.IndexOf(name, "item"), int64(1))
	assert.Equal(t, q.IndexOf(name, "noitem"), int64(2))
	assert.Equal(t, q.IndexOf(name, "itemno"), int64(-1))

	assert.Equal(t, []string{"a#b"}, q.GetAllQueues())
	assert.Equal(t, []string{"a#b"}, q.GetQueues("a"))

	name1 := Name{
		Tenant: "a",
		Queue:  "bb",
	}

	assert.Equal(t, q.Push(name1, "item"), int64(1))
	assert.Equal(t, []string{"a#b", "a#bb"}, q.GetAllQueues())
	assert.Equal(t, []string{"a#b", "a#bb"}, q.GetQueues("a"))

	name2 := Name{
		Tenant: "c",
		Queue:  "bb",
	}

	assert.Equal(t, q.Push(name2, "item"), int64(1))
	assert.Equal(t, []string{"a#b", "a#bb", "c#bb"}, q.GetAllQueues())
	assert.Equal(t, []string{"c#bb"}, q.GetQueues("c"))

	item1, found := q.Pull(name)
	assert.True(t, found)
	assert.Equal(t, item1.Member, "item")

	item2, found := q.Pull(name)
	assert.True(t, found)
	assert.Equal(t, item2.Member, "noitem")

	item3, found := q.Pull(name)
	assert.False(t, found)
	assert.Nil(t, item3)

	assert.Equal(t, q.Len(name), int64(0))

	ps := q.Subscribe("test")
	assert.Nil(t, ps.Ping())

}

func TestSamePrefixKey(t *testing.T) {
	local, _ := miniredis.Run()
	db := redis.NewClient(&redis.Options{
		Addr: local.Addr(),
	})
	defer db.Close()
	q := New(db)
	assert.Equal(t, q.Status(), "ping: PONG")

	name := Name{
		Tenant: "a",
		Queue:  "b",
	}
	assert.Equal(t, q.Push(name, "item"), int64(1))

	name1 := Name{
		Tenant: "aa",
		Queue:  "b",
	}
	assert.Equal(t, q.Push(name1, "item"), int64(1))

	assert.Equal(t, []string{"a#b", "aa#b"}, q.GetAllQueues())
	assert.Equal(t, []string{"a#b"}, q.GetQueues("a"))
	assert.Equal(t, []string{"aa#b"}, q.GetQueues("aa"))
}

func TestExistsZeroScore(t *testing.T) {
	local, _ := miniredis.Run()
	db := redis.NewClient(&redis.Options{
		Addr: local.Addr(),
	})
	defer db.Close()
	q := New(db)

	name := Name{
		Tenant: "a",
		Queue:  "b",
	}

	assert.Equal(t, int64(1), q.PushWithScore(name, Entry{
		Member: "zero-score",
		Score:  0,
	}))
	assert.True(t, q.Exists(name, "zero-score"))
	assert.Equal(t, int64(1), q.IndexOf(name, "zero-score"))
}

func TestPushStream(t *testing.T) {
	local, _ := miniredis.Run()
	db := redis.NewClient(&redis.Options{
		Addr: local.Addr(),
	})
	defer db.Close()
	q := New(db)
	assert.Equal(t, q.Status(), "ping: PONG")
}

package emitter

import (
	"bytes"
	"encoding/binary"
	"math"
	"testing"

	"github.com/alicebob/miniredis"
	"github.com/go-redis/redis/v7"
	"github.com/stretchr/testify/assert"
)

func TestEmitter(t *testing.T) {

	local, _ := miniredis.Run()
	db := redis.NewClient(&redis.Options{
		Addr: local.Addr(),
	})
	defer db.Close()

	broadcaster := NewEmitter(&Options{
		Redis: db,
		Key:   "test",
	})
	defer broadcaster.Close()

	assert.Equal(t, "PONG", broadcaster.redis.Ping().Val())

	data, err := broadcaster.Emit("test", "test")

	assert.Nil(t, err)
	assert.Empty(t, data.rooms)

	data, err = broadcaster.In("room1").Of("/patient").Emit("test1", "test1")

	assert.Nil(t, err)
	assert.Empty(t, data.rooms)

	broadcaster.rooms = []string{"room1"}

	data, err = broadcaster.In("room1").Of("/patient").Emit("test1", "test1")

	assert.Nil(t, err)
	assert.Empty(t, data.rooms)

	data, err = broadcaster.To("room1").Of("/customer").Emit("test2", "test2")

	assert.Nil(t, err)
	assert.Empty(t, data.rooms)

	buf := new(bytes.Buffer)
	var pi float64 = math.Pi
	binary.Write(buf, binary.LittleEndian, pi)

	data, err = broadcaster.To("room1").Of("/customer").Emit(buf.Bytes)
	assert.NotNil(t, err)

	data, err = broadcaster.To("room1").Of("/customer").Emit(buf.Bytes())
	assert.Nil(t, err)
}

func TestFlags(t *testing.T) {
	broadcaster := NewEmitter(&Options{})

	assert.Nil(t, broadcaster.flags["json"])
	broadcaster.JSON()
	assert.True(t, broadcaster.flags["json"].(bool))

	assert.Nil(t, broadcaster.flags["volatile"])
	broadcaster.Volatile()
	assert.True(t, broadcaster.flags["volatile"].(bool))

	assert.Nil(t, broadcaster.flags["broadcast"])
	broadcaster.Broadcast()
	assert.True(t, broadcaster.flags["broadcast"].(bool))

}

func TestHasBin(t *testing.T) {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, math.Pi)
	assert.True(t, hasBin(buf.Bytes()))
	assert.True(t, hasBin(*buf))
	assert.False(t, hasBin())
	assert.False(t, hasBin("notBin"))
	assert.True(t, hasBin([]interface{}{buf.Bytes()}))
	assert.True(t, hasBin([]interface{}{"notBin", buf.Bytes()}))
	assert.True(t, hasBin(map[string]interface{}{"buf": buf.Bytes()}))
}

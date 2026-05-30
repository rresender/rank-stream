package emitter

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/go-redis/redis/v7"
	log "github.com/sirupsen/logrus"
	"gopkg.in/vmihailenco/msgpack.v2"
)

const (
	// https://github.com/socketio/socket.io-parser/blob/master/index.js
	gEvent       = 2
	gBinaryEvent = 5
	uid          = "emitter"
)

// Options ...
type Options struct {
	// the name of the key to pub/sub events on as prefix (socket.io)
	Key string
	// redis client
	Redis *redis.Client
}

// Emitter Socket.IO redis base emitter
type Emitter struct {
	redis  *redis.Client
	prefix string
	rooms  []string
	flags  map[string]interface{}
	sync.RWMutex
}

// NewEmitter Emitter constructor
func NewEmitter(opts *Options) *Emitter {
	emitter := &Emitter{}
	emitter.redis = opts.Redis
	emitter.prefix = "socket.io"
	if opts.Key != "" {
		emitter.prefix = opts.Key
	}
	emitter.rooms = make([]string, 0)
	emitter.flags = make(map[string]interface{})
	return emitter
}

// Close releases emitter resources. The Redis client is owned by the caller
// and is not closed here.
func (e *Emitter) Close() {}

// Emit Send the packet
func (e *Emitter) Emit(data ...interface{}) (*Emitter, error) {
	e.Lock()
	defer e.Unlock()
	packet := make(map[string]interface{})
	packet["type"] = gEvent
	if hasBin(data...) {
		packet["type"] = gBinaryEvent
	}

	packet["data"] = data

	packet["nsp"] = "/"

	if nsp, ok := e.flags["nsp"]; ok {
		packet["nsp"] = nsp
		delete(e.flags, "nsp")
	}

	opts := map[string]interface{}{
		"rooms": e.rooms,
		"flags": e.flags,
	}

	chn := fmt.Sprintf("%s#%s#", e.prefix, packet["nsp"])

	if json, err := json.Marshal([]interface{}{uid, packet, opts}); err == nil {
		log.Debugf("payload to emit %s", string(json))
	}

	buf, err := msgpack.Marshal([]interface{}{uid, packet, opts})
	if err != nil {
		return nil, err
	}

	if len(e.rooms) > 0 {
		for _, room := range e.rooms {
			chnRoom := fmt.Sprintf("%s%s#", chn, room)
			e.redis.Publish(chnRoom, string(buf))
		}
	} else {
		e.redis.Publish(chn, string(buf))
	}

	e.rooms = make([]string, 0)
	e.flags = make(map[string]interface{})
	return e, nil
}

// In Limit emission to a certain `room`
func (e *Emitter) In(room string) *Emitter {
	e.Lock()
	defer e.Unlock()
	for _, r := range e.rooms {
		if r == room {
			return e
		}
	}
	e.rooms = append(e.rooms, room)
	return e
}

// To Limit emission to a certain `room`
func (e *Emitter) To(room string) *Emitter {
	return e.In(room)
}

// Of Limit emission to certain `namespace`
func (e *Emitter) Of(namespace string) *Emitter {
	e.Lock()
	defer e.Unlock()
	e.flags["nsp"] = namespace
	return e
}

// JSON flag
func (e *Emitter) JSON() *Emitter {
	e.Lock()
	defer e.Unlock()
	e.flags["json"] = true
	return e
}

// Volatile flag
func (e *Emitter) Volatile() *Emitter {
	e.Lock()
	defer e.Unlock()
	e.flags["volatile"] = true
	return e
}

// Broadcast flag
func (e *Emitter) Broadcast() *Emitter {
	e.Lock()
	defer e.Unlock()
	e.flags["broadcast"] = true
	return e
}

func hasBin(data ...interface{}) bool {
	if data == nil {
		return false
	}

	for _, d := range data {
		if hasBinValue(d) {
			return true
		}
	}

	return false
}

func hasBinValue(d interface{}) bool {
	switch res := d.(type) {
	case []byte:
		return true
	case bytes.Buffer:
		return true
	case *bytes.Buffer:
		return true
	case []interface{}:
		for _, each := range res {
			if hasBinValue(each) {
				return true
			}
		}
		return false
	case map[string]interface{}:
		for _, val := range res {
			if hasBinValue(val) {
				return true
			}
		}
		return false
	default:
		return false
	}
}

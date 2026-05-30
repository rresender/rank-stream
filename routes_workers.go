package main

import (
	"strings"
	"time"

	"rank-stream/queue"

	log "github.com/sirupsen/logrus"
)

func streamValueAsString(v interface{}) (string, bool) {
	switch s := v.(type) {
	case string:
		return s, true
	case []byte:
		return string(s), true
	default:
		return "", false
	}
}

func (c *Controller) ProcessQueueExpired(data []string) {
	if len(data) != 3 {
		return
	}
	event := data[0]
	key := data[1]
	item := data[2]
	log.Debugf("Event: [%s] key:[%s] item: [%s]", event, key, item)
	name := queue.NewName(key)
	tenant, room := splitRoom(key)

	if c.store.Exists(name, item) {
		_, found := c.store.Del(name, item)
		if found {
			go c.BroadcastQueuePosition(name)
		}
	}
	if eQueueTimeoutEvent == event {
		log.Infof("Queue: %v for item: %v has been timed out", key, item)
		c.broadcaster.In(item).Of(customerNamespace).Emit(eQueueNotPushedEvent, map[string]interface{}{
			"reason": "expired",
		})
		disconnectedKey := getDisconnectedKey(name, item)
		if c.store.IsPresent(disconnectedKey) {
			c.store.Remove(disconnectedKey)
		}
		c.pushQueueInfoToStream(queueToReportStream, "queue_event", &QueueInfoMessage{
			Category: "dequeue", Tenant: tenant, Room: room, Token: item, Timestamp: time.Now().Unix(),
		})
	} else {
		timeoutKey := getQueueTimeoutKey(name, item)
		if c.store.IsPresent(timeoutKey) {
			log.Infof("Queue: %v for item: %v has not been timed out. Removing it", key, item)
			c.store.Remove(timeoutKey)
			c.pushQueueInfoToStream(queueToReportStream, "queue_event", &QueueInfoMessage{
				Category: "dequeue", Tenant: tenant, Room: room, Token: item, Timestamp: time.Now().Unix(),
			})
		}
	}
}

// ListenKeyEvents watches Redis keyspace notifications for disconnect timeouts.
func (c *Controller) ListenKeyEvents() {
	pubsub := c.store.Subscribe("__keyspace@0__:disconnected*")
	for {
		func() {
			in, err := pubsub.ReceiveMessage()
			if err != nil {
				log.Error("ListenKeyEvents - Failed to get feedback ", err)
				c.store.Status()
				time.Sleep(retryTimeToReconnectRedis)
				return
			}
			if in == nil {
				log.Error("ListenKeyEvents - Message paylod is nil")
				return
			}
			log.Infof("Event has been received: channel: [%v] pattern: [%v] payload: [%v]", in.Channel, in.Pattern, in.Payload)
			if in.Payload == "expired" {
				data := strings.Split(in.Channel, "|")
				go c.ProcessQueueExpired(data)
			}
		}()
	}
}

func (c *Controller) AckAndDeleteMessage(streamName string, consumerGroup string, ID string) {
	acked, err := c.store.AckStream(streamName, consumerGroup, ID)
	log.Infof("ID: [%s] acked: %d", ID, acked)
	if err != nil {
		log.Errorf("ID: [%s] error while acking: %+v", ID, err)
	}
	deleted, err := c.store.DelStream(streamName, ID)
	log.Infof("ID: [%s] deleted: %d", ID, deleted)
	if err != nil {
		log.Errorf("ID: [%s] error while deleting: %+v", ID, err)
	}
}

// ListenEvents consumes queue commands from a Redis stream consumer group.
func (c *Controller) ListenEvents(streamName string, consumerGroup string) {
	for {
		func() {
			log.Infof("Listening for events from stream: [%s] and group: [%s]", streamName, consumerGroup)

			streams, err := c.store.ReadStream(streamName, consumerGroup, ">")
			if err != nil {
				log.Errorf("err on consume events: %+v", err)
				return
			}
			if len(streams) == 0 {
				return
			}
			for _, stream := range streams[0].Messages {
				streamID := stream.ID
				for k, v := range stream.Values {
					payload, ok := streamValueAsString(v)
					if !ok {
						log.Errorf("ID: [%s] event [%s]: unexpected value type %T", streamID, k, v)
						continue
					}
					go func(event, payload, id string) {
						log.Infof("ID: [%s] on event: [%s] payload: [%s]", id, event, payload)
						c.ProcessQueueMessages(payload, id)
						c.AckAndDeleteMessage(streamName, consumerGroup, id)
					}(k, payload, streamID)
				}
			}
		}()
	}
}

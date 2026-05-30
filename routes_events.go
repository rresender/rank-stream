package main

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"rank-stream/queue"

	log "github.com/sirupsen/logrus"
)

func decode(payload string) Packet {
	log.Infof("Event has been received: %s", payload)
	var packet Packet
	if err := json.Unmarshal([]byte(payload), &packet); err != nil {
		log.Error(err)
	}
	return packet
}

func getDisconnectedKey(name queue.Name, token string) string {
	return fmt.Sprintf("disconnected|%s|%s", name.Key(), token)
}

func getQueueTimeoutKey(name queue.Name, token string) string {
	return fmt.Sprintf("disconnected:timeout|%s|%s", name.Key(), token)
}

func getAvgQueueKey(name queue.Name) string {
	return fmt.Sprintf("realtime:avg-queue:%s:%s", name.Tenant, name.Queue)
}

func (c *Controller) GetAvgQueueTime(name queue.Name) float64 {
	val := c.store.Get(getAvgQueueKey(name))
	if val == "" {
		return 0
	}
	time, err := strconv.ParseFloat(val, 32)
	if err != nil || time < 0 {
		return 0
	}
	return math.Floor(time)
}

func splitRoom(room string) (tenant, roomName string) {
	parts := strings.Split(room, "#")
	if len(parts) != 2 {
		return room, ""
	}
	return parts[0], parts[1]
}

func (c *Controller) pushQueueInfoToStream(streamName string, keyMsg string, queueInfoMsg *QueueInfoMessage) (string, error) {
	message, err := TypeToInterface(queueInfoMsg)
	if err != nil {
		log.Errorf("Error converting [%+v] to map[string]interface{}. Error message: %s", queueInfoMsg, err)
		return "", err
	}
	id, err := c.store.PushStream(streamName, map[string]interface{}{keyMsg: message})
	if err != nil {
		log.Errorf("Error pushing a message [%+v] to the stream [%s]. Error message: %s", message, queueToReportStream, err)
		return id, err
	}
	log.Infof("Message [%+v] was sent to the stream [%s].", message, queueToReportStream)
	return id, nil
}

// ProcessQueueMessages handles inbound queue events from the Redis stream.
// See also ProcessQueueExpired for expired elements in queue.
func (c *Controller) ProcessQueueMessages(payload string, ID string) {
	packet := decode(payload)
	name := queue.NewName(packet.Room)
	tenant, room := splitRoom(packet.Room)

	switch packet.Event {
	case eQueuePushAtEvent:
		c.store.Remove(getDisconnectedKey(name, packet.Token))
		c.store.PushWithScore(name, queue.Entry{
			Member: packet.Token,
			Score:  packet.Score,
		})
		go func() {
			c.BroadcastQueuePosition(name)
			c.pushQueueInfoToStream(queueToReportStream, "queue_event", &QueueInfoMessage{
				Category: "enqueue", Tenant: tenant, Room: room, Token: packet.Token, Timestamp: time.Now().Unix(),
			})
		}()
	case eQueuePushEvent:
		c.store.Remove(getDisconnectedKey(name, packet.Token))
		if packet.Expired {
			if !c.store.Exists(name, packet.Token) {
				c.broadcaster.In(packet.Token).Of(customerNamespace).Emit(eQueueNotPushedEvent, map[string]interface{}{
					"reason": "expired",
				})
				break
			}
		}
		timeoutKey := getQueueTimeoutKey(name, packet.Token)
		if len(packet.TTL) > 0 && !c.store.IsPresent(timeoutKey) {
			c.store.SetWithExpire(timeoutKey, "timeout", TimeoutInSeconds(packet.TTL))
		}
		qpos := c.store.Push(name, packet.Token)
		avgqueue := c.GetAvgQueueTime(name)
		c.broadcaster.In(packet.Token).Of(customerNamespace).Emit(eQueuePushedEvent, map[string]interface{}{
			"item":     packet.Token,
			"position": qpos,
			"avgqueue": avgqueue,
		})
		go func() {
			c.BroadcastQueuePosition(name)
			c.pushQueueInfoToStream(queueToReportStream, "queue_event", &QueueInfoMessage{
				Category: "enqueue", Tenant: tenant, Room: room, Token: packet.Token, Timestamp: time.Now().Unix(),
			})
		}()

	case eQueuePullEvent:
		c.store.Remove(getQueueTimeoutKey(name, packet.Token))
		item, found := c.store.Pull(name)
		if found {
			body := map[string]interface{}{
				"item":  item.Member,
				"type":  "pull",
				"score": item.Score,
				"id":    packet.ID,
			}
			c.broadcaster.In(packet.ID).Of(providerNamespace).Emit(eQueuePulledEvent, body)
			c.broadcaster.In(name.Key()).Of(providerNamespace).Emit(eQueueItemRemovedEvent, body)
			go c.BroadcastQueuePosition(name)
		} else {
			c.broadcaster.In(packet.ID).Of(providerNamespace).Emit(eQueueEmptyEvent, map[string]interface{}{
				"empty": true,
				"item":  packet.Token,
				"type":  "pull",
				"id":    packet.ID,
			})
		}
	case eQueueGetEvent:
		c.store.Remove(getQueueTimeoutKey(name, packet.Token))
		item, found := c.store.Del(name, packet.Token)
		if found {
			body := map[string]interface{}{
				"item":  item.Member,
				"type":  "get",
				"score": item.Score,
				"id":    packet.ID,
			}
			c.broadcaster.In(packet.ID).Of(providerNamespace).Emit(eQueueRemovedEvent, body)
			c.broadcaster.In(name.Key()).Of(providerNamespace).Emit(eQueueItemRemovedEvent, body)
			go func() {
				c.BroadcastQueuePosition(name)
				c.pushQueueInfoToStream(queueToReportStream, "queue_event", &QueueInfoMessage{
					Category: "dequeue", Tenant: tenant, Room: room, Token: packet.Token, Timestamp: time.Now().Unix(),
				})
			}()
		} else {
			c.broadcaster.In(packet.ID).Of(providerNamespace).Emit(eQueueNotFoundEvent, map[string]interface{}{
				"notfound": true,
				"item":     packet.Token,
				"type":     "get",
				"id":       packet.ID,
			})
		}
	case eQueueDisconnectEvent:
		if c.store.Exists(name, packet.Token) {
			c.store.SetWithExpire(getDisconnectedKey(name, packet.Token), "disconnect", MaxQueueTimeoutInSeconds())
		}
	}
	log.Infof("ID: [%s] processed: %+v", ID, packet)
}

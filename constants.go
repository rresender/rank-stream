package main

import "time"

const prefix = "maindb"

const eQueuePositionEvent = "queue_position"
const eQueueLenghtEvent = "queue_length" // stable wire name; typo kept for Socket.IO clients
const eQueuePushEvent = "queue_push"
const eQueuePushedEvent = "queue_pushed"
const eQueueNotPushedEvent = "queue_notpushed"
const eQueuePushAtEvent = "queue_push_at"
const eQueuePullEvent = "queue_pull"
const eQueuePulledEvent = "queue_pulled"
const eQueueEmptyEvent = "queue_empty"
const eQueueGetEvent = "queue_get"
const eQueueRemovedEvent = "queue_removed"
const eQueueItemRemovedEvent = "queue_item_removed"
const eQueueNotFoundEvent = "queue_notfound"
const eQueueDisconnectEvent = "queue_disconnect"
const eQueueTimeoutEvent = "__keyspace@0__:disconnected:timeout"

const customerNamespace = "/customer"
const providerNamespace = "/provider"

const retryTimeToReconnectRedis = 5 * time.Second

// Packet is the JSON payload received from the queue event stream.
type Packet struct {
	Event     string  `json:"event"`
	Token     string  `json:"token"`
	Namespace string  `json:"namespace"`
	Room      string  `json:"room"`
	ID        string  `json:"socketId"`
	Score     float64 `json:"score"`
	Expired   bool    `json:"expired"`
	TTL       string  `json:"ttl"`
}

// QueueInfoMessage is sent to the reporting stream on enqueue/dequeue events.
type QueueInfoMessage struct {
	Category  string
	Tenant    string
	Room      string
	Token     string
	Timestamp int64
}

package main

import (
	"rank-stream/emitter"
	"rank-stream/queue"

	"github.com/gorilla/mux"
)

// Controller wires HTTP routes, stream workers, and realtime broadcasts.
type Controller struct {
	router      *mux.Router
	api         *mux.Router
	broadcaster *emitter.Emitter
	store       QueueStore
}

// NewController creates a controller. queuePool must satisfy QueueStore.
func NewController(router *mux.Router, queuePool *queue.Queue, broadcaster *emitter.Emitter) *Controller {
	api := router.PathPrefix("/queue-manager/api/v1").Subrouter()
	return &Controller{
		router:      router,
		api:         api,
		broadcaster: broadcaster,
		store:       queuePool,
	}
}

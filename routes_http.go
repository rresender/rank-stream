package main

import (
	"encoding/json"
	"net/http"

	"rank-stream/queue"

	"github.com/gorilla/mux"
	log "github.com/sirupsen/logrus"
)

// Health - Health check / Check redis connection
func writeJSON(w http.ResponseWriter, status int, payload interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		log.Errorf("failed to encode JSON response: %v", err)
	}
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

func (c *Controller) Health() {
	h := Handler{
		Route: func(r *mux.Route) {
			r.Path("/health").Methods(http.MethodGet)
		},
		Func: func(w http.ResponseWriter, r *http.Request) {
			writeJSON(w, http.StatusOK, map[string]string{"status": c.store.Status()})
		},
	}
	h.AddRoute(c.router)
}

// QueuesPerTenant - Get all queues from the given tenant
func (c *Controller) QueuesPerTenant() {
	h := Handler{
		Route: func(r *mux.Route) {
			r.Path("/queue/{tenant}").Methods(http.MethodGet)
		},
		Func: func(w http.ResponseWriter, r *http.Request) {
			vars := mux.Vars(r)
			tenant := vars["tenant"]
			keys := c.store.GetQueues(tenant)
			writeJSON(w, http.StatusOK, map[string][]string{"queues": keys})
		},
	}
	h.AddRoute(c.api)
}

// Queues - Get all queues
func (c *Controller) Queues() {
	h := Handler{
		Route: func(r *mux.Route) {
			r.Path("/queue").Methods(http.MethodGet)
		},
		Func: func(w http.ResponseWriter, r *http.Request) {
			keys := c.store.GetAllQueues()
			writeJSON(w, http.StatusOK, map[string][]string{"queues": keys})
		},
	}
	h.AddRoute(c.api)
}

// Push the given item into the given queue from the given tenant
func (c *Controller) Push() {
	h := Handler{
		Route: func(r *mux.Route) {
			r.Path("/queue/{tenant}/{name}/{item}").Methods(http.MethodPut)
		},
		Func: func(w http.ResponseWriter, r *http.Request) {
			vars := mux.Vars(r)
			name := GetQueueKey(vars)
			item := vars["item"]

			qpos := c.store.Push(name, item)

			writeJSON(w, http.StatusOK, map[string]int64{"position": qpos})
			go c.BroadcastQueueInfo(name)
		},
	}
	h.AddRoute(c.api)
}

// Pull the head item from the given queue from the given tenant
func (c *Controller) Pull() {
	h := Handler{
		Route: func(r *mux.Route) {
			r.Path("/queue/{tenant}/{name}").Methods(http.MethodDelete)
		},
		Func: func(w http.ResponseWriter, r *http.Request) {
			vars := mux.Vars(r)
			name := GetQueueKey(vars)
			item, found := c.store.Pull(name)
			if found {
				writeJSON(w, http.StatusOK, map[string]string{"item": item.Member})
				go c.BroadcastQueueInfo(name)
				go c.BroadcastQueuePosition(name)
				return
			}
			writeJSON(w, http.StatusOK, map[string]bool{"empty": true})
		},
	}
	h.AddRoute(c.api)
}

// Length - Get the length from the given queue from the given tenant
func (c *Controller) Length() {
	h := Handler{
		Route: func(r *mux.Route) {
			r.Path("/queue/length/{tenant}/{name}").Methods(http.MethodGet)
		},
		Func: func(w http.ResponseWriter, r *http.Request) {
			vars := mux.Vars(r)
			name := GetQueueKey(vars)
			writeJSON(w, http.StatusOK, map[string]int64{"length": c.store.Len(name)})
		},
	}
	h.AddRoute(c.api)
}

// IndexOf - Get the position of the given item from the given queue from the given tenant
func (c *Controller) IndexOf() {
	h := Handler{
		Route: func(r *mux.Route) {
			r.Path("/queue/index/{tenant}/{name}/{item}").Methods(http.MethodGet)
		},
		Func: func(w http.ResponseWriter, r *http.Request) {
			vars := mux.Vars(r)
			name := GetQueueKey(vars)
			item := vars["item"]
			writeJSON(w, http.StatusOK, map[string]int64{"position": c.store.IndexOf(name, item)})
		},
	}
	h.AddRoute(c.api)
}

// BroadcastQueueInfo notifies providers of the current queue length.
func (c *Controller) BroadcastQueueInfo(name queue.Name) {
	c.broadcaster.In(name.Key()).Of(providerNamespace).Emit(eQueueLenghtEvent, map[string]interface{}{
		"queue":  name.Queue,
		"length": c.store.Len(name),
	})
}

// BroadcastQueuePosition notifies each queued item of its current position.
func (c *Controller) BroadcastQueuePosition(name queue.Name) {
	items := c.store.GetQueueItems(name)
	avgqueue := c.GetAvgQueueTime(name)
	for _, item := range items {
		position := c.store.IndexOf(name, item)
		c.broadcaster.In(item).Of(customerNamespace).Emit(eQueuePositionEvent, map[string]interface{}{
			"item":     item,
			"position": position,
			"avgqueue": avgqueue,
		})
	}
}

package main

import (
	"crypto/tls"
	"fmt"
	"net/http"
	"rank-stream/emitter"
	"rank-stream/queue"
	"strings"

	"github.com/go-redis/redis/v7"
	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	log "github.com/sirupsen/logrus"
)

var (
	queueStream         string = GetQueueStream()
	queueGroup          string = GetQueueGroup()
	queueToReportStream string = GetQueueToReportStream()
	queueToReportGroup  string = GetQueueToReportGroup()
)

// BootStrap -
func BootStrap() (http.Handler, *redis.Client, *emitter.Emitter) {

	config := NewRedisConfig()

	opt := redis.Options{
		Addr:     config.Addr,
		Password: config.Password,
		DB:       0,
	}

	if config.Tls {
		opt.TLSConfig = &tls.Config{InsecureSkipVerify: true}
	}

	rdb := redis.NewClient(&opt)

	status := rdb.Ping()

	if status.Err() != nil {
		log.Fatalf("Could not reach redis server: %s status: %s", config.Addr, status)
	}

	log.Infof("Connected to Redis server: %s status %s", config.Addr, status)

	broadcaster := emitter.NewEmitter(&emitter.Options{
		Redis: rdb,
		Key:   prefix,
	})

	if _, err := rdb.XGroupCreateMkStream(queueStream, queueGroup, "0").Result(); err != nil {
		if !strings.Contains(fmt.Sprint(err), "BUSYGROUP") {
			log.Errorf(`Error creating the Consumer Group "%+v" for Stream "%+v"`, queueGroup, queueStream)
		}
	}

	if _, err := rdb.XGroupCreateMkStream(queueToReportStream, queueToReportGroup, "0").Result(); err != nil {
		if !strings.Contains(fmt.Sprint(err), "BUSYGROUP") {
			log.Errorf(`Error creating the Consumer Group "%+v" for Stream "%+v"`, queueToReportGroup, queueToReportStream)
		}
	}

	router := mux.NewRouter()

	queuePool := queue.New(rdb)

	ctl := NewController(router, queuePool, broadcaster)

	ctl.Health()
	ctl.QueuesPerTenant()
	ctl.Queues()
	ctl.Push()
	ctl.Pull()
	ctl.IndexOf()
	ctl.Length()

	go ctl.ListenEvents(queueStream, queueGroup)
	go ctl.ListenKeyEvents()
	return handlers.LoggingHandler(log.StandardLogger().Out, router), rdb, broadcaster
}

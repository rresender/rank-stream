package main

import (
	"net/http"

	log "github.com/sirupsen/logrus"
)

func init() {
	InitLogging()
}

func main() {
	handler, redis, _ := BootStrap()
	defer redis.Close()
	port := GetPort()
	log.Infof("Starting Queue Manager Service on port %s", port)
	log.Fatal(http.ListenAndServe(port, handler))
}

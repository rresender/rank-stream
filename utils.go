package main

import (
	"encoding/json"
	"fmt"
	"os"
	"rank-stream/queue"
	"strconv"
	"time"

	log "github.com/sirupsen/logrus"
)

// RedisConfig -
type RedisConfig struct {
	Addr     string
	Password string
	Tls      bool
}

// GetQueueKey -
func GetQueueKey(vars map[string]string) queue.Name {
	return queue.Name{
		Tenant: vars["tenant"],
		Queue:  vars["name"],
	}
}

// RedisConfig -
func NewRedisConfig() *RedisConfig {
	var found bool

	redisAddr, found := os.LookupEnv("REDIS_ADDR")
	if !found {
		redisAddr = "localhost:6379"
	}

	redisPassword, found := os.LookupEnv("REDIS_PASSWORD")
	if !found {
		redisPassword = ""
	}

	s, _ := os.LookupEnv("REDIS_TLS")
	redisTls, _ := strconv.ParseBool(s)

	return &RedisConfig{
		Addr:     redisAddr,
		Password: redisPassword,
		Tls:      redisTls,
	}
}

func GetPort() string {
	port, found := os.LookupEnv("PORT")
	if !found {
		port = "7000"
	}
	return fmt.Sprintf(":%s", port)
}

// MaxQueueTimeoutInSeconds -
func TimeoutInSeconds(value string) time.Duration {
	timeout, _ := strconv.Atoi(value)
	return time.Duration(timeout) * time.Second
}

func MaxQueueTimeoutInSeconds() time.Duration {
	max := 600 * time.Second
	value, found := os.LookupEnv("MAX_QUEUE_TIME")
	if !found {
		return max
	}
	return TimeoutInSeconds(value)
}

// InitLogging -
func InitLogging() {
	slvl, ok := os.LookupEnv("LOG_LEVEL")
	if !ok {
		slvl = log.InfoLevel.String()
	}
	lvl, err := log.ParseLevel(slvl)
	if err != nil {
		lvl = log.InfoLevel
	}
	log.SetLevel(lvl)
	formatter := &log.TextFormatter{
		FullTimestamp: true,
	}
	log.SetFormatter(formatter)
}

func GetQueueGroup() string {
	queueGroup, found := os.LookupEnv("QUEUE_GROUP")
	if !found {
		queueGroup = "queue_group"
	}
	return queueGroup
}

func GetQueueStream() string {
	queueStream, found := os.LookupEnv("QUEUE_STREAM")
	if !found {
		queueStream = "queue_stream"
	}
	return queueStream
}

func GetQueueToReportGroup() string {
	queueToReportGroup, found := os.LookupEnv("QUEUE_TO_REPORT_GROUP")
	if !found {
		queueToReportGroup = "queue-to-report_group"
	}
	return queueToReportGroup
}

func GetQueueToReportStream() string {
	queueToReportStream, found := os.LookupEnv("QUEUE_TO_REPORT_STREAM")
	if !found {
		queueToReportStream = "queue-to-report_stream"
	}
	return queueToReportStream
}

func TypeToInterface(typeStruct interface{}) (string, error) {
	log.Debugf("Type struct: %+v", typeStruct)
	b, err := json.Marshal(typeStruct)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

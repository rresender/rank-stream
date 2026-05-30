# rank-stream

A ranked wait-queue service backed by Redis. It tracks **position in line**, broadcasts live updates to clients via Socket.IO (Redis emitter), and supports call-center-style workflows such as disconnect timeouts and average wait hints.

## Problem

Generic job queues (SQS, RabbitMQ, Kafka, Bull) optimize for **work delivery**, not **“you are #47 in line.”** This service fills that gap:

- **Ordered membership** with a stable position per item
- **Live broadcasts** when the queue changes (push, pull, timeout)
- **Hooks for wait intelligence** — average queue time today; estimated wait time (EWT) and metrics on the roadmap

## Non-goals

- Not a replacement for Kafka, SQS, or general background job processing
- Not a full contact-center / ACD platform

## Architecture

```
HTTP API ──► QueueStore (Redis ZSET) ◄── Redis Stream workers
                    │
                    ├── Socket.IO emitter (position / length events)
                    └── Reporting stream (enqueue / dequeue audit)
```

## Quick start

### Requirements

- Go 1.14+
- Redis 5+ (streams, sorted sets; keyspace notifications for disconnect handling)

### Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `PORT` | `7000` | HTTP listen port |
| `REDIS_ADDR` | `localhost:6379` | Redis address |
| `REDIS_PASSWORD` | _(empty)_ | Redis password |
| `REDIS_TLS` | `false` | Enable TLS to Redis |
| `LOG_LEVEL` | `info` | Log level |
| `QUEUE_STREAM` | `queue_stream` | Inbound command stream |
| `QUEUE_GROUP` | `queue_group` | Consumer group for commands |
| `QUEUE_TO_REPORT_STREAM` | `queue-to-report_stream` | Outbound audit stream |
| `QUEUE_TO_REPORT_GROUP` | `queue-to-report_group` | Consumer group for audit |
| `MAX_QUEUE_TIME` | `600` | Max disconnect hold (seconds) |

### Run locally

```bash
# start Redis, then:
go run .
```

### Run tests

```bash
go test ./...
go vet ./...
go test -race ./...
```

### Integration tests

This repository includes an integration test suite that runs against a real Redis instance in Docker.

```bash
go test -tags=integration ./...
```

Make sure Docker is available locally before running these tests.

### Docker

Build the binary into `bin/app`, then:

```bash
docker build -t rank-stream .
docker run -p 7000:7000 -e REDIS_ADDR=host.docker.internal:6379 rank-stream
```

## HTTP API

Base path: `/queue-manager/api/v1`

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Health check (Redis ping) |
| GET | `/queue` | List all queue keys |
| GET | `/queue/{tenant}` | List queues for a tenant |
| PUT | `/queue/{tenant}/{name}/{item}` | Push item; returns `position` |
| DELETE | `/queue/{tenant}/{name}` | Pull head item |
| GET | `/queue/length/{tenant}/{name}` | Queue length |
| GET | `/queue/index/{tenant}/{name}/{item}` | Item position (1-based, -1 if absent) |

Queue keys use the format `{tenant}#{name}`.

## Realtime events

Socket.IO events are published via the Redis emitter (`maindb` prefix by default). Examples:

- `queue_position` — sent to each waiting item (customer namespace)
- `queue_length` — sent to the queue room (provider namespace)
- `queue_pushed`, `queue_pulled`, etc. — stream-driven lifecycle events

## Roadmap (Phase 2+)

- Graceful shutdown for HTTP and background workers
- Replace Redis `KEYS` with `SCAN` for production listing
- `pkg/` module layout and in-memory example
- Documented EWT formula and Prometheus metrics
- CI (test, vet, race detector)

## Tech debt (known)

- Go 1.14 and go-redis v7 — upgrade planned
- Duplicate miniredis entries in `go.mod`
- Redis `KEYS` used for queue listing (avoid at large scale)
- TLS to Redis uses `InsecureSkipVerify` when enabled

## License

MIT — see [LICENSE](./LICENSE).

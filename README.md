# tcp-ais-broker

Lightweight TCP broker that connects to one upstream AIS TCP feed and distributes framed NMEA messages across multiple downstream TCP consumers.

## Nomad Deployment

### Broker Job

```shell
nomad run deploy/broker.nomad
```

Deploys a single broker instance listening on `:9001` (downstream) and `:9101` (metrics). Consul service registration with HTTP readiness check on `/ready`.

### Worker Job

Each `collect-socket` worker connects to the broker via Consul DNS:

```hcl
job "collect-socket" {
  datacenters = ["dc1"]
  type = "service"

  group "workers" {
    count = 8

    task "collect-socket" {
      driver = "raw_exec"

      config {
        command = "/opt/bin/collect-socket"
        args = [
          "--tcp-host", "tcp-ais-broker.service.consul",
          "--tcp-port", "9001"
        ]
      }
    }
  }
}
```

## Usage

```shell
# Build
cargo build --release

# Run with defaults (connects to 153.44.253.27:5631, listens on :9001)
./target/release/tcp-ais-broker

# Custom upstream and downstream ports
./target/release/tcp-ais-broker \
  --upstream-host 153.44.253.27 \
  --upstream-port 5631 \
  --listen 0.0.0.0:9001 \
  --metrics-listen 0.0.0.0:9101

# Quick test with a downstream consumer
nc localhost 9001

# Health and metrics
curl http://localhost:9101/health
curl http://localhost:9101/ready
curl http://localhost:9101/metrics
```

## Configuration

| Flag | Default | Description |
|------|---------|-------------|
| `--upstream-host` | `153.44.253.27` | Upstream AIS feed host |
| `--upstream-port` | `5631` | Upstream AIS feed port |
| `--upstream-connect-timeout-ms` | `5000` | TCP connect timeout |
| `--upstream-read-timeout-ms` | `30000` | TCP read timeout |
| `--reconnect-min-ms` | `1000` | Min reconnect delay |
| `--reconnect-max-ms` | `30000` | Max reconnect delay |
| `--listen` | `0.0.0.0:9001` | Downstream TCP listen address |
| `--metrics-listen` | `0.0.0.0:9101` | HTTP metrics/health listen address |
| `--framing` | `line` | Framing mode (`line`) |
| `--max-line-bytes` | `4096` | Max bytes per NMEA line |
| `--ais-multipart-mode` | `affinity` | Multipart mode: `line`, `affinity`, `reassemble` |
| `--load-balance-strategy` | `round_robin` | `round_robin`, `least_pending`, `hash_affinity` |
| `--queue-max-messages` | `100000` | Max messages in queue |
| `--queue-max-bytes` | `268435456` | Max bytes in queue (256 MB) |
| `--backpressure-policy` | `block_upstream_read` | `block_upstream_read`, `drop_newest`, `drop_oldest`, `exit` |
| `--no-consumer-policy` | `buffer` | `buffer`, `drop`, `pause`, `exit` |

## Metrics

Exposed at `/metrics` on the metrics listen port:

```
tcp_broker_upstream_connected
tcp_broker_downstream_clients
tcp_broker_messages_received_total
tcp_broker_messages_delivered_total
tcp_broker_messages_dropped_total
tcp_broker_bytes_received_total
tcp_broker_bytes_delivered_total
tcp_broker_queue_depth_messages
tcp_broker_queue_depth_bytes
tcp_broker_upstream_reconnects_total
tcp_broker_downstream_disconnects_total
tcp_broker_multipart_groups_active
tcp_broker_multipart_timeouts_total
```

## Architecture

```
153.44.253.27:5631
        │
        │ single outbound TCP connection
        ▼
+--------------------------+
| tcp-ais-broker           |
| - upstream TCP client    |
| - AIS/NMEA framing       |
| - work queue             |
| - downstream TCP server  |
| - load balancing         |
+--------------------------+
        │
        ├── collect-socket[0]
        ├── collect-socket[1]
        ├── collect-socket[2]
        └── collect-socket[N]
```

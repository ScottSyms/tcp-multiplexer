Below is a first-pass technical specification.

TCP AIS Work Broker Specification

1. Purpose

Build a lightweight TCP broker that connects to one upstream AIS TCP feed and distributes framed messages across multiple downstream TCP consumers.

The broker allows existing collect-socket workers to remain TCP stream consumers while enabling horizontal scaling under Nomad.

2. Target Architecture

153.44.253.27:5631
        │
        │ single outbound TCP connection
        ▼
+--------------------------+
| tcp-ais-broker           |
|                          |
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

3. Functional Requirements

3.1 Upstream Connection

The broker shall connect as a TCP client to:

TCP_HOST = 153.44.253.27
TCP_PORT = 5631

The broker shall maintain one active upstream connection by default.

The broker shall automatically reconnect on failure using configurable backoff.

Configurable upstream parameters:

--upstream-host 153.44.253.27
--upstream-port 5631
--upstream-connect-timeout-ms 5000
--upstream-read-timeout-ms 30000
--reconnect-min-ms 1000
--reconnect-max-ms 30000

3.2 Downstream Server

The broker shall expose a TCP server for downstream consumers.

Example:

--listen 0.0.0.0:9001

Each collect-socket task connects to the broker as if it were the original TCP feed.

The broker shall support multiple simultaneous downstream TCP clients.

3.3 Message Framing

The broker shall read the upstream TCP byte stream and split it into complete messages.

For AIS/NMEA, the default framing mode shall be line-based:

delimiter = \n

The broker shall preserve the original line ending where possible.

Example upstream input:

!AIVDM,1,1,,A,15Mvq@001oJr>tpE>R0?wvlN0<0u,0*5C\r\n

Broker emits one framed message:

!AIVDM,1,1,,A,15Mvq@001oJr>tpE>R0?wvlN0<0u,0*5C\r\n

Configurable framing parameters:

--framing line
--max-line-bytes 4096
--preserve-line-ending true

3.4 AIS Multipart Handling

AIS multipart messages are a risk.

If the upstream sends multipart NMEA fragments such as:

!AIVDM,2,1,7,A,...
!AIVDM,2,2,7,A,...

then simple round-robin distribution may send fragment 1 to worker A and fragment 2 to worker B.

The broker shall support one of these modes:

Mode A: Line-level distribution

Each NMEA line is distributed independently.

Use this only if downstream workers do not require multipart reassembly, or if fragmentation is known not to matter.

--ais-multipart-mode line

Mode B: Fragment-affinity distribution

The broker shall route all fragments with the same multipart key to the same downstream worker.

Suggested key:

fragment_count + fragment_number + sequential_message_id + radio_channel

For NMEA AIS, relevant fields are:

!AIVDM,total_fragments,fragment_number,sequential_id,channel,payload,fill_bits*checksum

Affinity key:

sequential_id + channel

If sequential_id is empty, fall back to line-level distribution or local buffering.

--ais-multipart-mode affinity

Mode C: Broker-side reassembly

The broker shall buffer multipart AIS fragments and emit only complete logical AIS messages to a downstream worker.

This is the safest mode but requires more protocol logic.

--ais-multipart-mode reassemble
--multipart-timeout-ms 2000

Recommended default:

--ais-multipart-mode affinity

4. Load-Balancing Requirements

The broker shall distribute framed messages among currently connected downstream consumers.

Supported strategies:

round_robin
least_pending
hash_affinity

Default:

round_robin

For multipart AIS affinity mode, the broker shall override normal balancing for related fragments and send them to the same consumer.

If no downstream consumers are connected, the broker shall apply the configured backpressure policy.

5. Backpressure and Queueing

The broker shall maintain an in-memory queue between upstream read and downstream write.

Configurable queue parameters:

--queue-max-messages 100000
--queue-max-bytes 256MB

When the queue is full, supported policies:

block_upstream_read
drop_newest
drop_oldest
exit

Recommended default:

block_upstream_read

Rationale: blocking upstream reads applies TCP backpressure. This is preferable to silent data loss if the upstream tolerates it.

The broker shall expose counters for dropped messages if any drop policy is used.

6. Downstream Delivery Semantics

Baseline delivery semantics:

at-most-once

Once a message is written to a downstream TCP socket, the broker considers it delivered.

Optional enhanced mode:

application_ack

In this mode, workers must acknowledge messages. This requires changing collect-socket, so it is not part of the initial design.

Initial implementation shall assume:

delivery = at-most-once

Implication: if a worker disconnects after receiving a message but before processing it, that message may be lost.

7. Failure Handling

7.1 Upstream Disconnect

On upstream disconnect, the broker shall:

1. close or pause upstream reader;
2. retain connected downstream clients;
3. reconnect using backoff;
4. resume message distribution after reconnect.

7.2 Downstream Disconnect

If a downstream worker disconnects, the broker shall:

1. remove it from the active worker pool;
2. stop assigning new messages to it;
3. continue distributing to remaining workers.

If a write fails mid-message, the broker shall treat the message as undelivered only if it failed before any bytes were written. Otherwise the message is considered lost under at-most-once semantics.

7.3 No Downstream Workers

If no downstream workers are connected, broker behaviour is controlled by:

--no-consumer-policy buffer

Supported values:

buffer
drop
pause
exit

Recommended default:

buffer

with queue limits enforced.

8. Observability

The broker shall expose an HTTP metrics endpoint.

Example:

--metrics-listen 0.0.0.0:9101

Metrics:

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

Logs shall include:

upstream_connected
upstream_disconnected
downstream_connected
downstream_disconnected
queue_full
message_dropped
multipart_timeout

Logs should be structured JSON.

9. Health Checks

The broker shall expose:

/health
/ready
/metrics

Health semantics:

/health = process is alive
/ready  = upstream connected and at least one downstream worker connected

Nomad should use /health for liveness and /ready for readiness.

10. Security

Initial deployment may run on a trusted private network.

Optional security features:

--downstream-allow-cidr 10.0.0.0/8
--downstream-tls-cert
--downstream-tls-key
--upstream-tls false

If downstream consumers run on the same host or private Nomad network, plain TCP is acceptable for initial testing.

11. Nomad Deployment

11.1 Broker Job

job "tcp-ais-broker" {
  datacenters = ["dc1"]
  type = "service"
  group "broker" {
    count = 1
    network {
      port "downstream" {
        static = 9001
      }
      port "metrics" {
        static = 9101
      }
    }
    service {
      name = "tcp-ais-broker"
      port = "downstream"
      check {
        name     = "ready"
        type     = "http"
        path     = "/ready"
        port     = "metrics"
        interval = "10s"
        timeout  = "2s"
      }
    }
    task "broker" {
      driver = "raw_exec"
      config {
        command = "/opt/bin/tcp-ais-broker"
        args = [
          "--upstream-host", "153.44.253.27",
          "--upstream-port", "5631",
          "--listen", "0.0.0.0:9001",
          "--metrics-listen", "0.0.0.0:9101",
          "--framing", "line",
          "--ais-multipart-mode", "affinity",
          "--queue-max-messages", "100000",
          "--no-consumer-policy", "buffer"
        ]
      }
    }
  }
}

11.2 Worker Job

Each collect-socket instance connects to the broker instead of the upstream feed.

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

12. Configuration Summary

Required:

upstream_host
upstream_port
listen_addr
framing
load_balance_strategy

Recommended defaults:

upstream_host = 153.44.253.27
upstream_port = 5631
listen_addr = 0.0.0.0:9001
framing = line
ais_multipart_mode = affinity
load_balance_strategy = round_robin
queue_max_messages = 100000
queue_max_bytes = 268435456
no_consumer_policy = buffer
backpressure_policy = block_upstream_read
delivery = at_most_once

13. Non-Goals

The initial broker shall not:

persist messages to disk
guarantee exactly-once delivery
parse AIS payload content deeply
write to Iceberg
replace collect-socket
require NATS/Kafka/Redis

14. Test Plan

14.1 Unit Tests

Test:

line framing
partial TCP reads
multiple messages in one read
oversized lines
round-robin balancing
worker disconnect
no workers connected
queue overflow
multipart affinity routing

14.2 Integration Tests

Use a mock upstream TCP server that emits AIS-like NMEA lines.

Start:

1 broker
3 mock collect-socket clients
1 upstream feed simulator

Verify:

all messages delivered
each message delivered to exactly one worker
distribution is approximately balanced
multipart fragments go to same worker
broker reconnects after upstream disconnect
broker removes dead downstream workers

14.3 Load Tests

Simulate expected AIS message rates.

Measure:

messages/sec
latency p50/p95/p99
queue depth
CPU usage
memory usage
worker distribution
message loss
reconnect behaviour

15. Implementation Recommendation

Use Rust with Tokio.

Suggested crates:

tokio
tokio-util
bytes
clap
tracing
tracing-subscriber
metrics
metrics-exporter-prometheus
anyhow
thiserror

Main async tasks:

upstream_reader
framer
dispatcher
downstream_acceptor
worker_writer per downstream client
metrics_server

Internal channels:

upstream_reader → dispatcher: framed messages
dispatcher → worker_writer: assigned messages

16. MVP Scope

MVP should include:

single upstream TCP connection
downstream TCP listener
line-based framing
round-robin distribution
basic reconnect
basic queue
structured logs
Prometheus metrics
Nomad job file

Defer:

TLS
ACK protocol
disk persistence
broker clustering
exactly-once delivery
full AIS reassembly

17. Key Design Decision

The broker must split the upstream TCP byte stream into protocol messages before load balancing.

It must not load-balance arbitrary TCP bytes.

For AIS, the minimum safe unit is usually a full NMEA line. If multipart AIS messages are common and collect-socket expects to reassemble them, the broker must enforce fragment affinity or perform reassembly itself.

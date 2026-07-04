mod config;
mod dispatcher;
mod downstream;
mod framing;
mod http;
mod message;
mod multipart;
mod queue;
mod upstream;

use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::Arc;

use clap::Parser;
use metrics::describe_counter;
use metrics::describe_gauge;
use metrics_exporter_prometheus::PrometheusBuilder;
use tokio::sync::{mpsc, Notify};
use tracing_subscriber::EnvFilter;

use crate::config::Config;
use crate::dispatcher::{dispatcher_task, DispatcherCommand};
use crate::downstream::downstream_listener_task;
use crate::http::http_server_task;
use crate::queue::BoundedQueue;
use crate::upstream::upstream_reader_task;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = Config::parse();

    setup_logging();
    let prometheus_handle = setup_metrics();

    let upstream_connected = Arc::new(AtomicBool::new(false));
    let downstream_count = Arc::new(AtomicUsize::new(0));
    let pause_flag = Arc::new(AtomicBool::new(false));
    let shutdown = Arc::new(Notify::new());

    let queue = BoundedQueue::new(config.queue_max_messages, config.queue_max_bytes);

    let (cmd_tx, cmd_rx) = mpsc::channel::<DispatcherCommand>(256);

    let mut handles = Vec::new();

    handles.push(tokio::spawn(dispatcher_task(
        queue.clone(),
        cmd_rx,
        upstream_connected.clone(),
        downstream_count.clone(),
        pause_flag.clone(),
        config.clone(),
        shutdown.clone(),
    )));

    handles.push(tokio::spawn(upstream_reader_task(
        config.clone(),
        queue.clone(),
        upstream_connected.clone(),
        pause_flag.clone(),
        shutdown.clone(),
    )));

    handles.push(tokio::spawn(downstream_listener_task(
        config.clone(),
        cmd_tx.clone(),
        downstream_count.clone(),
        shutdown.clone(),
    )));

    handles.push(tokio::spawn(http_server_task(
        config.clone(),
        upstream_connected.clone(),
        downstream_count.clone(),
        prometheus_handle,
        shutdown.clone(),
    )));

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("shutdown signal received");
            shutdown.notify_waiters();
        }
    }

    for handle in handles {
        let _ = handle.await;
    }

    tracing::info!("broker shut down");
    Ok(())
}

fn setup_logging() {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_target(false)
        .init();
}

fn setup_metrics() -> metrics_exporter_prometheus::PrometheusHandle {
    describe_counter!("tcp_broker_messages_received_total", "Total messages received from upstream");
    describe_counter!("tcp_broker_messages_delivered_total", "Total messages delivered to downstream workers");
    describe_counter!("tcp_broker_messages_dropped_total", "Total messages dropped");
    describe_counter!("tcp_broker_bytes_received_total", "Total bytes received from upstream");
    describe_counter!("tcp_broker_bytes_delivered_total", "Total bytes delivered to downstream workers");
    describe_counter!("tcp_broker_upstream_reconnects_total", "Total upstream reconnection attempts");
    describe_counter!("tcp_broker_downstream_disconnects_total", "Total downstream disconnections");
    describe_counter!("tcp_broker_lines_truncated_total", "Total oversized lines truncated");
    describe_counter!("tcp_broker_multipart_timeouts_total", "Total multipart reassembly timeouts");

    describe_gauge!("tcp_broker_upstream_connected", "Whether upstream is connected (1 or 0)");
    describe_gauge!("tcp_broker_downstream_clients", "Number of connected downstream clients");
    describe_gauge!("tcp_broker_queue_depth_messages", "Current number of messages in queue");
    describe_gauge!("tcp_broker_queue_depth_bytes", "Current total bytes in queue");
    describe_gauge!("tcp_broker_multipart_groups_active", "Number of active multipart affinity groups");

    let builder = PrometheusBuilder::new();
    let handle = builder
        .install_recorder()
        .expect("failed to install prometheus recorder");
    handle
}

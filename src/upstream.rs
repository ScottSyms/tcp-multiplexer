use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::net::TcpStream;
use tokio::sync::Notify;
use futures_util::StreamExt;
use tokio::time;
use tokio_util::codec::FramedRead;

use crate::config::{BackpressurePolicy, Config, MultipartMode};
use crate::framing::LineCodec;
use crate::message::Message;
use crate::multipart::{compute_affinity_key, parse_ais_multipart};
use crate::queue::BoundedQueue;

pub async fn upstream_reader_task(
    config: Config,
    queue: Arc<BoundedQueue>,
    upstream_connected: Arc<AtomicBool>,
    pause_flag: Arc<AtomicBool>,
    shutdown: Arc<Notify>,
) {
    let addr = format!("{}:{}", config.upstream_host, config.upstream_port);
    let connect_timeout = Duration::from_millis(config.upstream_connect_timeout_ms);
    let read_timeout = Duration::from_millis(config.upstream_read_timeout_ms);
    let reconnect_min = Duration::from_millis(config.reconnect_min_ms);
    let reconnect_max = Duration::from_millis(config.reconnect_max_ms);
    let max_line_bytes = config.max_line_bytes;
    let mut reconnect_delay = reconnect_min;

    loop {
        if pause_flag.load(Ordering::Acquire) {
            time::sleep(Duration::from_millis(100)).await;
            continue;
        }

        tracing::info!(addr = %addr, "connecting to upstream");

        let stream = match time::timeout(connect_timeout, TcpStream::connect(&addr)).await {
            Ok(Ok(stream)) => stream,
            Ok(Err(e)) => {
                tracing::warn!(addr = %addr, error = %e, "upstream connect failed");
                reconnect(&mut reconnect_delay, reconnect_min, reconnect_max, &shutdown).await;
                continue;
            }
            Err(_) => {
                tracing::warn!(addr = %addr, "upstream connect timed out");
                reconnect(&mut reconnect_delay, reconnect_min, reconnect_max, &shutdown).await;
                continue;
            }
        };

        reconnect_delay = reconnect_min;
        upstream_connected.store(true, Ordering::Release);
        metrics::gauge!("tcp_broker_upstream_connected").set(1.0);
        tracing::info!(addr = %addr, "upstream_connected");

        let framer = LineCodec::new(max_line_bytes);
        let mut framed = FramedRead::new(stream, framer);

        loop {
            if pause_flag.load(Ordering::Acquire) {
                break;
            }

            tokio::select! {
                biased;

                _ = shutdown.notified() => {
                    tracing::info!("upstream reader shutting down");
                    upstream_connected.store(false, Ordering::Release);
                    metrics::gauge!("tcp_broker_upstream_connected").set(0.0);
                    return;
                }

                result = time::timeout(read_timeout, framed.next()) => {
                    match result {
                        Ok(Some(Ok(line_bytes))) => {
                            metrics::counter!("tcp_broker_messages_received_total").increment(1);
                            metrics::counter!("tcp_broker_bytes_received_total").increment(line_bytes.len() as u64);

                            let affinity_key = match config.ais_multipart_mode {
                                MultipartMode::Affinity => {
                                    let line_str = std::str::from_utf8(&line_bytes).unwrap_or("");
                                    parse_ais_multipart(line_str)
                                        .as_ref()
                                        .and_then(compute_affinity_key)
                                }
                                _ => None,
                            };

                            let msg = Message {
                                data: line_bytes,
                                affinity_key,
                            };

                            match config.backpressure_policy {
                                BackpressurePolicy::BlockUpstreamRead => {
                                    queue.push(msg).await;
                                }
                                BackpressurePolicy::DropNewest => {
                                    if !queue.try_push(msg).await {
                                        metrics::counter!("tcp_broker_messages_dropped_total").increment(1);
                                    }
                                }
                                BackpressurePolicy::DropOldest => {
                                    queue.push_drop_oldest(msg).await;
                                }
                                BackpressurePolicy::Exit => {
                                    if !queue.try_push(msg).await {
                                        tracing::error!("queue full, exiting per backpressure policy");
                                        std::process::exit(1);
                                    }
                                }
                            }
                        }
                        Ok(Some(Err(e))) => {
                            tracing::warn!(addr = %addr, error = %e, "upstream read error");
                            break;
                        }
                        Ok(None) => {
                            tracing::info!(addr = %addr, "upstream_disconnected");
                            break;
                        }
                        Err(_) => {
                            tracing::warn!(addr = %addr, "upstream read timed out");
                            break;
                        }
                    }
                }
            }
        }

        upstream_connected.store(false, Ordering::Release);
        metrics::gauge!("tcp_broker_upstream_connected").set(0.0);
        metrics::counter!("tcp_broker_upstream_reconnects_total").increment(1);

        reconnect(&mut reconnect_delay, reconnect_min, reconnect_max, &shutdown).await;
    }
}

async fn reconnect(
    delay: &mut Duration,
    _min: Duration,
    max: Duration,
    shutdown: &Arc<Notify>,
) {
    tracing::info!(delay_ms = delay.as_millis(), "reconnecting");

    tokio::select! {
        _ = time::sleep(*delay) => {}
        _ = shutdown.notified() => {}
    }

    *delay = (*delay * 2).min(max);
}

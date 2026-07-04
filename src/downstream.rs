use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::sync::Notify;

use crate::config::Config;
use crate::dispatcher::DispatcherCommand;
use crate::message::Message;

pub async fn downstream_listener_task(
    config: Config,
    cmd_tx: mpsc::Sender<DispatcherCommand>,
    downstream_count: Arc<AtomicUsize>,
    shutdown: Arc<Notify>,
) {
    let listener = match tokio::net::TcpListener::bind(&config.listen).await {
        Ok(l) => l,
        Err(e) => {
            tracing::error!(addr = %config.listen, error = %e, "failed to bind downstream listener");
            return;
        }
    };

    tracing::info!(addr = %config.listen, "downstream listener started");

    let worker_id_counter = Arc::new(AtomicUsize::new(0));

    loop {
        tokio::select! {
            biased;

            _ = shutdown.notified() => {
                tracing::info!("downstream listener shutting down");
                return;
            }

            result = listener.accept() => {
                match result {
                    Ok((stream, addr)) => {
                        tracing::info!(remote = %addr, "downstream_connected");
                        let id = worker_id_counter.fetch_add(1, Ordering::Relaxed);
                        let cmd_tx = cmd_tx.clone();
                        let downstream_count = downstream_count.clone();

                        tokio::spawn(worker_writer_task(
                            id,
                            stream,
                            cmd_tx,
                            downstream_count,
                            shutdown.clone(),
                        ));
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "failed to accept downstream connection");
                    }
                }
            }
        }
    }
}

async fn worker_writer_task(
    worker_id: usize,
    stream: TcpStream,
    cmd_tx: mpsc::Sender<DispatcherCommand>,
    downstream_count: Arc<AtomicUsize>,
    shutdown: Arc<Notify>,
) {
    let (mut read_half, mut write_half) = stream.into_split();
    let (msg_tx, mut msg_rx) = mpsc::unbounded_channel::<Message>();

    cmd_tx
        .send(DispatcherCommand::Register {
            id: worker_id,
            sender: msg_tx,
        })
        .await
        .ok();
    downstream_count.fetch_add(1, Ordering::Release);

    let mut read_buf = [0u8; 1];

    loop {
        tokio::select! {
            biased;

            _ = shutdown.notified() => {
                tracing::info!(worker_id, "writer shutting down");
                break;
            }

            Some(msg) = msg_rx.recv() => {
                if let Err(e) = write_half.write_all(&msg.data).await {
                    tracing::warn!(worker_id, error = %e, "write to downstream failed");
                    break;
                }
                metrics::counter!("tcp_broker_messages_delivered_total").increment(1);
                metrics::counter!("tcp_broker_bytes_delivered_total").increment(msg.data.len() as u64);
            }

            result = read_half.read(&mut read_buf) => {
                match result {
                    Ok(0) => {
                        tracing::info!(worker_id, "downstream_disconnected");
                        break;
                    }
                    Err(e) => {
                        tracing::warn!(worker_id, error = %e, "downstream read error");
                        break;
                    }
                    _ => {}
                }
            }
        }
    }

    downstream_count.fetch_sub(1, Ordering::Release);
    cmd_tx
        .send(DispatcherCommand::Unregister { id: worker_id })
        .await
        .ok();
    metrics::counter!("tcp_broker_downstream_disconnects_total").increment(1);
    metrics::gauge!("tcp_broker_downstream_clients").set(downstream_count.load(Ordering::Acquire) as f64);
}

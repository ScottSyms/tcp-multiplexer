use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::time;

use crate::config::{Config, LoadBalanceStrategy, NoConsumerPolicy};
use crate::message::Message;
use crate::queue::BoundedQueue;

pub type WorkerId = usize;

pub enum DispatcherCommand {
    Register {
        id: WorkerId,
        sender: mpsc::UnboundedSender<Message>,
    },
    Unregister {
        id: WorkerId,
    },
}

pub async fn dispatcher_task(
    queue: Arc<BoundedQueue>,
    mut cmd_rx: mpsc::Receiver<DispatcherCommand>,
    _upstream_connected: Arc<AtomicBool>,
    _downstream_count: Arc<AtomicUsize>,
    pause_flag: Arc<AtomicBool>,
    config: Config,
    shutdown: Arc<tokio::sync::Notify>,
) {
    let mut workers: HashMap<WorkerId, mpsc::UnboundedSender<Message>> = HashMap::new();
    let mut affinity_map: HashMap<String, WorkerId> = HashMap::new();
    let mut rr_counter: usize = 0;
    let mut metrics_tick = tokio::time::interval(Duration::from_secs(5));

    loop {
        tokio::select! {
            biased;

            _ = shutdown.notified() => {
                tracing::info!("dispatcher shutting down");
                return;
            }

            _ = metrics_tick.tick() => {
                metrics::gauge!("tcp_broker_queue_depth_messages").set(queue.len() as f64);
                metrics::gauge!("tcp_broker_queue_depth_bytes").set(queue.current_bytes() as f64);
                metrics::gauge!("tcp_broker_multipart_groups_active").set(affinity_map.len() as f64);
            }

            Some(cmd) = cmd_rx.recv() => {
                match cmd {
                    DispatcherCommand::Register { id, sender } => {
                        tracing::info!(worker_id = id, "worker registered");
                        workers.insert(id, sender);
                        metrics::gauge!("tcp_broker_downstream_clients").set(workers.len() as f64);
                        pause_flag.store(false, Ordering::Release);
                    }
                    DispatcherCommand::Unregister { id } => {
                        tracing::info!(worker_id = id, "worker unregistered");
                        workers.remove(&id);
                        affinity_map.retain(|_, wid| *wid != id);
                        metrics::gauge!("tcp_broker_downstream_clients").set(workers.len() as f64);
                    }
                }
                continue;
            }

            msg = queue.pop() => {
                if workers.is_empty() {
                    match config.no_consumer_policy {
                        NoConsumerPolicy::Buffer => {
                            queue.push(msg).await;
                            time::sleep(Duration::from_millis(50)).await;
                            continue;
                        }
                        NoConsumerPolicy::Drop => {
                            metrics::counter!("tcp_broker_messages_dropped_total").increment(1);
                            continue;
                        }
                        NoConsumerPolicy::Pause => {
                            pause_flag.store(true, Ordering::Release);
                            metrics::counter!("tcp_broker_messages_dropped_total").increment(1);
                            continue;
                        }
                        NoConsumerPolicy::Exit => {
                            tracing::error!("no downstream consumers, exiting");
                            std::process::exit(1);
                        }
                    }
                }

                let worker_id = select_worker(&msg, &workers, &mut affinity_map, &mut rr_counter, &config);

                if let Some(worker_id) = worker_id {
                    if let Some(sender) = workers.get(&worker_id) {
                        if let Err(e) = sender.send(msg) {
                            tracing::warn!(worker_id, error = %e, "worker channel closed, removing");
                            workers.remove(&worker_id);
                            affinity_map.retain(|_, wid| *wid != worker_id);
                        }
                    }
                }
            }
        }
    }
}

fn select_worker(
    msg: &Message,
    workers: &HashMap<WorkerId, mpsc::UnboundedSender<Message>>,
    affinity_map: &mut HashMap<String, WorkerId>,
    rr_counter: &mut usize,
    config: &Config,
) -> Option<WorkerId> {
    if workers.is_empty() {
        return None;
    }

    if let Some(ref key) = msg.affinity_key {
        if let Some(&worker_id) = affinity_map.get(key) {
            if workers.contains_key(&worker_id) {
                return Some(worker_id);
            }
            affinity_map.remove(key);
        }

        let worker_id = match config.load_balance_strategy {
            LoadBalanceStrategy::RoundRobin => {
                let id = *workers.keys().nth(*rr_counter % workers.len())?;
                *rr_counter = (*rr_counter + 1) % workers.len();
                id
            }
            LoadBalanceStrategy::LeastPending => {
                select_least_pending(workers)?
            }
            LoadBalanceStrategy::HashAffinity => {
                let hash = simple_hash(key);
                *workers.keys().nth(hash % workers.len())?
            }
        };

        affinity_map.insert(key.clone(), worker_id);
        return Some(worker_id);
    }

    match config.load_balance_strategy {
        LoadBalanceStrategy::RoundRobin => {
            let id = *workers.keys().nth(*rr_counter % workers.len())?;
            *rr_counter = (*rr_counter + 1) % workers.len();
            Some(id)
        }
        LoadBalanceStrategy::LeastPending => select_least_pending(workers),
        LoadBalanceStrategy::HashAffinity => {
            let hash = simple_hash(&format!("{}", *rr_counter));
            let id = *workers.keys().nth(hash % workers.len())?;
            *rr_counter = (*rr_counter + 1) % workers.len();
            Some(id)
        }
    }
}

fn select_least_pending(
    workers: &HashMap<WorkerId, mpsc::UnboundedSender<Message>>,
) -> Option<WorkerId> {
    workers.keys().copied().next()
}

fn simple_hash(s: &str) -> usize {
    let mut hash: usize = 5381;
    for b in s.bytes() {
        hash = hash.wrapping_mul(33).wrapping_add(b as usize);
    }
    hash
}

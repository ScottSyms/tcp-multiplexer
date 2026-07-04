use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use metrics_exporter_prometheus::PrometheusHandle;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::sync::Notify;

use crate::config::Config;

pub async fn http_server_task(
    config: Config,
    upstream_connected: Arc<AtomicBool>,
    downstream_count: Arc<AtomicUsize>,
    prometheus_handle: PrometheusHandle,
    shutdown: Arc<Notify>,
) {
    let listener = match TcpListener::bind(&config.metrics_listen).await {
        Ok(l) => l,
        Err(e) => {
            tracing::error!(
                addr = %config.metrics_listen,
                error = %e,
                "failed to bind metrics HTTP server"
            );
            return;
        }
    };

    tracing::info!(addr = %config.metrics_listen, "metrics HTTP server started");

    loop {
        tokio::select! {
            biased;

            _ = shutdown.notified() => {
                tracing::info!("HTTP server shutting down");
                return;
            }

            result = listener.accept() => {
                match result {
                    Ok((mut stream, _addr)) => {
                        let upstream_connected = upstream_connected.clone();
                        let downstream_count = downstream_count.clone();
                        let prometheus_handle = prometheus_handle.clone();

                        tokio::spawn(async move {
                            handle_connection(
                                &mut stream,
                                &upstream_connected,
                                &downstream_count,
                                &prometheus_handle,
                            )
                            .await;
                        });
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "failed to accept HTTP connection");
                    }
                }
            }
        }
    }
}

async fn handle_connection(
    stream: &mut tokio::net::TcpStream,
    upstream_connected: &AtomicBool,
    downstream_count: &AtomicUsize,
    prometheus_handle: &PrometheusHandle,
) {
    let mut buf = [0u8; 1024];
    let n = match stream.read(&mut buf).await {
        Ok(n) if n > 0 => n,
        _ => return,
    };

    let request = String::from_utf8_lossy(&buf[..n]);
    let request_line = request.lines().next().unwrap_or("");

    let (status, body, content_type) = if request_line.starts_with("GET /health") {
        ("200 OK", "ok\n".to_string(), "text/plain")
    } else if request_line.starts_with("GET /ready") {
        let up = upstream_connected.load(Ordering::Acquire);
        let clients = downstream_count.load(Ordering::Acquire);
        if up && clients > 0 {
            ("200 OK", "ready\n".to_string(), "text/plain")
        } else {
            ("503 Service Unavailable", "not ready\n".to_string(), "text/plain")
        }
    } else if request_line.starts_with("GET /metrics") {
        ("200 OK", prometheus_handle.render(), "text/plain; charset=utf-8")
    } else {
        ("404 Not Found", "not found\n".to_string(), "text/plain")
    };

    let response = format!(
        "HTTP/1.1 {}\r\nContent-Type: {}\r\nContent-Length: {}\r\n\r\n{}",
        status,
        content_type,
        body.len(),
        body
    );

    let _ = stream.write_all(response.as_bytes()).await;
}

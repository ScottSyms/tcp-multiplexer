use clap::Parser;

#[derive(Debug, Clone, Parser)]
#[command(name = "tcp-ais-broker", about = "TCP AIS work broker")]
pub struct Config {
    #[arg(long, default_value = "153.44.253.27")]
    pub upstream_host: String,

    #[arg(long, default_value_t = 5631)]
    pub upstream_port: u16,

    #[arg(long, default_value_t = 5000)]
    pub upstream_connect_timeout_ms: u64,

    #[arg(long, default_value_t = 30000)]
    pub upstream_read_timeout_ms: u64,

    #[arg(long, default_value_t = 1000)]
    pub reconnect_min_ms: u64,

    #[arg(long, default_value_t = 30000)]
    pub reconnect_max_ms: u64,

    #[arg(long, default_value = "0.0.0.0:7001")]
    pub listen: String,

    #[arg(long, default_value = "line")]
    pub framing: String,

    #[arg(long, default_value_t = 4096)]
    pub max_line_bytes: usize,

    #[arg(long, default_value_t = true)]
    pub preserve_line_ending: bool,

    #[arg(long, default_value = "affinity")]
    pub ais_multipart_mode: MultipartMode,

    #[arg(long, default_value = "round_robin")]
    pub load_balance_strategy: LoadBalanceStrategy,

    #[arg(long, default_value_t = 100000)]
    pub queue_max_messages: usize,

    #[arg(long, default_value_t = 268435456)]
    pub queue_max_bytes: usize,

    #[arg(long, default_value = "block_upstream_read")]
    pub backpressure_policy: BackpressurePolicy,

    #[arg(long, default_value = "buffer")]
    pub no_consumer_policy: NoConsumerPolicy,

    #[arg(long, default_value = "at_most_once")]
    pub delivery: DeliveryMode,

    #[arg(long, default_value = "0.0.0.0:9101")]
    pub metrics_listen: String,

    #[arg(long, default_value_t = 2000)]
    pub multipart_timeout_ms: u64,

    #[arg(long)]
    pub downstream_allow_cidr: Option<String>,

    #[arg(long)]
    pub downstream_tls_cert: Option<String>,

    #[arg(long)]
    pub downstream_tls_key: Option<String>,

    #[arg(long, default_value_t = false)]
    pub upstream_tls: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, clap::ValueEnum)]
#[clap(rename_all = "snake_case")]
pub enum MultipartMode {
    Line,
    Affinity,
    Reassemble,
}

#[derive(Debug, Clone, Copy, PartialEq, clap::ValueEnum)]
#[clap(rename_all = "snake_case")]
pub enum LoadBalanceStrategy {
    RoundRobin,
    LeastPending,
    HashAffinity,
}

#[derive(Debug, Clone, Copy, PartialEq, clap::ValueEnum)]
#[clap(rename_all = "snake_case")]
pub enum BackpressurePolicy {
    BlockUpstreamRead,
    DropNewest,
    DropOldest,
    Exit,
}

#[derive(Debug, Clone, Copy, PartialEq, clap::ValueEnum)]
#[clap(rename_all = "snake_case")]
pub enum NoConsumerPolicy {
    Buffer,
    Drop,
    Pause,
    Exit,
}

#[derive(Debug, Clone, Copy, PartialEq, clap::ValueEnum)]
#[clap(rename_all = "snake_case")]
pub enum DeliveryMode {
    AtMostOnce,
    ApplicationAck,
}

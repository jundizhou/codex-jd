use clap::Parser;
use std::net::IpAddr;
use std::path::PathBuf;

/// CLI arguments for the proxy.
#[derive(Debug, Clone, Parser)]
#[command(name = "responses-api-proxy", about = "Minimal OpenAI responses proxy")]
pub struct Args {
    /// Address to listen on. Defaults to loopback for local-only operation.
    #[arg(long, default_value = "127.0.0.1")]
    pub listen_address: IpAddr,

    /// Port to listen on. If not set, an ephemeral port is used.
    #[arg(long)]
    pub port: Option<u16>,

    /// Path to a JSON file to write startup info (single line). Includes {"port": <u16>}.
    #[arg(long, value_name = "FILE")]
    pub server_info: Option<PathBuf>,

    /// Enable HTTP shutdown endpoint at GET /shutdown
    #[arg(long)]
    pub http_shutdown: bool,

    /// Directory where request/response dumps should be written as JSON.
    #[arg(long, value_name = "DIR")]
    pub dump_dir: Option<PathBuf>,

    /// Absolute path to the local app-server control socket.
    #[arg(long, value_name = "PATH")]
    pub app_server_socket: Option<PathBuf>,

    /// Shared secret accepted in the inbound `Authorization: Bearer` header.
    /// When omitted, authentication is disabled for backwards compatibility.
    #[arg(long, value_name = "SECRET")]
    pub worker_api_key: Option<String>,

    /// Maximum loaded app-server sessions; durable conversations do not occupy execution slots.
    /// Falls back to CODEX_SESSION_POOL_SIZE, then 32 in queue mode or five otherwise.
    #[arg(long, value_name = "N")]
    pub session_pool_size: Option<usize>,

    /// Enable bounded conversation scheduling and authenticated queue controls.
    #[arg(long)]
    pub queue: bool,

    /// Release quarantined conversations after one successful account/identity check.
    /// Checks repeat after 10/20/30 seconds; remote unknown work may still be running.
    #[arg(long, requires = "queue")]
    pub queue_auto_recover: bool,

    /// Maximum running requests in queue mode.
    #[arg(long, default_value_t = 2, value_parser = clap::value_parser!(u16).range(1..=32))]
    pub queue_max_running: u16,

    /// Minimum milliseconds between requests in the same conversation.
    #[arg(long, default_value_t = 800, value_parser = clap::value_parser!(u64).range(0..=60_000))]
    pub queue_conversation_gap_ms: u64,

    /// Minimum milliseconds between account request starts.
    #[arg(long, default_value_t = 300, value_parser = clap::value_parser!(u64).range(0..=60_000))]
    pub queue_start_gap_ms: u64,

    /// Minimum gap for a verified new user input appended to known history.
    #[arg(long, default_value_t = 1500, value_parser = clap::value_parser!(u64).range(0..=60_000))]
    pub queue_user_gap_ms: u64,

    /// Minimum gap for verified outputs of the preceding model's tool calls.
    #[arg(long, default_value_t = 300, value_parser = clap::value_parser!(u64).range(0..=60_000))]
    pub queue_tool_gap_ms: u64,

    /// Unload idle threads while retaining their durable conversation identities.
    #[arg(long, default_value_t = 900, value_parser = clap::value_parser!(u64).range(1..=86_400))]
    pub queue_idle_ttl_secs: u64,

    /// Exclusive durable dispatch journal; preserve this file across worker restarts.
    #[arg(long, requires = "queue", value_name = "FILE")]
    pub queue_state: Option<PathBuf>,
}

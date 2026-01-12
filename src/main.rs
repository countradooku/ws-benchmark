use anyhow::{Context, Result};
use clap::Parser;
use futures_util::{SinkExt, StreamExt};
use hdrhistogram::Histogram;
use rand::prelude::{IndexedRandom, SliceRandom};
use serde::{Deserialize, Serialize};
use sonic_rs::JsonValueTrait;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, Mutex};
use tokio::time::{interval, sleep};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, error, info, warn};

// =============================================================================
// Configuration
// =============================================================================

#[derive(Parser, Debug)]
#[command(name = "ws-benchmark")]
#[command(about = "WebSocket tag filtering benchmark", long_about = None)]
struct Config {
    /// WebSocket host
    #[arg(long, env = "WS_HOST", default_value = "stream-v2.projectscylla.com")]
    ws_host: String,

    /// WebSocket port
    #[arg(long, env = "WS_PORT", default_value = "443")]
    ws_port: u16,

    /// Application key
    #[arg(long, env = "APP_KEY", default_value = "knife-library-likely")]
    app_key: String,

    /// Channel name
    #[arg(long, env = "CHANNEL", default_value = "trident_filter_tokens_v1")]
    channel: String,

    /// Scenario (1-5)
    #[arg(long, env = "SCENARIO", default_value = "1")]
    scenario: u8,

    /// Token addresses JSON file
    #[arg(long, env = "TOKEN_FILE", default_value = "token-addresses.json")]
    token_file: PathBuf,

    /// Filter update interval in milliseconds (Scenario 2)
    #[arg(long, env = "FILTER_UPDATE_INTERVAL", default_value = "5000")]
    filter_update_interval: u64,

    /// Target number of clients (1000, 5000, or 10000)
    #[arg(long, env = "NUM_CLIENTS", default_value = "1000")]
    num_clients: usize,

    /// Duration to ramp up to target clients in seconds
    #[arg(long, env = "RAMP_DURATION", default_value = "30")]
    ramp_duration: u64,

    /// Duration to hold at target client count in seconds
    #[arg(long, env = "HOLD_DURATION", default_value = "60")]
    hold_duration: u64,

    /// Duration to ramp down in seconds
    #[arg(long, env = "RAMP_DOWN_DURATION", default_value = "10")]
    ramp_down_duration: u64,
}

// =============================================================================
// Data Structures
// =============================================================================

#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
enum FilterValue {
    Single {
        key: String,
        cmp: String,
        val: String,
    },
    Multiple {
        key: String,
        cmp: String,
        vals: Vec<String>,
    },
}

#[derive(Debug, Deserialize)]
struct PusherMessage {
    event: String,
    #[serde(default)]
    channel: Option<String>,
    #[serde(default)]
    data: Option<sonic_rs::Value>,
    #[serde(default)]
    tags: Option<sonic_rs::Value>,
}

#[derive(Debug, Serialize)]
struct SubscribeMessage {
    event: String,
    data: SubscribeData,
}

#[derive(Debug, Serialize)]
struct SubscribeData {
    channel: String,
    filter: FilterValue,
}

#[derive(Debug, Serialize)]
struct PongMessage {
    event: String,
    data: sonic_rs::Value,
}

// =============================================================================
// Metrics
// =============================================================================

#[derive(Clone)]
struct Metrics {
    subscribe_latency: Arc<Mutex<Histogram<u64>>>,
    filter_update_latency: Arc<Mutex<Histogram<u64>>>,
    e2e_latency: Arc<Mutex<Histogram<u64>>>,
    messages_received: Arc<AtomicU64>,
    subscribe_success: Arc<AtomicU64>,
    subscribe_failed: Arc<AtomicU64>,
    filter_updates: Arc<AtomicU64>,
    connection_errors: Arc<AtomicU64>,
    active_connections: Arc<AtomicUsize>,
}

impl Metrics {
    fn new() -> Self {
        Self {
            subscribe_latency: Arc::new(Mutex::new(
                Histogram::<u64>::new_with_bounds(1, 60_000, 3).unwrap(),
            )),
            filter_update_latency: Arc::new(Mutex::new(
                Histogram::<u64>::new_with_bounds(1, 60_000, 3).unwrap(),
            )),
            e2e_latency: Arc::new(Mutex::new(
                Histogram::<u64>::new_with_bounds(1, 60_000, 3).unwrap(),
            )),
            messages_received: Arc::new(AtomicU64::new(0)),
            subscribe_success: Arc::new(AtomicU64::new(0)),
            subscribe_failed: Arc::new(AtomicU64::new(0)),
            filter_updates: Arc::new(AtomicU64::new(0)),
            connection_errors: Arc::new(AtomicU64::new(0)),
            active_connections: Arc::new(AtomicUsize::new(0)),
        }
    }

    async fn print_summary(&self) {
        info!("╔════════════════════════════════════════════════════════════╗");
        info!("║                    BENCHMARK SUMMARY                       ║");
        info!("╚════════════════════════════════════════════════════════════╝");

        let sub_hist = self.subscribe_latency.lock().await;
        let filter_hist = self.filter_update_latency.lock().await;
        let e2e_hist = self.e2e_latency.lock().await;

        info!("");
        info!("Connection Metrics:");
        info!(
            "  Subscribe Success:   {}",
            self.subscribe_success.load(Ordering::Relaxed)
        );
        info!(
            "  Subscribe Failed:    {}",
            self.subscribe_failed.load(Ordering::Relaxed)
        );
        info!(
            "  Connection Errors:   {}",
            self.connection_errors.load(Ordering::Relaxed)
        );
        info!(
            "  Filter Updates:      {}",
            self.filter_updates.load(Ordering::Relaxed)
        );
        info!(
            "  Messages Received:   {}",
            self.messages_received.load(Ordering::Relaxed)
        );

        info!("");
        info!("Subscribe Latency (ms):");
        if sub_hist.len() > 0 {
            info!("  Min:    {:.2}", sub_hist.min());
            info!("  Mean:   {:.2}", sub_hist.mean());
            info!("  p50:    {:.2}", sub_hist.value_at_quantile(0.50));
            info!("  p95:    {:.2}", sub_hist.value_at_quantile(0.95));
            info!("  p99:    {:.2}", sub_hist.value_at_quantile(0.99));
            info!("  Max:    {:.2}", sub_hist.max());
        } else {
            info!("  No data");
        }

        if filter_hist.len() > 0 {
            info!("");
            info!("Filter Update Latency (ms):");
            info!("  Min:    {:.2}", filter_hist.min());
            info!("  Mean:   {:.2}", filter_hist.mean());
            info!("  p50:    {:.2}", filter_hist.value_at_quantile(0.50));
            info!("  p95:    {:.2}", filter_hist.value_at_quantile(0.95));
            info!("  p99:    {:.2}", filter_hist.value_at_quantile(0.99));
            info!("  Max:    {:.2}", filter_hist.max());
        }

        info!("");
        info!("End-to-End Latency (ms):");
        if e2e_hist.len() > 0 {
            info!("  Min:    {:.2}", e2e_hist.min());
            info!("  Mean:   {:.2}", e2e_hist.mean());
            info!("  p50:    {:.2}", e2e_hist.value_at_quantile(0.50));
            info!("  p95:    {:.2}", e2e_hist.value_at_quantile(0.95));
            info!("  p99:    {:.2}", e2e_hist.value_at_quantile(0.99));
            info!("  Max:    {:.2}", e2e_hist.max());
        } else {
            info!("  No data");
        }

        info!("");
        info!("═══════════════════════════════════════════════════════════");
    }
}

// =============================================================================
// Token Management
// =============================================================================

#[derive(Clone)]
struct TokenPool {
    addresses: Arc<Vec<String>>,
}

impl TokenPool {
    fn load_from_file(path: &PathBuf) -> Result<Self> {
        let content = std::fs::read_to_string(path)
            .context(format!("Failed to read token file: {:?}", path))?;

        let addresses: Vec<String> =
            sonic_rs::from_str(&content).context("Failed to parse token addresses JSON")?;

        info!("Loaded {} token addresses from {:?}", addresses.len(), path);

        Ok(Self {
            addresses: Arc::new(addresses),
        })
    }

    fn generate_fake(count: usize) -> Self {
        warn!("Generating {} fake token addresses", count);
        let addresses: Vec<String> = (0..count).map(|i| format!("0x{:040x}", i)).collect();

        Self {
            addresses: Arc::new(addresses),
        }
    }

    fn get_random(&self) -> String {
        let mut rng = rand::rng();
        self.addresses[..].choose(&mut rng).unwrap().clone()
    }

    fn get_random_unique(&self, count: usize) -> Vec<String> {
        let mut rng = rand::rng();
        let max_count = self.addresses.len().min(count);

        let mut indices: Vec<usize> = (0..self.addresses.len()).collect();
        indices.shuffle(&mut rng);

        indices
            .into_iter()
            .take(max_count)
            .map(|i| self.addresses[i].clone())
            .collect()
    }
}

// =============================================================================
// Filter Building
// =============================================================================

fn build_filter(scenario: u8, tokens: &TokenPool) -> FilterValue {
    match scenario {
        1 => FilterValue::Single {
            key: "token_address".to_string(),
            cmp: "eq".to_string(),
            val: tokens.get_random(),
        },
        2 => FilterValue::Single {
            key: "token_address".to_string(),
            cmp: "eq".to_string(),
            val: tokens.get_random(),
        },
        3 => FilterValue::Multiple {
            key: "token_address".to_string(),
            cmp: "in".to_string(),
            vals: tokens.get_random_unique(10),
        },
        4 => FilterValue::Multiple {
            key: "token_address".to_string(),
            cmp: "in".to_string(),
            vals: tokens.get_random_unique(100),
        },
        5 => FilterValue::Multiple {
            key: "token_address".to_string(),
            cmp: "in".to_string(),
            vals: tokens.get_random_unique(500),
        },
        _ => FilterValue::Single {
            key: "token_address".to_string(),
            cmp: "eq".to_string(),
            val: tokens.get_random(),
        },
    }
}

// =============================================================================
// WebSocket Client
// =============================================================================

async fn run_client(
    id: usize,
    config: Arc<Config>,
    tokens: TokenPool,
    metrics: Metrics,
    mut shutdown: broadcast::Receiver<()>,
) -> Result<()> {
    let protocol = if config.ws_port == 443 { "wss" } else { "ws" };
    let url = format!(
        "{}://{}:{}/app/{}",
        protocol, config.ws_host, config.ws_port, config.app_key
    );

    debug!("Client {} connecting to {}", id, url);

    // Connect to WebSocket
    let (ws_stream, _) = match connect_async(&url).await {
        Ok(result) => result,
        Err(e) => {
            error!("Client {} failed to connect: {}", id, e);
            metrics.connection_errors.fetch_add(1, Ordering::Relaxed);
            return Err(e.into());
        }
    };

    metrics.active_connections.fetch_add(1, Ordering::Relaxed);
    debug!("Client {} connected successfully", id);

    let (mut write, mut read) = ws_stream.split();

    let mut subscribe_time: Option<Instant> = None;
    let mut update_time: Option<Instant> = None;
    let mut subscribed = false;
    let mut is_updating = false;

    // Scenario 2: Setup periodic filter updates
    let mut filter_update_timer = if config.scenario == 2 {
        Some(interval(Duration::from_millis(
            config.filter_update_interval,
        )))
    } else {
        None
    };

    loop {
        tokio::select! {
            // Handle shutdown signal
            _ = shutdown.recv() => {
                debug!("Client {} received shutdown signal", id);
                break;
            }

            // Handle filter updates (Scenario 2)
            Some(_) = async {
                match &mut filter_update_timer {
                    Some(timer) => Some(timer.tick().await),
                    None => None,
                }
            } => {
                if subscribed {
                    let filter = build_filter(config.scenario, &tokens);
                    let subscribe_msg = SubscribeMessage {
                        event: "pusher:subscribe".to_string(),
                        data: SubscribeData {
                            channel: config.channel.clone(),
                            filter,
                        },
                    };

                    update_time = Some(Instant::now());
                    is_updating = true;

                    if let Ok(json) = sonic_rs::to_string(&subscribe_msg) {
                        if let Err(e) = write.send(Message::Text(json)).await {
                            error!("Client {} failed to send filter update: {}", id, e);
                            break;
                        }
                    }
                }
            }

            // Handle incoming messages
            msg = read.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        // Handle raw ping
                        if text == "ping" {
                            if let Err(e) = write.send(Message::Text("pong".to_string())).await {
                                error!("Client {} failed to send pong: {}", id, e);
                                break;
                            }
                            continue;
                        }

                        // Parse Pusher message
                        let pusher_msg: PusherMessage = match sonic_rs::from_str(&text) {
                            Ok(msg) => msg,
                            Err(e) => {
                                debug!("Client {} failed to parse message: {} - Raw: {}", id, e, text);
                                continue;
                            }
                        };

                        debug!("Client {} received event: {}", id, pusher_msg.event);

                        match pusher_msg.event.as_str() {
                            "pusher:ping" => {
                                let pong = PongMessage {
                                    event: "pusher:pong".to_string(),
                                    data: sonic_rs::json!({}),
                                };
                                if let Ok(json) = sonic_rs::to_string(&pong) {
                                    if let Err(e) = write.send(Message::Text(json)).await {
                                        error!("Client {} failed to send pusher:pong: {}", id, e);
                                        break;
                                    }
                                }
                            }

                            "pusher:connection_established" => {
                                debug!("Client {} connection established", id);
                                let filter = build_filter(config.scenario, &tokens);
                                let subscribe_msg = SubscribeMessage {
                                    event: "pusher:subscribe".to_string(),
                                    data: SubscribeData {
                                        channel: config.channel.clone(),
                                        filter,
                                    },
                                };

                                subscribe_time = Some(Instant::now());

                                if let Ok(json) = sonic_rs::to_string(&subscribe_msg) {
                                    if let Err(e) = write.send(Message::Text(json)).await {
                                        error!("Client {} failed to subscribe: {}", id, e);
                                        break;
                                    }
                                }
                            }

                            "pusher_internal:subscription_succeeded" => {
                                if is_updating {
                                    // Filter update response
                                    if let Some(start) = update_time {
                                        let latency = start.elapsed().as_millis() as u64;
                                        metrics.filter_update_latency.lock().await
                                            .record(latency)
                                            .ok();
                                        metrics.filter_updates.fetch_add(1, Ordering::Relaxed);
                                    }
                                    is_updating = false;
                                } else {
                                    // Initial subscription
                                    if let Some(start) = subscribe_time {
                                        let latency = start.elapsed().as_millis() as u64;
                                        metrics.subscribe_latency.lock().await
                                            .record(latency)
                                            .ok();
                                        metrics.subscribe_success.fetch_add(1, Ordering::Relaxed);
                                        subscribed = true;
                                        debug!("Client {} subscribed successfully", id);
                                    }
                                }
                            }

                            "pusher:error" => {
                                error!("Client {} subscription error: {:?}", id, pusher_msg.data);
                                metrics.subscribe_failed.fetch_add(1, Ordering::Relaxed);
                            }

                            _ => {
                                // Channel message
                                if subscribed && pusher_msg.channel.as_ref() == Some(&config.channel) {
                                    metrics.messages_received.fetch_add(1, Ordering::Relaxed);

                                    // Calculate E2E latency
                                    let mut send_timestamp: Option<u64> = None;

                                    // Debug: Log the message structure for the first few messages
                                    if metrics.messages_received.load(Ordering::Relaxed) <= 3 {
                                        info!("Client {} received message - Event: {}, Tags: {:?}, Data: {:?}",
                                            id, pusher_msg.event, pusher_msg.tags, pusher_msg.data);
                                    }

                                    // Check root-level tags
                                    if let Some(tags) = &pusher_msg.tags {
                                        if let Some(ts) = tags.get("timestamp") {
                                            // Try as u64 first, then as string
                                            send_timestamp = ts.as_u64().or_else(|| {
                                                ts.as_str().and_then(|s| s.parse::<u64>().ok())
                                            });
                                            debug!("Client {} found timestamp in root tags: {:?}", id, ts);
                                        } else {
                                            debug!("Client {} tags present but no timestamp field. Tags: {:?}", id, tags);
                                        }
                                    } else {
                                        debug!("Client {} no root-level tags", id);
                                    }

                                    // Fallback: check inside data
                                    if send_timestamp.is_none() {
                                        if let Some(data) = &pusher_msg.data {
                                            if let Some(tags) = data.get("tags") {
                                                if let Some(ts) = tags.get("timestamp") {
                                                    // Try as u64 first, then as string
                                                    send_timestamp = ts.as_u64().or_else(|| {
                                                        ts.as_str().and_then(|s| s.parse::<u64>().ok())
                                                    });
                                                    debug!("Client {} found timestamp in data.tags: {:?}", id, ts);
                                                }
                                            } else if let Some(ts) = data.get("timestamp") {
                                                // Try as u64 first, then as string
                                                send_timestamp = ts.as_u64().or_else(|| {
                                                    ts.as_str().and_then(|s| s.parse::<u64>().ok())
                                                });
                                                debug!("Client {} found timestamp in data: {:?}", id, ts);
                                            }
                                        }
                                    }

                                    if let Some(ts) = send_timestamp {
                                        let now = std::time::SystemTime::now()
                                            .duration_since(std::time::UNIX_EPOCH)
                                            .unwrap()
                                            .as_millis() as u64;

                                        let latency = now.saturating_sub(ts);

                                        // Sanity check: ignore if > 60s
                                        if latency < 60_000 {
                                            metrics.e2e_latency.lock().await
                                                .record(latency)
                                                .ok();
                                            debug!("Client {} recorded E2E latency: {}ms", id, latency);
                                        } else {
                                            warn!("Client {} E2E latency too high ({}ms), ignoring", id, latency);
                                        }
                                    } else {
                                        // Only log for first few messages to avoid spam
                                        if metrics.messages_received.load(Ordering::Relaxed) <= 3 {
                                            warn!("Client {} no timestamp found in message", id);
                                        }
                                    }
                                }
                            }
                        }
                    }

                    Some(Ok(Message::Close(_))) => {
                        debug!("Client {} received close frame", id);
                        break;
                    }

                    Some(Err(e)) => {
                        error!("Client {} WebSocket error: {}", id, e);
                        metrics.connection_errors.fetch_add(1, Ordering::Relaxed);
                        break;
                    }

                    None => {
                        debug!("Client {} stream ended", id);
                        break;
                    }

                    _ => {}
                }
            }
        }
    }

    metrics.active_connections.fetch_sub(1, Ordering::Relaxed);
    debug!("Client {} disconnected", id);

    Ok(())
}

// =============================================================================
// Ramping Schedule
// =============================================================================

async fn run_ramping_test(config: Arc<Config>, tokens: TokenPool, metrics: Metrics) -> Result<()> {
    let (shutdown_tx, _) = broadcast::channel::<()>(1);
    let mut tasks = Vec::new();

    info!("Starting ramping test");
    info!("Target: {} clients", config.num_clients);

    // Stage 1: Ramp up to target clients
    let stage_start = Instant::now();
    info!(
        "Stage 1: ramping to {} clients over {}s",
        config.num_clients, config.ramp_duration
    );

    let clients_per_second = config.num_clients as f64 / config.ramp_duration as f64;
    let mut spawned = 0;
    let mut last_log = Instant::now();

    while spawned < config.num_clients {
        let elapsed = stage_start.elapsed().as_secs_f64();
        let target_now = (clients_per_second * elapsed).min(config.num_clients as f64) as usize;

        while spawned < target_now {
            let client_config = Arc::clone(&config);
            let client_tokens = tokens.clone();
            let client_metrics = metrics.clone();
            let shutdown_rx = shutdown_tx.subscribe();

            let id = spawned;
            spawned += 1;

            let task = tokio::spawn(async move {
                run_client(
                    id,
                    client_config,
                    client_tokens,
                    client_metrics,
                    shutdown_rx,
                )
                .await
            });

            tasks.push(task);
        }

        // Sleep a bit before checking again
        sleep(Duration::from_millis(100)).await;

        // Log progress every 5 seconds
        if last_log.elapsed() >= Duration::from_secs(5) {
            let active = metrics.active_connections.load(Ordering::Relaxed);
            let received = metrics.messages_received.load(Ordering::Relaxed);
            info!(
                "Stage 1: spawned={}, active={}, messages_received={}",
                spawned, active, received
            );
            last_log = Instant::now();
        }
    }

    // Wait for remaining ramp time
    let remaining = config
        .ramp_duration
        .saturating_sub(stage_start.elapsed().as_secs());
    if remaining > 0 {
        sleep(Duration::from_secs(remaining)).await;
    }

    info!(
        "Stage 1 complete: {} clients spawned, {} active",
        spawned,
        metrics.active_connections.load(Ordering::Relaxed)
    );

    // Stage 2: Hold at target
    let stage_start = Instant::now();
    info!(
        "Stage 2: holding at {} clients for {}s",
        config.num_clients, config.hold_duration
    );

    let hold_interval = Duration::from_secs(5);
    let mut last_log = Instant::now();

    while stage_start.elapsed() < Duration::from_secs(config.hold_duration) {
        sleep(Duration::from_millis(500)).await;

        if last_log.elapsed() >= hold_interval {
            let active = metrics.active_connections.load(Ordering::Relaxed);
            let received = metrics.messages_received.load(Ordering::Relaxed);
            info!("Stage 2: active={}, messages_received={}", active, received);
            last_log = Instant::now();
        }
    }

    info!(
        "Stage 2 complete: {} active",
        metrics.active_connections.load(Ordering::Relaxed)
    );

    // Stage 3: Ramp down
    info!("Stage 3: ramping down over {}s", config.ramp_down_duration);

    // Signal shutdown to all clients
    shutdown_tx.send(()).ok();

    // Wait for graceful shutdown
    info!("Waiting for graceful shutdown (max 30s)");
    tokio::select! {
        _ = sleep(Duration::from_secs(30)) => {
            info!("Graceful shutdown timeout reached");
        }
        _ = async {
            futures_util::future::join_all(tasks).await;
        } => {
            info!("All tasks completed before timeout");
        }
    }

    info!(
        "Stage 3 complete: {} active",
        metrics.active_connections.load(Ordering::Relaxed)
    );

    Ok(())
}

// =============================================================================
// Main
// =============================================================================

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let config = Arc::new(Config::parse());

    // Print banner
    info!("════════════════════════════════════════════════════════════");
    info!("           TAG FILTERING BENCHMARK (Rust)");
    info!("════════════════════════════════════════════════════════════");
    info!("Scenario: {}", config.scenario);
    info!("Target Clients: {}", config.num_clients);
    info!("Ramp Duration: {}s", config.ramp_duration);
    info!("Hold Duration: {}s", config.hold_duration);
    info!(
        "WebSocket: {}://{}:{}",
        if config.ws_port == 443 { "wss" } else { "ws" },
        config.ws_host,
        config.ws_port
    );
    info!("Channel: {}", config.channel);

    let scenario_descriptions = [
        "Single random token_address per client (eq)",
        "Single token + periodic filter UPDATES",
        "10 random token_addresses per client (IN)",
        "100 random token_addresses per client (IN)",
        "500 random token_addresses per client (IN)",
    ];

    if config.scenario >= 1 && config.scenario <= 5 {
        info!(
            "Description: {}",
            scenario_descriptions[config.scenario as usize - 1]
        );
    }

    if config.scenario == 2 {
        info!(
            "Filter update interval: {}ms",
            config.filter_update_interval
        );
    }

    // Load tokens
    let tokens = if config.token_file.exists() {
        TokenPool::load_from_file(&config.token_file)?
    } else {
        TokenPool::generate_fake(10_000)
    };

    info!("Token addresses: {}", tokens.addresses.len());
    info!("════════════════════════════════════════════════════════════");

    // Initialize metrics
    let metrics = Metrics::new();

    // Run test
    run_ramping_test(Arc::clone(&config), tokens, metrics.clone()).await?;

    // Print summary
    metrics.print_summary().await;

    info!("════════════════════════════════════════════════════════════");
    info!("                 BENCHMARK COMPLETE");
    info!("════════════════════════════════════════════════════════════");

    Ok(())
}

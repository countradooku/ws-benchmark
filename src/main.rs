use anyhow::Result;
use clap::Parser;
use futures_util::{SinkExt, StreamExt};
use hdrhistogram::Histogram;
use rand::prelude::IndexedRandom;
use serde::{Deserialize, Serialize};
use sonic_rs::JsonValueTrait;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::broadcast;
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

    /// Target number of clients
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

    /// Client ID offset for multi-machine benchmarking
    #[arg(long, env = "CLIENT_ID_OFFSET", default_value = "0")]
    client_id_offset: usize,

    /// Warm-up duration in seconds (metrics discarded during this phase)
    #[arg(long, env = "WARMUP_DURATION", default_value = "0")]
    warmup_duration: u64,
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
// Per-Client Results (Lock-Free)
// =============================================================================

struct ClientResult {
    subscribe_latency_ms: Option<u64>,
    filter_update_latencies: Vec<u64>,
    e2e_latencies: Vec<u64>,
    messages_received: u64,
    messages_received_during_warmup: u64,
    connected: bool,
    subscribe_success: bool,
    connection_error: bool,
}

impl ClientResult {
    fn new() -> Self {
        Self {
            subscribe_latency_ms: None,
            filter_update_latencies: Vec::with_capacity(64),
            e2e_latencies: Vec::with_capacity(10000),
            messages_received: 0,
            messages_received_during_warmup: 0,
            connected: false,
            subscribe_success: false,
            connection_error: false,
        }
    }
}

// =============================================================================
// Global Atomic Counters (for live stats only)
// =============================================================================

#[derive(Clone)]
struct LiveStats {
    active_connections: Arc<AtomicUsize>,
    messages_received: Arc<AtomicU64>,
    subscribe_success: Arc<AtomicU64>,
    connection_errors: Arc<AtomicU64>,
    warmup_complete: Arc<std::sync::atomic::AtomicBool>,
}

impl LiveStats {
    fn new() -> Self {
        Self {
            active_connections: Arc::new(AtomicUsize::new(0)),
            messages_received: Arc::new(AtomicU64::new(0)),
            subscribe_success: Arc::new(AtomicU64::new(0)),
            connection_errors: Arc::new(AtomicU64::new(0)),
            warmup_complete: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
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
        let content = std::fs::read_to_string(path)?;
        let addresses: Vec<String> = sonic_rs::from_str(&content)?;
        info!("Loaded {} token addresses", addresses.len());
        Ok(Self {
            addresses: Arc::new(addresses),
        })
    }

    fn generate_fake(count: usize) -> Self {
        let addresses: Vec<String> = (0..count).map(|i| format!("token_{:08x}", i)).collect();
        Self {
            addresses: Arc::new(addresses),
        }
    }

    fn get_random(&self) -> String {
        let mut rng = rand::rng();
        self.addresses.choose(&mut rng).unwrap().clone()
    }

    fn get_random_unique(&self, count: usize) -> Vec<String> {
        let mut rng = rand::rng();
        let count = count.min(self.addresses.len());
        let indices: Vec<usize> = (0..self.addresses.len())
            .collect::<Vec<_>>()
            .choose_multiple(&mut rng, count)
            .copied()
            .collect();
        indices.iter().map(|&i| self.addresses[i].clone()).collect()
    }
}

// =============================================================================
// Filter Building
// =============================================================================

#[inline]
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
// Timestamp extraction (inlined for speed)
// =============================================================================

#[inline(always)]
fn extract_timestamp(pusher_msg: &PusherMessage) -> Option<u64> {
    // Check root-level tags first
    if let Some(tags) = &pusher_msg.tags {
        if let Some(ts) = tags.get("timestamp") {
            if let Some(v) = ts.as_u64() {
                return Some(v);
            }
            if let Some(s) = ts.as_str() {
                if let Ok(v) = s.parse::<u64>() {
                    return Some(v);
                }
            }
        }
    }

    // Fallback: check inside data
    if let Some(data) = &pusher_msg.data {
        if let Some(tags) = data.get("tags") {
            if let Some(ts) = tags.get("timestamp") {
                if let Some(v) = ts.as_u64() {
                    return Some(v);
                }
                if let Some(s) = ts.as_str() {
                    if let Ok(v) = s.parse::<u64>() {
                        return Some(v);
                    }
                }
            }
        }
        if let Some(ts) = data.get("timestamp") {
            if let Some(v) = ts.as_u64() {
                return Some(v);
            }
            if let Some(s) = ts.as_str() {
                if let Ok(v) = s.parse::<u64>() {
                    return Some(v);
                }
            }
        }
    }

    None
}

// =============================================================================
// WebSocket Client (returns results, no shared locks)
// =============================================================================

async fn run_client(
    id: usize,
    config: Arc<Config>,
    tokens: TokenPool,
    live_stats: LiveStats,
    mut shutdown: broadcast::Receiver<()>,
) -> ClientResult {
    let mut result = ClientResult::new();

    // Check if we should record metrics (after warmup)
    let should_record = || live_stats.warmup_complete.load(Ordering::Relaxed);

    let protocol = if config.ws_port == 443 { "wss" } else { "ws" };
    let url = format!(
        "{}://{}:{}/app/{}",
        protocol, config.ws_host, config.ws_port, config.app_key
    );

    debug!("Client {} connecting to {}", id, url);

    // Connect to WebSocket
    let (ws_stream, _) = match connect_async(&url).await {
        Ok(r) => r,
        Err(e) => {
            error!("Client {} failed to connect: {}", id, e);
            live_stats.connection_errors.fetch_add(1, Ordering::Relaxed);
            result.connection_error = true;
            return result;
        }
    };

    result.connected = true;
    live_stats
        .active_connections
        .fetch_add(1, Ordering::Relaxed);
    debug!("Client {} connected successfully", id);

    let (mut write, mut read) = ws_stream.split();

    let mut subscribe_time: Option<Instant> = None;
    let mut update_time: Option<Instant> = None;
    let mut subscribed = false;
    let mut is_updating = false;
    let mut logged_first_message = false;

    // Scenario 2: Setup periodic filter updates
    let mut filter_update_timer = if config.scenario == 2 {
        Some(interval(Duration::from_millis(
            config.filter_update_interval,
        )))
    } else {
        None
    };

    // Pre-serialize pong message
    let pong_json = sonic_rs::to_string(&PongMessage {
        event: "pusher:pong".to_string(),
        data: sonic_rs::json!({}),
    })
    .unwrap();

    loop {
        tokio::select! {
            biased;

            // Handle shutdown signal (high priority)
            _ = shutdown.recv() => {
                debug!("Client {} received shutdown signal", id);
                break;
            }

            // Handle incoming messages (highest throughput path)
            msg = read.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        // Handle raw ping
                        if text == "ping" {
                            let _ = write.send(Message::Text("pong".to_string())).await;
                            continue;
                        }

                        // Parse Pusher message
                        let pusher_msg: PusherMessage = match sonic_rs::from_str(&text) {
                            Ok(msg) => msg,
                            Err(_) => continue,
                        };

                        match pusher_msg.event.as_str() {
                            "pusher:ping" => {
                                let _ = write.send(Message::Text(pong_json.clone())).await;
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
                                    if let Some(start) = update_time {
                                        if should_record() {
                                            result.filter_update_latencies.push(start.elapsed().as_millis() as u64);
                                        }
                                    }
                                    is_updating = false;
                                } else {
                                    if let Some(start) = subscribe_time {
                                        result.subscribe_latency_ms = Some(start.elapsed().as_millis() as u64);
                                        result.subscribe_success = true;
                                        live_stats.subscribe_success.fetch_add(1, Ordering::Relaxed);
                                        subscribed = true;
                                        debug!("Client {} subscribed successfully", id);
                                    }
                                }
                            }

                            "pusher:error" => {
                                error!("Client {} subscription error: {:?}", id, pusher_msg.data);
                            }

                            _ => {
                                // Channel message - hot path
                                if subscribed && pusher_msg.channel.as_ref() == Some(&config.channel) {
                                    live_stats.messages_received.fetch_add(1, Ordering::Relaxed);

                                    // Log first message for debugging
                                    if !logged_first_message {
                                        info!("Client {} first message - Event: {}, Tags: {:?}",
                                            id, pusher_msg.event, pusher_msg.tags);
                                        logged_first_message = true;
                                    }

                                    // Only record metrics after warmup
                                    if should_record() {
                                        result.messages_received += 1;

                                        // Extract and record E2E latency
                                        if let Some(ts) = extract_timestamp(&pusher_msg) {
                                            let now = std::time::SystemTime::now()
                                                .duration_since(std::time::UNIX_EPOCH)
                                                .unwrap()
                                                .as_millis() as u64;

                                            let latency = now.saturating_sub(ts);

                                            // Sanity check: ignore if > 60s
                                            if latency < 60_000 {
                                                result.e2e_latencies.push(latency);
                                            }
                                        }
                                    } else {
                                        result.messages_received_during_warmup += 1;
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
                        result.connection_error = true;
                        break;
                    }

                    None => {
                        debug!("Client {} stream ended", id);
                        break;
                    }

                    _ => {}
                }
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
        }
    }

    live_stats
        .active_connections
        .fetch_sub(1, Ordering::Relaxed);
    debug!("Client {} disconnected", id);

    result
}

// =============================================================================
// Aggregate Results
// =============================================================================

fn aggregate_results(results: Vec<ClientResult>) {
    let mut subscribe_hist = Histogram::<u64>::new_with_bounds(1, 60_000, 3).unwrap();
    let mut filter_hist = Histogram::<u64>::new_with_bounds(1, 60_000, 3).unwrap();
    let mut e2e_hist = Histogram::<u64>::new_with_bounds(1, 60_000, 3).unwrap();

    let mut total_messages: u64 = 0;
    let mut subscribe_success: u64 = 0;
    let mut subscribe_failed: u64 = 0;
    let mut connection_errors: u64 = 0;
    let mut filter_updates: u64 = 0;

    for r in results {
        total_messages += r.messages_received;

        if r.connection_error {
            connection_errors += 1;
        } else if r.subscribe_success {
            subscribe_success += 1;
            if let Some(lat) = r.subscribe_latency_ms {
                let _ = subscribe_hist.record(lat);
            }
        } else if r.connected {
            subscribe_failed += 1;
        } else {
            connection_errors += 1;
        }

        for lat in r.filter_update_latencies {
            let _ = filter_hist.record(lat);
            filter_updates += 1;
        }

        for lat in r.e2e_latencies {
            let _ = e2e_hist.record(lat);
        }
    }

    info!("╔════════════════════════════════════════════════════════════╗");
    info!("║                    BENCHMARK SUMMARY                       ║");
    info!("╚════════════════════════════════════════════════════════════╝");

    info!("");
    info!("Connection Metrics:");
    info!("  Subscribe Success:   {}", subscribe_success);
    info!("  Subscribe Failed:    {}", subscribe_failed);
    info!("  Connection Errors:   {}", connection_errors);
    info!("  Filter Updates:      {}", filter_updates);
    info!("  Messages Received:   {}", total_messages);

    info!("");
    info!("Subscribe Latency (ms):");
    if subscribe_hist.len() > 0 {
        info!("  Min:    {}", subscribe_hist.min());
        info!("  Mean:   {:.2}", subscribe_hist.mean());
        info!("  p50:    {}", subscribe_hist.value_at_quantile(0.50));
        info!("  p95:    {}", subscribe_hist.value_at_quantile(0.95));
        info!("  p99:    {}", subscribe_hist.value_at_quantile(0.99));
        info!("  Max:    {}", subscribe_hist.max());
    } else {
        info!("  No data");
    }

    if filter_hist.len() > 0 {
        info!("");
        info!("Filter Update Latency (ms):");
        info!("  Min:    {}", filter_hist.min());
        info!("  Mean:   {:.2}", filter_hist.mean());
        info!("  p50:    {}", filter_hist.value_at_quantile(0.50));
        info!("  p95:    {}", filter_hist.value_at_quantile(0.95));
        info!("  p99:    {}", filter_hist.value_at_quantile(0.99));
        info!("  Max:    {}", filter_hist.max());
    }

    info!("");
    info!("End-to-End Latency (ms):");
    if e2e_hist.len() > 0 {
        info!("  Min:    {}", e2e_hist.min());
        info!("  Mean:   {:.2}", e2e_hist.mean());
        info!("  p50:    {}", e2e_hist.value_at_quantile(0.50));
        info!("  p95:    {}", e2e_hist.value_at_quantile(0.95));
        info!("  p99:    {}", e2e_hist.value_at_quantile(0.99));
        info!("  Max:    {}", e2e_hist.max());
        info!("  Samples:{}", e2e_hist.len());
    } else {
        info!("  No data");
    }

    info!("");
    info!("════════════════════════════════════════════════════════════");
    info!("                  BENCHMARK COMPLETE");
    info!("════════════════════════════════════════════════════════════");
}

// =============================================================================
// Test Runner
// =============================================================================

async fn run_ramping_test(
    config: Arc<Config>,
    tokens: TokenPool,
    live_stats: LiveStats,
) -> Result<Vec<ClientResult>> {
    let (shutdown_tx, _) = broadcast::channel::<()>(1);
    let mut tasks = Vec::with_capacity(config.num_clients);

    info!("Starting ramping test");
    info!(
        "Target: {} clients (IDs {}-{})",
        config.num_clients,
        config.client_id_offset,
        config.client_id_offset + config.num_clients - 1
    );

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
            let client_stats = live_stats.clone();
            let shutdown_rx = shutdown_tx.subscribe();

            let id = config.client_id_offset + spawned;
            spawned += 1;

            let task = tokio::spawn(async move {
                run_client(id, client_config, client_tokens, client_stats, shutdown_rx).await
            });

            tasks.push(task);
        }

        // Sleep a bit before checking again
        sleep(Duration::from_millis(50)).await;

        // Log progress every 5 seconds
        if last_log.elapsed() >= Duration::from_secs(5) {
            let active = live_stats.active_connections.load(Ordering::Relaxed);
            let received = live_stats.messages_received.load(Ordering::Relaxed);
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
        live_stats.active_connections.load(Ordering::Relaxed)
    );

    // Stage 2: Warm-up phase (if configured)
    if config.warmup_duration > 0 {
        let stage_start = Instant::now();
        info!(
            "Stage 2: warming up for {}s (metrics discarded)",
            config.warmup_duration
        );

        let warmup_interval = Duration::from_secs(5);
        let mut last_log = Instant::now();

        while stage_start.elapsed() < Duration::from_secs(config.warmup_duration) {
            sleep(Duration::from_millis(500)).await;

            if last_log.elapsed() >= warmup_interval {
                let active = live_stats.active_connections.load(Ordering::Relaxed);
                let received = live_stats.messages_received.load(Ordering::Relaxed);
                info!(
                    "Warm-up: active={}, messages={} (discarding)",
                    active, received
                );
                last_log = Instant::now();
            }
        }

        info!("Warm-up complete, starting measurement phase");
    }

    // Mark warmup as complete - start recording metrics
    live_stats.warmup_complete.store(true, Ordering::Relaxed);

    // Stage 3: Hold at target (measurement phase)
    let stage_start = Instant::now();
    info!("Stage 3: measuring for {}s", config.hold_duration);

    let hold_interval = Duration::from_secs(5);
    let mut last_log = Instant::now();

    while stage_start.elapsed() < Duration::from_secs(config.hold_duration) {
        sleep(Duration::from_millis(500)).await;

        if last_log.elapsed() >= hold_interval {
            let active = live_stats.active_connections.load(Ordering::Relaxed);
            let received = live_stats.messages_received.load(Ordering::Relaxed);
            let success = live_stats.subscribe_success.load(Ordering::Relaxed);
            let errors = live_stats.connection_errors.load(Ordering::Relaxed);
            info!(
                "Stage 3: active={}, subscribed={}, errors={}, messages={}",
                active, success, errors, received
            );
            last_log = Instant::now();
        }
    }

    info!(
        "Stage 4 complete: {} active",
        live_stats.active_connections.load(Ordering::Relaxed)
    );

    // Stage 4: Ramp down
    info!("Stage 4: ramping down over {}s", config.ramp_down_duration);

    // Signal shutdown to all clients
    let _ = shutdown_tx.send(());

    // Collect all results
    info!("Collecting results from all clients...");
    let mut results = Vec::with_capacity(tasks.len());

    for task in tasks {
        match tokio::time::timeout(Duration::from_secs(10), task).await {
            Ok(Ok(result)) => results.push(result),
            Ok(Err(e)) => {
                warn!("Task join error: {}", e);
                results.push(ClientResult::new());
            }
            Err(_) => {
                warn!("Task timed out during collection");
                results.push(ClientResult::new());
            }
        }
    }

    info!(
        "Stage 3 complete: {} active",
        live_stats.active_connections.load(Ordering::Relaxed)
    );

    Ok(results)
}

// =============================================================================
// Main
// =============================================================================

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("ws_benchmark=info".parse().unwrap()),
        )
        .init();

    let config = Arc::new(Config::parse());

    info!("════════════════════════════════════════════════════════════");
    info!("              WebSocket Benchmark v2.0 (Lock-Free)");
    info!("════════════════════════════════════════════════════════════");
    info!("");
    info!("Configuration:");
    info!("  Host:           {}:{}", config.ws_host, config.ws_port);
    info!("  App Key:        {}", config.app_key);
    info!("  Channel:        {}", config.channel);
    info!("  Scenario:       {}", config.scenario);
    info!("  Num Clients:    {}", config.num_clients);
    info!("  Client Offset:  {}", config.client_id_offset);
    info!("  Ramp Duration:  {}s", config.ramp_duration);
    info!("  Warmup Duration:{}s", config.warmup_duration);
    info!("  Hold Duration:  {}s", config.hold_duration);
    info!("");

    // Load tokens
    let tokens = if config.token_file.exists() {
        TokenPool::load_from_file(&config.token_file)?
    } else {
        warn!(
            "Token file not found: {:?}, generating fake tokens",
            config.token_file
        );
        TokenPool::generate_fake(10000)
    };

    // Create live stats
    let live_stats = LiveStats::new();

    // Run the test and collect results
    let results = run_ramping_test(config, tokens, live_stats).await?;

    // Aggregate and print results (single-threaded, after all clients done)
    aggregate_results(results);

    Ok(())
}

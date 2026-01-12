#!/bin/bash

# WebSocket Benchmark Runner Script
# Runs all scenarios with different client counts

set -e

# Configuration
WS_HOST="${WS_HOST:-stream-v2.projectscylla.com}"
WS_PORT="${WS_PORT:-443}"
APP_KEY="${APP_KEY:-knife-library-likely}"
CHANNEL="${CHANNEL:-trident_filter_tokens_v1}"
TOKEN_FILE="${TOKEN_FILE:-token-addresses.json}"

# Test configuration
RAMP_DURATION=30
HOLD_DURATION=60
RAMP_DOWN_DURATION=10

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Results directory
RESULTS_DIR="benchmark_results_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$RESULTS_DIR"

echo -e "${BLUE}════════════════════════════════════════════════════════════${NC}"
echo -e "${BLUE}     WebSocket Tag Filtering Benchmark Suite${NC}"
echo -e "${BLUE}════════════════════════════════════════════════════════════${NC}"
echo ""
echo "Results will be saved to: $RESULTS_DIR"
echo "Configuration:"
echo "  Server: wss://${WS_HOST}:${WS_PORT}"
echo "  Channel: ${CHANNEL}"
echo "  Ramp Duration: ${RAMP_DURATION}s"
echo "  Hold Duration: ${HOLD_DURATION}s"
echo "  Ramp Down: ${RAMP_DOWN_DURATION}s"
echo ""

# Build the project first
echo -e "${YELLOW}Building project...${NC}"
cargo build --release
echo -e "${GREEN}✓ Build complete${NC}"
echo ""

# Function to run a single benchmark
run_benchmark() {
    local scenario=$1
    local num_clients=$2
    local scenario_name=$3

    local output_file="${RESULTS_DIR}/scenario${scenario}_${num_clients}clients.log"

    echo -e "${BLUE}────────────────────────────────────────────────────────────${NC}"
    echo -e "${YELLOW}Running: Scenario ${scenario} - ${scenario_name}${NC}"
    echo -e "${YELLOW}Clients: ${num_clients}${NC}"
    echo -e "${BLUE}────────────────────────────────────────────────────────────${NC}"

    cargo run --release -- \
        --ws-host "$WS_HOST" \
        --ws-port "$WS_PORT" \
        --app-key "$APP_KEY" \
        --channel "$CHANNEL" \
        --scenario "$scenario" \
        --num-clients "$num_clients" \
        --ramp-duration "$RAMP_DURATION" \
        --hold-duration "$HOLD_DURATION" \
        --ramp-down-duration "$RAMP_DOWN_DURATION" \
        2>&1 | tee "$output_file"

    if [ ${PIPESTATUS[0]} -eq 0 ]; then
        echo -e "${GREEN}✓ Scenario ${scenario} with ${num_clients} clients completed successfully${NC}"
    else
        echo -e "${RED}✗ Scenario ${scenario} with ${num_clients} clients failed${NC}"
    fi

    echo ""
    sleep 5  # Cool down between tests
}

# Scenario 1: Single random token_address per client (eq)
echo -e "${GREEN}═══════════════════════════════════════════════════════════${NC}"
echo -e "${GREEN} SCENARIO 1: Mass Subscribe with unique subscription tags${NC}"
echo -e "${GREEN} Description: Each client subscribes to a random token_address (eq)${NC}"
echo -e "${GREEN}═══════════════════════════════════════════════════════════${NC}"
echo ""
run_benchmark 1 1000 "Single token_address (eq)"
run_benchmark 1 5000 "Single token_address (eq)"
run_benchmark 1 10000 "Single token_address (eq)"

# Scenario 2: Mass subscription tag update
echo -e "${GREEN}═══════════════════════════════════════════════════════════${NC}"
echo -e "${GREEN} SCENARIO 2: Mass subscription tag update${NC}"
echo -e "${GREEN} Description: Each client updates their filter every 5 seconds${NC}"
echo -e "${GREEN}═══════════════════════════════════════════════════════════${NC}"
echo ""
run_benchmark 2 1000 "Filter updates every 5s"
run_benchmark 2 5000 "Filter updates every 5s"
run_benchmark 2 10000 "Filter updates every 5s"

# Scenario 3: 10 random token_addresses per client (IN)
echo -e "${GREEN}═══════════════════════════════════════════════════════════${NC}"
echo -e "${GREEN} SCENARIO 3: Mass Subscribe with 10 token_addresses${NC}"
echo -e "${GREEN} Description: Each client subscribes to 10 random token_addresses (IN)${NC}"
echo -e "${GREEN}═══════════════════════════════════════════════════════════${NC}"
echo ""
run_benchmark 3 1000 "10 token_addresses (IN)"
run_benchmark 3 5000 "10 token_addresses (IN)"
run_benchmark 3 10000 "10 token_addresses (IN)"

# Scenario 4: 100 random token_addresses per client (IN)
echo -e "${GREEN}═══════════════════════════════════════════════════════════${NC}"
echo -e "${GREEN} SCENARIO 4: Mass Subscribe with 100 token_addresses${NC}"
echo -e "${GREEN} Description: Each client subscribes to 100 random token_addresses (IN)${NC}"
echo -e "${GREEN}═══════════════════════════════════════════════════════════${NC}"
echo ""
run_benchmark 4 1000 "100 token_addresses (IN)"
run_benchmark 4 5000 "100 token_addresses (IN)"
run_benchmark 4 10000 "100 token_addresses (IN)"

# Scenario 5: 500 random token_addresses per client (IN)
echo -e "${GREEN}═══════════════════════════════════════════════════════════${NC}"
echo -e "${GREEN} SCENARIO 5: Mass Subscribe with 500 token_addresses${NC}"
echo -e "${GREEN} Description: Each client subscribes to 500 random token_addresses (IN)${NC}"
echo -e "${GREEN}═══════════════════════════════════════════════════════════${NC}"
echo ""
run_benchmark 5 1000 "500 token_addresses (IN)"
run_benchmark 5 5000 "500 token_addresses (IN)"
run_benchmark 5 10000 "500 token_addresses (IN)"

# Generate summary
echo -e "${BLUE}════════════════════════════════════════════════════════════${NC}"
echo -e "${BLUE}     Benchmark Suite Complete!${NC}"
echo -e "${BLUE}════════════════════════════════════════════════════════════${NC}"
echo ""
echo "Results saved in: $RESULTS_DIR"
echo ""
echo "Generating summary..."

# Create summary file
SUMMARY_FILE="${RESULTS_DIR}/summary.txt"
{
    echo "WebSocket Tag Filtering Benchmark Summary"
    echo "=========================================="
    echo "Date: $(date)"
    echo "Server: wss://${WS_HOST}:${WS_PORT}"
    echo "Channel: ${CHANNEL}"
    echo ""
    echo "Configuration:"
    echo "  Ramp Duration: ${RAMP_DURATION}s"
    echo "  Hold Duration: ${HOLD_DURATION}s"
    echo "  Ramp Down: ${RAMP_DOWN_DURATION}s"
    echo ""
    echo "Results:"
    echo "--------"
    echo ""

    for log_file in "$RESULTS_DIR"/*.log; do
        if [ -f "$log_file" ]; then
            filename=$(basename "$log_file")
            echo "=== $filename ==="

            # Extract key metrics
            grep -E "(Subscribe Success|Subscribe Failed|Connection Errors|Messages Received|Mean:|p95:|p99:)" "$log_file" || echo "No metrics found"
            echo ""
        fi
    done
} > "$SUMMARY_FILE"

echo -e "${GREEN}✓ Summary generated: ${SUMMARY_FILE}${NC}"
echo ""
echo "View individual results:"
ls -1 "$RESULTS_DIR"/*.log | while read file; do
    echo "  - $(basename "$file")"
done
echo ""
echo -e "${GREEN}All benchmarks completed successfully!${NC}"

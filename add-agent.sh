#!/bin/bash
set -e
# ═══════════════════════════════════════════════════════════════
# EdgeCitadel: Add an agent (run on the server)
#
# Usage: ./add-agent.sh <agent-id>
# Example: ./add-agent.sh us-claw-remote
#
# Prints the join command to run on the agent's machine.
# ═══════════════════════════════════════════════════════════════

RED='\033[0;31m'; GREEN='\033[0;32m'; CYAN='\033[0;36m'; NC='\033[0m'

AGENT_ID="${1:?Usage: ./add-agent.sh <agent-id>}"
AGENT_ID=$(echo "$AGENT_ID" | tr '[:upper:]' '[:lower:]' | tr ' ' '-')

# Read NATS token from .env
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NATS_TOKEN=""
if [[ -f "$SCRIPT_DIR/.env" ]]; then
    NATS_TOKEN=$(grep -E '^NATS_TOKEN=' "$SCRIPT_DIR/.env" | cut -d= -f2- | tr -d '"' | tr -d "'")
fi
if [[ -z "$NATS_TOKEN" ]]; then
    NATS_TOKEN="changeme"
fi

# Detect this machine's reachable address
# Priority: Tailscale IP > first non-loopback IPv4
if command -v tailscale &>/dev/null; then
    SERVER_HOST=$(tailscale ip -4 2>/dev/null || true)
fi
if [[ -z "$SERVER_HOST" ]]; then
    SERVER_HOST=$(hostname -I 2>/dev/null | awk '{print $1}')
fi
if [[ -z "$SERVER_HOST" ]]; then
    SERVER_HOST=$(ip -4 route get 1 2>/dev/null | awk '{print $7; exit}')
fi

echo ""
echo -e "${GREEN}Agent '${AGENT_ID}' ready to join.${NC}"
echo ""
echo "═══════════════════════════════════════════════════════════"
echo " Browser-based agent (openclaw-client over MQTT):"
echo "═══════════════════════════════════════════════════════════"
echo ""
echo "  git clone https://github.com/zhonghaozhan/EdgeCitadel.git"
echo "  cd EdgeCitadel && ./join.sh ${SERVER_HOST} ${NATS_TOKEN} ${AGENT_ID}"
echo ""
echo "═══════════════════════════════════════════════════════════"
echo " Python adapter (gemma / watchdog / hermes / etc. over NATS):"
echo "═══════════════════════════════════════════════════════════"
echo ""
echo "  Edit adapters/<name>/agent.env on the adapter host:"
echo "    NATS_URL=nats://${SERVER_HOST}:4222"
echo "    NATS_TOKEN=${NATS_TOKEN}"
echo "  Then: python -m adapters.<name>.adapter"
echo ""
echo "═══════════════════════════════════════════════════════════"
echo ""
echo -e " Agent ID:       ${AGENT_ID}"
echo -e " Broker (MQTT, browser):  ${SERVER_HOST}:1883"
echo -e " Broker (NATS, adapters): ${SERVER_HOST}:4222"
echo -e " NATS token:     ${NATS_TOKEN}"
echo ""

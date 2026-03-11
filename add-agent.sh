#!/bin/bash
set -e
# ═══════════════════════════════════════════════════════════════
# EdgeCitadel: Add an agent (run on the server)
#
# Usage: ./add-agent.sh <agent-id>
# Example: ./add-agent.sh us-claw-remote
#
# Prints the join command to run on the agent's machine.
# NATS does not require per-agent credentials by default.
# ═══════════════════════════════════════════════════════════════

RED='\033[0;31m'; GREEN='\033[0;32m'; CYAN='\033[0;36m'; NC='\033[0m'

AGENT_ID="${1:?Usage: ./add-agent.sh <agent-id>}"
AGENT_ID=$(echo "$AGENT_ID" | tr '[:upper:]' '[:lower:]' | tr ' ' '-')

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
echo " Run this on the agent's machine to join:"
echo "═══════════════════════════════════════════════════════════"
echo ""
echo "  git clone https://github.com/zhonghaozhan/EdgeCitadel.git"
echo "  cd EdgeCitadel && ./join.sh ${SERVER_HOST} ${AGENT_ID}"
echo ""
echo "═══════════════════════════════════════════════════════════"
echo ""
echo -e " Agent ID:      ${AGENT_ID}"
echo -e " NATS server:   ${SERVER_HOST}:4222"
echo ""

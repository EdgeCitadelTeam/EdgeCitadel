#!/bin/bash
set -e
# ═══════════════════════════════════════════════════════════════
# EdgeCitadel: Add an agent (run on the server)
#
# Usage: ./add-agent.sh <agent-id>
# Example: ./add-agent.sh us-claw-remote
#
# Creates MQTT credentials and prints the join command
# to run on the agent's machine.
# ═══════════════════════════════════════════════════════════════

RED='\033[0;31m'; GREEN='\033[0;32m'; CYAN='\033[0;36m'; NC='\033[0m'

AGENT_ID="${1:?Usage: ./add-agent.sh <agent-id>}"
AGENT_ID=$(echo "$AGENT_ID" | tr '[:upper:]' '[:lower:]' | tr ' ' '-')

# Generate a random password
MQTT_PASS=$(openssl rand -base64 18 | tr -d '/+=' | head -c 20)

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
echo -e "${CYAN}Creating MQTT user '${AGENT_ID}'...${NC}"

# Create the MQTT user in the running mosquitto container
docker compose exec -T mqtt mosquitto_passwd -b /mosquitto/config/passwd "$AGENT_ID" "$MQTT_PASS"

# Reload mosquitto to pick up new credentials (SIGHUP)
docker compose kill -s HUP mqtt 2>/dev/null || docker compose restart mqtt

echo -e "${GREEN}Done.${NC}"
echo ""
echo "═══════════════════════════════════════════════════════════"
echo " Run this on the agent's machine to join:"
echo "═══════════════════════════════════════════════════════════"
echo ""
echo "  git clone https://github.com/zhonghaozhan/EdgeCitadel.git"
echo "  cd EdgeCitadel && ./join.sh ${SERVER_HOST} ${MQTT_PASS}"
echo ""
echo "═══════════════════════════════════════════════════════════"
echo ""
echo -e " Agent ID:      ${AGENT_ID}"
echo -e " MQTT user:     ${AGENT_ID}"
echo -e " MQTT password: ${MQTT_PASS}"
echo -e " Server:        ${SERVER_HOST}:1883"
echo ""

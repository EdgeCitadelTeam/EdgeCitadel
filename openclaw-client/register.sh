#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONF_FILE="${OPENCLAW_CONF:-$SCRIPT_DIR/openclaw.conf}"

# -- Load config -------------------------------------------------------------
if [[ ! -f "$CONF_FILE" ]]; then
    echo "Config not found: $CONF_FILE"
    echo "   Copy openclaw.conf.example -> openclaw.conf and fill it in."
    exit 1
fi
source "$CONF_FILE"

# -- Validate required fields ------------------------------------------------
for var in DEPLOYMENT_NAME DEPLOYMENT_HOST DEPLOYMENT_PORT; do
    if [[ -z "${!var}" ]]; then
        echo "Required config missing: $var"
        exit 1
    fi
done
if [[ ${#AGGREGATORS[@]} -eq 0 ]]; then
    echo "AGGREGATORS array is empty in $CONF_FILE"
    exit 1
fi

# -- CLI argument parsing ----------------------------------------------------
TARGET=""
ACTION="register"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --target)       TARGET="$2"; shift 2 ;;
        --deregister)   ACTION="deregister"; shift ;;
        --list)         ACTION="list"; shift ;;
        --status)       ACTION="status"; shift ;;
        --help)
            echo "Usage: ./register.sh [--target <name>] [--deregister] [--list] [--status]"
            echo ""
            echo "  (no flags)            Register with all configured aggregators"
            echo "  --target <name>       Limit to one aggregator"
            echo "  --deregister          Remove from aggregators"
            echo "  --list                Show configured aggregators and key status"
            echo "  --status              Check live registration status"
            exit 0
            ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

# -- Helpers -----------------------------------------------------------------
get_api_key() {
    local name="$1"
    local key_var="API_KEY_$(echo "$name" | tr '[:lower:]-' '[:upper:]_')"
    echo "${!key_var}"
}

parse_aggregator() {
    AGG_NAME="${1%%=*}"
    AGG_URL="${1##*=}"
}

do_register() {
    local agg_name="$1" agg_url="$2" api_key="$3"
    echo "  -> Registering '$DEPLOYMENT_NAME' with '$agg_name' ($agg_url)..."
    local resp
    resp=$(curl -sf --max-time 10 \
        -X POST "$agg_url/api/deployments/register" \
        -H "Content-Type: application/json" \
        -H "api-key: $api_key" \
        -d "{
            \"name\":        \"$DEPLOYMENT_NAME\",
            \"host\":        \"$DEPLOYMENT_HOST\",
            \"port\":        $DEPLOYMENT_PORT,
            \"description\": \"$DEPLOYMENT_DESCRIPTION\"
        }" 2>&1) || { echo "  Could not reach $agg_url"; return 1; }
    echo "$resp" | jq -r '"  OK: " + .message' 2>/dev/null || echo "  OK: Registered"
}

do_deregister() {
    local agg_name="$1" agg_url="$2" api_key="$3"
    echo "  -> Deregistering '$DEPLOYMENT_NAME' from '$agg_name'..."
    curl -sf --max-time 10 \
        -X DELETE "$agg_url/api/deployments/$DEPLOYMENT_NAME" \
        -H "api-key: $api_key" \
        | jq -r '"  OK: " + .message' 2>/dev/null || echo "  OK: Removed"
}

do_status() {
    local agg_name="$1" agg_url="$2" api_key="$3"
    local result
    result=$(curl -sf --max-time 5 \
        "$agg_url/api/deployments/$DEPLOYMENT_NAME/status" \
        -H "api-key: $api_key" 2>/dev/null) \
        || { echo "  [$agg_name] unreachable"; return; }
    local status
    status=$(echo "$result" | jq -r '.status // "unknown"')
    echo "  [$agg_name] $agg_url -> $status"
}

run_on_targets() {
    local action_fn="$1"
    for entry in "${AGGREGATORS[@]}"; do
        parse_aggregator "$entry"
        if [[ -n "$TARGET" && "$AGG_NAME" != "$TARGET" ]]; then
            continue
        fi
        local api_key
        api_key=$(get_api_key "$AGG_NAME")
        if [[ -z "$api_key" ]]; then
            echo "  No API key for '$AGG_NAME' -- skipping"
            echo "     (set API_KEY_$(echo "$AGG_NAME" | tr '[:lower:]-' '[:upper:]_') in openclaw.conf)"
            continue
        fi
        $action_fn "$AGG_NAME" "$AGG_URL" "$api_key"
    done
}

# -- Actions -----------------------------------------------------------------
case "$ACTION" in
    register)
        echo "Registering '$DEPLOYMENT_NAME' ($DEPLOYMENT_HOST:$DEPLOYMENT_PORT)"
        [[ -n "$TARGET" ]] && echo "   Target: $TARGET" || echo "   Target: all aggregators"
        echo ""
        run_on_targets do_register
        ;;
    deregister)
        echo "Deregistering '$DEPLOYMENT_NAME'"
        [[ -n "$TARGET" ]] && echo "   Target: $TARGET" || echo "   Target: all aggregators"
        echo ""
        run_on_targets do_deregister
        ;;
    list)
        echo "Configured aggregators for '$DEPLOYMENT_NAME':"
        echo ""
        for entry in "${AGGREGATORS[@]}"; do
            parse_aggregator "$entry"
            key=$(get_api_key "$AGG_NAME")
            key_status=$([[ -n "$key" ]] && echo "key set" || echo "no key")
            echo "  $AGG_NAME -> $AGG_URL  ($key_status)"
        done
        ;;
    status)
        echo "Registration status for '$DEPLOYMENT_NAME':"
        echo ""
        run_on_targets do_status
        ;;
esac

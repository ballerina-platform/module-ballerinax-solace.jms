#!/bin/bash
# Script to initialize Solace queues for testing using SEMP API
# Configures queues with guaranteed messaging support for transacted sessions

set -e

SOLACE_URL="http://localhost:8080"
SEMP_URL="${SOLACE_URL}/SEMP/v2/config"
MONITOR_URL="${SOLACE_URL}/SEMP/v2/monitor"
VPN="default"
AUTH="admin:admin"

# Poll the monitor API for state == "up" before provisioning anything.
echo "Waiting for Solace message VPN '$VPN' to become operational..."
vpn_up=false
for i in $(seq 1 60); do
    state=$(curl -s -u "$AUTH" "$MONITOR_URL/msgVpns/$VPN?select=state" 2>/dev/null \
        | grep -o '"state"[[:space:]]*:[[:space:]]*"[^"]*"' | grep -o '[^"]*"$' | tr -d '"')
    if [ "$state" = "up" ]; then
        echo "Message VPN is up after ~$((i * 3))s"
        vpn_up=true
        break
    fi
    sleep 3
done

if [ "$vpn_up" != "true" ]; then
    echo "ERROR: Message VPN did not report 'up' within the timeout."
    exit 1
fi

sleep 5

# Function to create a queue with guaranteed messaging support. Retries a few times: the SEMP
# API can report the broker as reachable before its message-spool subsystem is actually ready
# to accept queue creation.
create_queue() {
    local queue_name=$1
    echo "Creating queue: $queue_name"

    for _ in 1 2 3 4 5; do
        response=$(curl -s -X POST "${SEMP_URL}/msgVpns/${VPN}/queues" \
            -u "${AUTH}" \
            -H "Content-Type: application/json" \
            -d "{
                \"queueName\": \"${queue_name}\",
                \"accessType\": \"exclusive\",
                \"permission\": \"delete\",
                \"ingressEnabled\": true,
                \"egressEnabled\": true,
                \"maxMsgSpoolUsage\": 100,
                \"respectTtlEnabled\": true
            }")
        if ! echo "$response" | grep -q '"error"'; then
            break
        fi
        sleep 3
    done

    if echo "$response" | grep -q '"error"'; then
        echo "ERROR: Failed to create queue '$queue_name' after 5 attempts: $response"
        exit 1
    fi
}

# Note: Compression is enabled by default on the Solace broker on port 55003
# The SMF service compression is already available, no additional configuration needed

# Note: Queues are configured for guaranteed messaging (persistent delivery)
# This enables support for transacted sessions which require guaranteed transport

# Create queues
create_queue "test-queue"
create_queue "test-transacted-queue"
create_queue "producer-ttl-queue"

echo "Solace initialization completed!"

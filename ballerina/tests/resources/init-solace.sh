#!/bin/bash
# Script to initialize Solace queues for testing using SEMP API
# Configures queues with guaranteed messaging support for transacted sessions

set -e

SOLACE_URL="http://localhost:8080"
SEMP_URL="${SOLACE_URL}/SEMP/v2/config"
VPN="default"
AUTH="admin:admin"

# Wait until the SEMP API is actually ready rather than sleeping a fixed interval. Provisioning an
# unready broker silently failed before (responses were discarded with `|| true`), so the queues
# were never created and every queue-dependent test failed with "Queue Not Found".
echo "Waiting for Solace broker to be ready..."
ready=false
for _ in $(seq 1 60); do
    if [ "$(curl -s -o /dev/null -w '%{http_code}' -u "${AUTH}" "${SEMP_URL}/about")" = "200" ]; then
        ready=true
        break
    fi
    sleep 5
done
if [ "$ready" != "true" ]; then
    echo "ERROR: Solace SEMP API at ${SEMP_URL} did not become ready within the timeout." >&2
    exit 1
fi

# Issues a SEMP POST and fails loudly on a real error. A 2xx is success; a 400 ALREADY_EXISTS is
# treated as success so the script is idempotent across re-runs against a persistent broker.
# The SEMP /about endpoint comes up before the message-spool (guaranteed-messaging) subsystem, so
# queue creation can transiently return MESSAGE_SPOOL_DATA_NOT_AVAILABLE; retry on that until the
# spool is ready rather than failing the whole provisioning step.
semp_post() {
    local url=$1
    local payload=$2
    local description=$3
    local response http_code body attempt
    for attempt in $(seq 1 24); do
        response=$(curl -sS -X POST "$url" -u "${AUTH}" -H "Content-Type: application/json" \
            -d "$payload" -w $'\n%{http_code}' 2>/dev/null)
        http_code=$(printf '%s' "$response" | tail -n1)
        body=$(printf '%s' "$response" | sed '$d')
        if [ "$http_code" -ge 200 ] && [ "$http_code" -lt 300 ]; then
            echo "  created: ${description}"
            return 0
        elif [ "$http_code" = "400" ] && printf '%s' "$body" | grep -qi "already exists"; then
            echo "  exists:  ${description}"
            return 0
        elif [ "$http_code" = "000" ] || printf '%s' "$body" | grep -qiE "SPOOL_DATA_NOT_AVAILABLE|not available|not ready"; then
            # Management plane is up but the message-spool is still initializing; wait and retry.
            sleep 5
        else
            echo "ERROR (HTTP ${http_code}) while creating ${description}:" >&2
            printf '%s\n' "$body" >&2
            exit 1
        fi
    done
    echo "ERROR: ${description} did not succeed after retries (message spool not ready)." >&2
    exit 1
}

# Function to create a queue with guaranteed messaging support
create_queue() {
    local queue_name=$1
    semp_post "${SEMP_URL}/msgVpns/${VPN}/queues" \
        "{
            \"queueName\": \"${queue_name}\",
            \"accessType\": \"exclusive\",
            \"permission\": \"delete\",
            \"ingressEnabled\": true,
            \"egressEnabled\": true,
            \"maxMsgSpoolUsage\": 100,
            \"respectTtlEnabled\": true
        }" \
        "queue ${queue_name}"
}

# Note: Compression is enabled by default on the Solace broker on port 55003
# The SMF service compression is already available, no additional configuration needed

# Note: Queues are configured for guaranteed messaging (persistent delivery)
# This enables support for transacted sessions which require guaranteed transport

# Function to add a topic subscription to a queue (topic-to-queue mapping)
add_queue_subscription() {
    local queue_name=$1
    local topic=$2
    semp_post "${SEMP_URL}/msgVpns/${VPN}/queues/${queue_name}/subscriptions" \
        "{
            \"subscriptionTopic\": \"${topic}\"
        }" \
        "subscription '${topic}' on queue ${queue_name}"
}

# Create queues
create_queue "test-queue"
create_queue "test-transacted-queue"

# Queue + topic-to-queue mapping used by the SMF persistent publisher tests
create_queue "smf-test-queue"
add_queue_subscription "smf-test-queue" "smf/test/persistent"

# Queues + topic-to-queue mappings used by the SMF receiver, settlement, and service tests
create_queue "smf-receiver-queue"
add_queue_subscription "smf-receiver-queue" "smf/test/receiver"
create_queue "smf-settlement-queue"
add_queue_subscription "smf-settlement-queue" "smf/test/settlement"
create_queue "smf-rejected-queue"
add_queue_subscription "smf-rejected-queue" "smf/test/rejected"
create_queue "smf-service-queue"
add_queue_subscription "smf-service-queue" "smf/test/service"
create_queue "smf-autoack-queue"
add_queue_subscription "smf-autoack-queue" "smf/test/autoack"

# Replay log (VPN-wide) + queue used by the SMF message replay tests.
# The replay log must exist before the test messages are published.
semp_post "${SEMP_URL}/msgVpns/${VPN}/replayLogs" \
    "{
        \"replayLogName\": \"test-replay-log\",
        \"ingressEnabled\": true,
        \"egressEnabled\": true,
        \"maxSpoolUsage\": 50
    }" \
    "replay log test-replay-log"
create_queue "smf-replay-queue"
add_queue_subscription "smf-replay-queue" "smf/test/replay"

echo "Solace initialization completed!"

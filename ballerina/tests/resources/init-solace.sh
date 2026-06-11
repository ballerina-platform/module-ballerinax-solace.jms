#!/bin/bash
# Script to initialize Solace queues for testing using SEMP API
# Configures queues with guaranteed messaging support for transacted sessions

set -e

echo "Waiting for Solace broker to be ready..."
sleep 10

SOLACE_URL="http://localhost:8080"
SEMP_URL="${SOLACE_URL}/SEMP/v2/config"
VPN="default"
AUTH="admin:admin"

# Function to create a queue with guaranteed messaging support
create_queue() {
    local queue_name=$1
    echo "Creating queue: $queue_name"

    curl -X POST "${SEMP_URL}/msgVpns/${VPN}/queues" \
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
        }" \
        -s -o /dev/null -w "%{http_code}\n" || true
}

# Note: Compression is enabled by default on the Solace broker on port 55003
# The SMF service compression is already available, no additional configuration needed

# Note: Queues are configured for guaranteed messaging (persistent delivery)
# This enables support for transacted sessions which require guaranteed transport

# Function to add a topic subscription to a queue (topic-to-queue mapping)
add_queue_subscription() {
    local queue_name=$1
    local topic=$2
    echo "Adding subscription '${topic}' to queue: $queue_name"

    curl -X POST "${SEMP_URL}/msgVpns/${VPN}/queues/${queue_name}/subscriptions" \
        -u "${AUTH}" \
        -H "Content-Type: application/json" \
        -d "{
            \"subscriptionTopic\": \"${topic}\"
        }" \
        -s -o /dev/null -w "%{http_code}\n" || true
}

# Create queues
create_queue "test-queue"
create_queue "test-transacted-queue"

# Queue + topic-to-queue mapping used by the SMF persistent publisher tests
create_queue "smf-test-queue"
add_queue_subscription "smf-test-queue" "smf/test/persistent"

echo "Solace initialization completed!"

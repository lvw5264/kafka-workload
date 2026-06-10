#!/usr/bin/env bash
# Antithesis eventually_ driver script that checks if TOTAL_BROKERS=3 amount of nodes report ApiVersion, indicating they are alive
# KAFKA_HOME and BOOTSTRAP server is currently set to work in docker.io/bitnamilegacy/kafka:3.7.0 only
# Equivalent of checking from this command in a kafka container, where since 3 are hardcoded it should report 3:
# "/opt/bitnami/kafka/bin/kafka-broker-api-versions.sh" --bootstrap-server "localhost:9092" | grep -i ApiVersions

# fail out whenever a single bash line has an error, such as if bootstrap server is down
# https://gist.github.com/akrasic/380bda362e0420be08709152c91ca1f9
set -euo pipefail

# just in case kafka changes path
KAFKA_HOME=/opt/bitnami/kafka
# Use kafka1 container's kafka instance to run command
BOOTSTRAP=localhost:9092

# changeable amount of total brokers. currently this is hardcoded in docker-compose.yaml
TOTAL_BROKERS=3

OUTPUT=$("$KAFKA_HOME/bin/kafka-broker-api-versions.sh" \
    --bootstrap-server "$BOOTSTRAP" 2>&1)

# Count reachable brokers — each emits an "ApiVersions:" line
REACHABLE=$(echo "$OUTPUT" | grep -c "ApiVersions" || true)

if [ "$REACHABLE" -lt "$TOTAL_BROKERS" ]; then
    echo "FAIL: only $REACHABLE/$TOTAL_BROKERS brokers reachable"
    echo "$OUTPUT"
    exit 1
fi

echo "PASS: all $TOTAL_BROKERS brokers reachable"
# returns exit code 0 if exit 1 was not run

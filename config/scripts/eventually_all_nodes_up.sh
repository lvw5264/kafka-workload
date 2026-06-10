#!/usr/bin/env bash
# Antithesis eventually_ driver script that checks if TOTAL_BROKERS=3 amount of nodes report ApiVersion, indicating they are alive
# KAFKA_HOME and BOOTSTRAP server is currently set to work in docker.io/bitnamilegacy/kafka:3.7.0 only
# Equivalent of checking from this command in a kafka container, where since 3 are hardcoded it should report 3:
# "/opt/bitnami/kafka/bin/kafka-broker-api-versions.sh" --bootstrap-server "localhost:9092" | grep -i ApiVersions

# just in case kafka changes path
KAFKA_HOME=/opt/bitnami/kafka
# Use kafka1 container's kafka instance to run command
BOOTSTRAP=localhost:9092

# changeable amount of total brokers. currently this is hardcoded in docker-compose.yaml
TOTAL_BROKERS=3

SDK_TARGETS=()
if [ -n "${ANTITHESIS_OUTPUT_DIR:-}" ]; then
    SDK_TARGETS+=("${ANTITHESIS_OUTPUT_DIR}/sdk.jsonl")
fi
if [ -n "${ANTITHESIS_SDK_LOCAL_OUTPUT:-}" ]; then
    SDK_TARGETS+=("$ANTITHESIS_SDK_LOCAL_OUTPUT")
fi

write_sdk() {
    local line="$1"
    for target in "${SDK_TARGETS[@]}"; do
        echo "$line" >> "$target"
    done
}

if [ ${#SDK_TARGETS[@]} -gt 0 ]; then
    write_sdk '{"antithesis_assert":{"hit":false,"must_hit":true,"assert_type":"always","display_type":"Always","message":"All cluster nodes are reachable","condition":false,"id":"All cluster nodes are reachable","location":{"class":"","function":"main","file":"eventually_all_nodes_up.sh","begin_line":1,"begin_column":0},"details":null}}'
fi

OUTPUT=$("$KAFKA_HOME/bin/kafka-broker-api-versions.sh" \
    --bootstrap-server "$BOOTSTRAP" 2>&1)

# Count reachable brokers — each emits an "ApiVersions:" line
REACHABLE=$(echo "$OUTPUT" | grep -c "ApiVersions" || true)

if [ "$REACHABLE" -lt "$TOTAL_BROKERS" ]; then
    if [ ${#SDK_TARGETS[@]} -gt 0 ]; then
        write_sdk '{"antithesis_assert":{"hit":true,"must_hit":true,"assert_type":"always","display_type":"Always","message":"All cluster nodes are reachable","condition":false,"id":"All cluster nodes are reachable","location":{"class":"","function":"main","file":"eventually_all_nodes_up.sh","begin_line":1,"begin_column":0},"details":null}}'
    fi
    echo "FAIL: only $REACHABLE/$TOTAL_BROKERS brokers reachable"
    echo "$OUTPUT"
    exit 1
fi

if [ ${#SDK_TARGETS[@]} -gt 0 ]; then
    write_sdk '{"antithesis_assert":{"hit":true,"must_hit":true,"assert_type":"always","display_type":"Always","message":"All cluster nodes are reachable","condition":true,"id":"All cluster nodes are reachable","location":{"class":"","function":"main","file":"eventually_all_nodes_up.sh","begin_line":1,"begin_column":0},"details":null}}'
fi
echo "PASS: all $TOTAL_BROKERS brokers reachable"
# returns exit code 0 if exit 1 was not run

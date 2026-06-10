#!/usr/bin/env bash
# Antithesis eventually_ driver script that checks if kafka quorum leader eventually gets elected
# KAFKA_HOME and BOOTSTRAP server is currently set to work in docker.io/bitnamilegacy/kafka:3.7.0 only
# Equivalent of checking LeaderId from this command in a kafka container:
# "/opt/bitnami/kafka/bin/kafka-metadata-quorum.sh" --bootstrap-server "localhost:9092" describe --status

# just in case kafka changes path
KAFKA_HOME=/opt/bitnami/kafka
# Use kafka1 container's kafka instance to run command
BOOTSTRAP=localhost:9092

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
    write_sdk '{"antithesis_assert":{"hit":false,"must_hit":true,"assert_type":"always","display_type":"Always","message":"Controller leader is elected","condition":false,"id":"Controller leader is elected","location":{"class":"","function":"main","file":"eventually_kafka_leader.sh","begin_line":1,"begin_column":0},"details":null}}'
fi

QUORUM_LEADER=$("$KAFKA_HOME/bin/kafka-metadata-quorum.sh" \
    --bootstrap-server "$BOOTSTRAP" \
    describe --status 2>&1 | grep -E "^LeaderId:" | awk '{print $2}')

if [ -z "$QUORUM_LEADER" ] || [ "$QUORUM_LEADER" = "-1" ]; then
    if [ ${#SDK_TARGETS[@]} -gt 0 ]; then
        write_sdk '{"antithesis_assert":{"hit":true,"must_hit":true,"assert_type":"always","display_type":"Always","message":"Controller leader is elected","condition":false,"id":"Controller leader is elected","location":{"class":"","function":"main","file":"eventually_kafka_leader.sh","begin_line":1,"begin_column":0},"details":null}}'
    fi
    echo "FAIL: no valid controller leader found"
    exit 1
fi

if [ ${#SDK_TARGETS[@]} -gt 0 ]; then
    write_sdk '{"antithesis_assert":{"hit":true,"must_hit":true,"assert_type":"always","display_type":"Always","message":"Controller leader is elected","condition":true,"id":"Controller leader is elected","location":{"class":"","function":"main","file":"eventually_kafka_leader.sh","begin_line":1,"begin_column":0},"details":null}}'
fi
echo "PASS: controller leader is $QUORUM_LEADER"

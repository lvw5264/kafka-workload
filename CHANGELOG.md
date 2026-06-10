Full Logs
=========

This provides a more detailed walkthrough of how I went through the Antithesis testing process than the 1 page summary.

## Technical Take Home Challenge

> Create your own fork of one of the following open-source example repositories from the Antithesis GitHub: <https://github.com/antithesishq/kafka-workload> <https://github.com/antithesishq/etcd-test-composer>

> Within your fork, add an anytime or eventually driver command. Within that driver command you should add at least one SDK assertion.  

> Please assume that the software under test (SUT) will be run in a chaotic environment with [random faults](https://antithesis.com/docs/environment/fault_injection), this should impact your decision on the type of driver command and SDK assertion to use.

### Add a temporary assertion for testing

Because this is the first time I'm modifying a rust application, as I am most familiar with Python, I looked at what past assertions were added by past Antithesis engineers.

Connor McKee added, then removed the following assertion:

```
                                antithesis_sdk::assert_reachable!("observer consumer was un-assigned from topic partition before read completion", &json!({ "topic": topic, "partition": partition, "last_read_offset": last_read_offset }));
```

https://github.com/antithesishq/kafka-workload/commit/ff15c65ece3ccb115c8bde2aa1638ef419cd27ad

I reintroduced it and the line `use serde_json::json` to test adding my first assertion. Then I rebuilt it:

```
docker build . -t localhost/antithesis-kafka-workload:latest
```

After testing that it worked, I removed the line and then built it again back to the original state. Since it was for temporary testing only, I did not git commit this.

### eventually driver command

> Within your fork, add an anytime or eventually driver command. Within that driver command you should add at least one SDK assertion.  

> Please assume that the software under test (SUT) will be run in a chaotic environment with [random faults](https://antithesis.com/docs/environment/fault_injection), this should impact your decision on the type of driver command and SDK assertion to use.

In response to this, I considered what failure metrics we should use the eventually_ driver for. 

https://antithesis.com/docs/test_templates/test_composer_reference/#eventually-command

I noticed that the kafka repo did not have Kafka Quorum Leader election checking, and Kafka node uptime checking. These are essential conditions, very vulnerable to network, time sync, node hang, or CPU/RAM fault conditions that Antithesis would inject.

Test scripts prefixed with `eventually_` use the eventually driver, which allows time for the system to recover to reach an ideal state. 

Looking at the example for etcd, for Java there is an eventually validation, checking if all nodes are up and healthy.

https://github.com/antithesishq/etcd-test-composer/blob/main/test-template/java-health-check/src/main/java/com/antithesis/etcd/validation/EventuallyValidation.java

The ideal state for Kafka is that there remains a Controller elected, and that all 3 nodes are up. 

While I'm not proficient in rust, I considered whether to study up to create such an `bin/eventually_` rust script that would use rdkafka to query controller election and node uptime, with the mindset that since all test drivers are in the workload container that I should try putting it there.

However after googling how to check that kafka cluster nodes are up and controller election, I decided instead that a bash script using official kafka uptime commands would be more reliable. 

https://kafka.apache.org/41/operations/kraft/#metadata-quorum-tool

This requires it to be in one of the kafka containers such as `kafka1` not the `workload` container. After all, Antithesis allows the `eventually_` driver to be in any container as long as it is in `/opt/antithesis/test/v1/kafka` .

I decided to create `eventually_all_nodes_up.sh` and mount it to kafka1 container. `eventually_` driver is a good pick for Leader election because at first all nodes will come up, but it takes time for a leader to eventually be selected.

```
#!/usr/bin/env bash
# Antithesis eventually_ driver script that checks if kafka quorum leader eventually gets elected
# KAFKA_HOME and BOOTSTRAP server is currently set to work in docker.io/bitnamilegacy/kafka:3.7.0 only
# Equivalent of checking LeaderId from this command in a kafka container:
# "/opt/bitnami/kafka/bin/kafka-metadata-quorum.sh" --bootstrap-server "localhost:9092" --describe --status

# fail out whenever a single bash line has an error, such as if bootstrap server is down
# https://gist.github.com/akrasic/380bda362e0420be08709152c91ca1f9
set -euo pipefail

KAFKA_HOME=/opt/bitnami/kafka
BOOTSTRAP=localhost:9092

KAFKA_LEADER=$("$KAFKA_HOME/bin/kafka-metadata-quorum.sh" \
    --bootstrap-server "$BOOTSTRAP" \
    --describe --status 2>&1 | grep -E "^LeaderId:" | awk '{print $2}')

if [ -z "$KAFKA_LEADER" ] || [ "$KAFKA_LEADER" = "-1" ]; then
    echo "FAIL: no valid controller leader found"
    exit 1
fi

echo "PASS: controller leader is $KAFKA_LEADER"
# returns exit code 0 if exit 1 was not run
```

Next, I created `eventually_all_nodes_up.sh`. `eventually_` driver is also a good pick for kafka node liveliness, since they may go up and down in normal operation without issue, not just injected failures, they just need to come up eventually.

I hardcoded TOTAL_BROKERS=3 for this test here, since docker-compose.yaml is also hardcoded to 3 kafka nodes, but best practice would be to derive it from docker-compose or workload-config.json if they scale up.

```
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
```

I then mounted them to `/opt/antithesis/test/v1/kafka` in `configs/docker-compose.yaml` :

```
services:
  kafka1:
    volumes:
      - ./scripts/eventually_kafka_leader.sh:/opt/antithesis/test/v1/kafka/eventually_kafka_leader.sh:z
      - ./scripts/eventually_all_nodes_up.sh:/opt/antithesis/test/v1/kafka/eventually_all_nodes_up.sh:z
```

### Try out the eventually tests

Finally, I did the following commands to start up the docker-compose and enter kafka-1 container.

```bash
cd config/
docker-compose up
```

https://antithesis.com/docs/test_templates/testing_locally/#how-to-check-test-templates-locally

As per the above steps, I tested the scripts directly, making sure to fix any regressions:

```bash
$ docker-compose exec kafka-1 /bin/bash
root@15a3b747dedb:/# ls /opt/antithesis/test/v1/kafka/eventually_*
eventually_all_nodes_up.sh  eventually_kafka_leader.sh  
root@15a3b747dedb:/# /opt/antithesis/test/v1/kafka/eventually_kafka_leader.sh 
PASS: controller leader is 1
root@15a3b747dedb:/# echo $?
0

root@15a3b747dedb:/# /opt/antithesis/test/v1/kafka/eventually_all_nodes_up.sh 
PASS: all 3 brokers reachable

root@15a3b747dedb:/# echo $?
0
```

For bash scripts, Antithesis does not appear to provide an equivalent of the rust SDK environment variable `ANTITHESIS_SDK_LOCAL_OUTPUT`, it instead checks stdout output and exit code, the output of which can be found with `echo $?` above.

Or we can do oneliners from the git root. Here I caught the case of one node not being up 30 seconds while it was still starting, and then simulated the eventually clause of Antithesis by trying it again and seeing that eventually 3 nodes came up:

```
$ docker-compose -f config/docker-compose.yaml exec kafka-1 /opt/antithesis/test/v1/kafka/eventually_kafka_leader.sh 
WARN[0000] /var/home/lvw5264/git/kafka-workload/config/docker-compose.yaml: the attribute `version` is obsolete, it will be ignored, please remove it to avoid potential confusion 
PASS: controller leader is 1

$ echo $?
0

$ docker-compose -f config/docker-compose.yaml exec kafka-1 /opt/antithesis/test/v1/kafka/eventually_all_nodes_up.sh
WARN[0000] /var/home/lvw5264/git/kafka-workload/config/docker-compose.yaml: the attribute `version` is obsolete, it will be ignored, please remove it to avoid potential confusion 
FAIL: only 2/3 brokers reachable
10.0.0.15:9092 (id: 1 rack: null) -> (
        Produce(0): 0 to 10 [usable: 10],
        Fetch(1): 0 to 16 [usable: 16],
        ListOffsets(2): 0 to 8 [usable: 8],
        Metadata(3): 0 to 12 [usable: 12],
        LeaderAndIsr(4): UNSUPPORTED,
        StopReplica(5): UNSUPPORTED,
        UpdateMetadata(6): UNSUPPORTED,
        ControlledShutdown(7): UNSUPPORTED,
        OffsetCommit(8): 0 to 9 [usable: 9],
        OffsetFetch(9): 0 to 9 [usable: 9],
        FindCoordinator(10): 0 to 4 [usable: 4],
        JoinGroup(11): 0 to 9 [usable: 9],
        Heartbeat(12): 0 to 4 [usable: 4],
        LeaveGroup(13): 0 to 5 [usable: 5],
        SyncGroup(14): 0 to 5 [usable: 5],
        DescribeGroups(15): 0 to 5 [usable: 5],
        ListGroups(16): 0 to 4 [usable: 4],
        SaslHandshake(17): 0 to 1 [usable: 1],
        ApiVersions(18): 0 to 3 [usable: 3],
        CreateTopics(19): 0 to 7 [usable: 7],
        DeleteTopics(20): 0 to 6 [usable: 6],
        DeleteRecords(21): 0 to 2 [usable: 2],
        InitProducerId(22): 0 to 4 [usable: 4],
        OffsetForLeaderEpoch(23): 0 to 4 [usable: 4],
        AddPartitionsToTxn(24): 0 to 4 [usable: 4],
        AddOffsetsToTxn(25): 0 to 3 [usable: 3],
        EndTxn(26): 0 to 3 [usable: 3],
        WriteTxnMarkers(27): 0 to 1 [usable: 1],
        TxnOffsetCommit(28): 0 to 3 [usable: 3],
        DescribeAcls(29): 0 to 3 [usable: 3],
        CreateAcls(30): 0 to 3 [usable: 3],
        DeleteAcls(31): 0 to 3 [usable: 3],
        DescribeConfigs(32): 0 to 4 [usable: 4],
        AlterConfigs(33): 0 to 2 [usable: 2],
        AlterReplicaLogDirs(34): 0 to 2 [usable: 2],
        DescribeLogDirs(35): 0 to 4 [usable: 4],
        SaslAuthenticate(36): 0 to 2 [usable: 2],
        CreatePartitions(37): 0 to 3 [usable: 3],
        CreateDelegationToken(38): 0 to 3 [usable: 3],
        RenewDelegationToken(39): 0 to 2 [usable: 2],
        ExpireDelegationToken(40): 0 to 2 [usable: 2],
        DescribeDelegationToken(41): 0 to 3 [usable: 3],
        DeleteGroups(42): 0 to 2 [usable: 2],
        ElectLeaders(43): 0 to 2 [usable: 2],
        IncrementalAlterConfigs(44): 0 to 1 [usable: 1],
        AlterPartitionReassignments(45): 0 [usable: 0],
        ListPartitionReassignments(46): 0 [usable: 0],
        OffsetDelete(47): 0 [usable: 0],
        DescribeClientQuotas(48): 0 to 1 [usable: 1],
        AlterClientQuotas(49): 0 to 1 [usable: 1],
        DescribeUserScramCredentials(50): 0 [usable: 0],
        AlterUserScramCredentials(51): 0 [usable: 0],
        DescribeQuorum(55): 0 to 1 [usable: 1],
        AlterPartition(56): UNSUPPORTED,
        UpdateFeatures(57): 0 to 1 [usable: 1],
        Envelope(58): UNSUPPORTED,
        DescribeCluster(60): 0 to 1 [usable: 1],
        DescribeProducers(61): 0 [usable: 0],
        UnregisterBroker(64): 0 [usable: 0],
        DescribeTransactions(65): 0 [usable: 0],
        ListTransactions(66): 0 [usable: 0],
        AllocateProducerIds(67): UNSUPPORTED,
        ConsumerGroupHeartbeat(68): 0 [usable: 0],
        ConsumerGroupDescribe(69): UNSUPPORTED,
        GetTelemetrySubscriptions(71): UNSUPPORTED,
        PushTelemetry(72): UNSUPPORTED,
        ListClientMetricsResources(74): 0 [usable: 0]
)
10.0.0.25:9092 (id: 3 rack: null) -> (
        Produce(0): 0 to 10 [usable: 10],
        Fetch(1): 0 to 16 [usable: 16],
        ListOffsets(2): 0 to 8 [usable: 8],
        Metadata(3): 0 to 12 [usable: 12],
        LeaderAndIsr(4): UNSUPPORTED,
        StopReplica(5): UNSUPPORTED,
        UpdateMetadata(6): UNSUPPORTED,
        ControlledShutdown(7): UNSUPPORTED,
        OffsetCommit(8): 0 to 9 [usable: 9],
        OffsetFetch(9): 0 to 9 [usable: 9],
        FindCoordinator(10): 0 to 4 [usable: 4],
        JoinGroup(11): 0 to 9 [usable: 9],
        Heartbeat(12): 0 to 4 [usable: 4],
        LeaveGroup(13): 0 to 5 [usable: 5],
        SyncGroup(14): 0 to 5 [usable: 5],
        DescribeGroups(15): 0 to 5 [usable: 5],
        ListGroups(16): 0 to 4 [usable: 4],
        SaslHandshake(17): 0 to 1 [usable: 1],
        ApiVersions(18): 0 to 3 [usable: 3],
        CreateTopics(19): 0 to 7 [usable: 7],
        DeleteTopics(20): 0 to 6 [usable: 6],
        DeleteRecords(21): 0 to 2 [usable: 2],
        InitProducerId(22): 0 to 4 [usable: 4],
        OffsetForLeaderEpoch(23): 0 to 4 [usable: 4],
        AddPartitionsToTxn(24): 0 to 4 [usable: 4],
        AddOffsetsToTxn(25): 0 to 3 [usable: 3],
        EndTxn(26): 0 to 3 [usable: 3],
        WriteTxnMarkers(27): 0 to 1 [usable: 1],
        TxnOffsetCommit(28): 0 to 3 [usable: 3],
        DescribeAcls(29): 0 to 3 [usable: 3],
        CreateAcls(30): 0 to 3 [usable: 3],
        DeleteAcls(31): 0 to 3 [usable: 3],
        DescribeConfigs(32): 0 to 4 [usable: 4],
        AlterConfigs(33): 0 to 2 [usable: 2],
        AlterReplicaLogDirs(34): 0 to 2 [usable: 2],
        DescribeLogDirs(35): 0 to 4 [usable: 4],
        SaslAuthenticate(36): 0 to 2 [usable: 2],
        CreatePartitions(37): 0 to 3 [usable: 3],
        CreateDelegationToken(38): 0 to 3 [usable: 3],
        RenewDelegationToken(39): 0 to 2 [usable: 2],
        ExpireDelegationToken(40): 0 to 2 [usable: 2],
        DescribeDelegationToken(41): 0 to 3 [usable: 3],
        DeleteGroups(42): 0 to 2 [usable: 2],
        ElectLeaders(43): 0 to 2 [usable: 2],
        IncrementalAlterConfigs(44): 0 to 1 [usable: 1],
        AlterPartitionReassignments(45): 0 [usable: 0],
        ListPartitionReassignments(46): 0 [usable: 0],
        OffsetDelete(47): 0 [usable: 0],
        DescribeClientQuotas(48): 0 to 1 [usable: 1],
        AlterClientQuotas(49): 0 to 1 [usable: 1],
        DescribeUserScramCredentials(50): 0 [usable: 0],
        AlterUserScramCredentials(51): 0 [usable: 0],
        DescribeQuorum(55): 0 to 1 [usable: 1],
        AlterPartition(56): UNSUPPORTED,
        UpdateFeatures(57): 0 to 1 [usable: 1],
        Envelope(58): UNSUPPORTED,
        DescribeCluster(60): 0 to 1 [usable: 1],
        DescribeProducers(61): 0 [usable: 0],
        UnregisterBroker(64): 0 [usable: 0],
        DescribeTransactions(65): 0 [usable: 0],
        ListTransactions(66): 0 [usable: 0],
        AllocateProducerIds(67): UNSUPPORTED,
        ConsumerGroupHeartbeat(68): 0 [usable: 0],
        ConsumerGroupDescribe(69): UNSUPPORTED,
        GetTelemetrySubscriptions(71): UNSUPPORTED,
        PushTelemetry(72): UNSUPPORTED,
        ListClientMetricsResources(74): 0 [usable: 0]
)

$ echo $?
1

$ docker-compose -f config/docker-compose.yaml exec kafka-1 /opt/antithesis/test/v1/kafka/eventually_all_nodes_up.sh
WARN[0000] /var/home/lvw5264/git/kafka-workload/config/docker-compose.yaml: the attribute `version` is obsolete, it will be ignored, please remove it to avoid potential confusion 
PASS: all 3 brokers reachable

$ echo $?
0
```

For bash scripts, Antithesis does not appear to provide an equivalent of the rust SDK environment variable `ANTITHESIS_SDK_LOCAL_OUTPUT`, it instead checks stdout output and exit code, the output of which can be found with `echo $?` above.

### ANTITHESIS_SDK_LOCAL_OUTPUT: /app/logs/sdk_output.jsonl

Finally, I noticed for bash scripts, Antithesis does not appear to provide an equivalent of the rust SDK environment variable `ANTITHESIS_SDK_LOCAL_OUTPUT` . I have to write JSONL lines manually with bash using the "fallback SDK".

https://antithesis.com/docs/using_antithesis/sdk/fallback/

I modified the scripts accordingly:

On the workload container for its rust backend, to get the output of ANTITHESIS_SDK_LOCAL_OUTPUT variable I had to set the output environment variable:

```
ANTITHESIS_SDK_LOCAL_OUTPUT: /app/logs/sdk_output.jsonl
```

I did not modify the workload container's rust code, but is the output of the variable from workload container anyway:

```
$ docker-compose exec workload /bin/bash
root@6a3ecbd9e9f3:/app# cat /app/logs/sdk_output.jsonl 
{"antithesis_sdk":{"language":{"name":"Rust","version":"1.73.0"},"sdk_version":"0.2.6","protocol_version":"1.1.0"}}
{"antithesis_assert":{"assert_type":"reachability","condition":false,"details":null,"display_type":"Reachable","hit":false,"id":"observer consumer was un-assigned from topic partition before read completion","location":{"begin_column":33,"begin_line":137,"class":"antithesis_kafka_workload::kafka::test_consumer","file":"src/kafka/test_consumer.rs","function":"antithesis_kafka_workload::kafka::test_consumer::TestConsumerContext::all_messages_received"},"message":"observer consumer was un-assigned from topic partition before read completion","must_hit":true}}
{"antithesis_assert":{"assert_type":"reachability","condition":false,"details":null,"display_type":"Unreachable","hit":false,"id":"Message key was previously assigned to different partition","location":{"begin_column":29,"begin_line":37,"class":"antithesis_kafka_workload::validation::application_message_partitioning","file":"src/validation/application_message_partitioning.rs","function":"<antithesis_kafka_workload::validation::application_message_partitioning::ApplicationMessagePartitioningValidator as antithesis_kafka_workload::domain::validator::TestValidator>::validate_event"},"message":"Message key was previously assigned to different partition","must_hit":false}}
{"antithesis_assert":{"assert_type":"reachability","condition":false,"details":null,"display_type":"Unreachable","hit":false,"id":"Message offset is less than the last committed offset for this consumer group","location":{"begin_column":25,"begin_line":71,"class":"antithesis_kafka_workload::validation::consumer_message_ordering","file":"src/validation/consumer_message_ordering.rs","function":"<antithesis_kafka_workload::validation::consumer_message_ordering::ConsumerMessageOrderingValidator as antithesis_kafka_workload::domain::validator::TestValidator>::validate_event"},"message":"Message offset is less than the last committed offset for this consumer group","must_hit":false}}
{"antithesis_assert":{"assert_type":"reachability","condition":false,"details":null,"display_type":"Unreachable","hit":false,"id":"Message offset is not greater than previous offset","location":{"begin_column":21,"begin_line":42,"class":"antithesis_kafka_workload::validation::producer_message_ordering","file":"src/validation/producer_message_ordering.rs","function":"<antithesis_kafka_workload::validation::producer_message_ordering::ProducerMessageOrderingValidator as antithesis_kafka_workload::domain::validator::TestValidator>::validate_event"},"message":"Message offset is not greater than previous offset","must_hit":false}}
{"antithesis_assert":{"assert_type":"reachability","condition":false,"details":null,"display_type":"Unreachable","hit":false,"id":"Identical message previously seen (reader)_","location":{"begin_column":25,"begin_line":28,"class":"antithesis_kafka_workload::validation::producer_idempotence","file":"src/validation/producer_idempotence.rs","function":"<antithesis_kafka_workload::validation::producer_idempotence::ProducerIdempotenceValidator as antithesis_kafka_workload::domain::validator::TestValidator>::validate_event"},"message":"Identical message previously seen (reader)_","must_hit":false}}
{"antithesis_assert":{"assert_type":"reachability","condition":false,"details":null,"display_type":"Unreachable","hit":false,"id":"Identical message previously seen (writer)","location":{"begin_column":25,"begin_line":38,"class":"antithesis_kafka_workload::validation::producer_idempotence","file":"src/validation/producer_idempotence.rs","function":"<antithesis_kafka_workload::validation::producer_idempotence::ProducerIdempotenceValidator as antithesis_kafka_workload::domain::validator::TestValidator>::validate_event"},"message":"Identical message previously seen (writer)","must_hit":false}}
{"antithesis_assert":{"assert_type":"reachability","condition":false,"details":null,"display_type":"Unreachable","hit":false,"id":"Message data differs from previous read at location","location":{"begin_column":37,"begin_line":67,"class":"antithesis_kafka_workload::validation::message_integrity","file":"src/validation/message_integrity.rs","function":"<antithesis_kafka_workload::validation::message_integrity::MessageIntegrityValidator as antithesis_kafka_workload::domain::validator::TestValidator>::validate_event"},"message":"Message data differs from previous read at location","must_hit":false}}
{"antithesis_assert":{"assert_type":"reachability","condition":false,"details":null,"display_type":"Unreachable","hit":false,"id":"Message data differs from previous write by producer at location","location":{"begin_column":25,"begin_line":86,"class":"antithesis_kafka_workload::validation::message_integrity","file":"src/validation/message_integrity.rs","function":"<antithesis_kafka_workload::validation::message_integrity::MessageIntegrityValidator as antithesis_kafka_workload::domain::validator::TestValidator>::validate_event"},"message":"Message data differs from previous write by producer at location","must_hit":false}}
{"antithesis_assert":{"assert_type":"reachability","condition":false,"details":null,"display_type":"Unreachable","hit":false,"id":"Message data differs from previous write at location","location":{"begin_column":37,"begin_line":112,"class":"antithesis_kafka_workload::validation::message_integrity","file":"src/validation/message_integrity.rs","function":"<antithesis_kafka_workload::validation::message_integrity::MessageIntegrityValidator as antithesis_kafka_workload::domain::validator::TestValidator>::validate_event"},"message":"Message data differs from previous write at location","must_hit":false}}
{"antithesis_assert":{"assert_type":"reachability","condition":false,"details":null,"display_type":"Unreachable","hit":false,"id":"Message data differs from previous write at location","location":{"begin_column":25,"begin_line":131,"class":"antithesis_kafka_workload::validation::message_integrity","file":"src/validation/message_integrity.rs","function":"<antithesis_kafka_workload::validation::message_integrity::MessageIntegrityValidator as antithesis_kafka_workload::domain::validator::TestValidator>::validate_event"},"message":"Message data differs from previous write at location","must_hit":false}}
{"antithesis_assert":{"assert_type":"reachability","condition":false,"details":null,"display_type":"Unreachable","hit":false,"id":"Read message was never written","location":{"begin_column":33,"begin_line":152,"class":"antithesis_kafka_workload::validation::message_integrity","file":"src/validation/message_integrity.rs","function":"<antithesis_kafka_workload::validation::message_integrity::MessageIntegrityValidator as antithesis_kafka_workload::domain::validator::TestValidator>::validate_event"},"message":"Read message was never written","must_hit":false}}
{"antithesis_assert":{"assert_type":"reachability","condition":false,"details":null,"display_type":"Unreachable","hit":false,"id":"Written message never read","location":{"begin_column":33,"begin_line":156,"class":"antithesis_kafka_workload::validation::message_integrity","file":"src/validation/message_integrity.rs","function":"<antithesis_kafka_workload::validation::message_integrity::MessageIntegrityValidator as antithesis_kafka_workload::domain::validator::TestValidator>::validate_event"},"message":"Written message never read","must_hit":false}}
```

## Regressions

The following regressions were found when running docker on my Fedora Linux 44 x86_64 laptop with SELinux enforcing. This is a distro configuration akin to Red Hat Enterprise Linux 10 with SELinux enforcing, which is common in the US Government.

I had to resolve the regressions to get it to a functioning state first before accomplishing the challenge.

### `antithesis-kafka-workload:latest` no longer ambiguously points to local tag `localhost/antithesis-kafka-workload:latest` on newer docker versions.

In `config/docker-compose.yaml` , `localhost/antithesis-kafka-workload:latest` was not found. My hunch was right that it was better to specify the `localhost/` tag on the header for local usage. 

> **Note:** Perhaps in the past, older docker versions ambiguously pointed `localhost/antithesis-kafka-workload:latest` to `antithesis-kafka-workload:latest` if it existed, and it used to work. In my experience, that's not the behavior that podman allowed, and it seems newer docker versions stopped allowing it too.

### New Bitnami Kafka images now paywalled, old ones moved to bitnamilegacy/kafka

`docker.io/bitnami/kafka:3.7.0` is missing, as it was replaced by Bitnami Secure Images, some of which are a paid product. Rather than leave old versions up, bitnami moved all their old tags to docker.io/bitnamilegacy .

* The immediate fix to stay compatible with the tests is to switch to docker.io/bitnamilegacy/kafka:3.7.0 https://github.com/bitnami/containers/issues/88895
* If I was a customer, I would create a GitHub Pull Request or a support ticket to Antithesis informing them that the docker compose logs indicate that this image needs to be replaced with a free version by upstream vendor decision. 
* If this me working for Antithesis for a customer, create ad merge that same pull request for now. But we should consider asking the customer for a copy of their latest successfully built docker container image rather than just the Dockerfile so we can test using exactly what they have. Then after all testing is successful, only then would we suggest them to change to a different kafka image. Depending on customer discussions, we may end up having to consider buying the matching image from Bitnami:
    * Bitnami Apache Kafka 4.3.0 : Best to try the latest version, but there might be incompatibilities so consider 3.9.2. https://app-catalog.vmware.com/bitnami/releases/f349e9df-0c80-4200-81b5-b6e0e8c4bc09
    * Bitnami Apache Kafka 3.9.2 : not exactly the same as 3.7, but more likely to be backwards compatible than 3.6. https://app-catalog.vmware.com/bitnami/releases/f2465948-8d6a-42ca-8b71-8b1ce285189d
    * Bitnami Apache Kafka 3.6.0: 3.7.0 isn't directly available anymore, if 3.9.2 doesn't work, use this.  https://app-catalog.vmware.com/bitnami/releases/84e899ab-6657-4745-9acd-f6a212492865

### SELinux requires volumes to be mounted with `:z` to be readable by multiple containers

We need to make sure that `:z` is added to the volume mount path, so it can be accessed by this and other containers. Adding `:z` should be harmless for non SELinux systems.

> **Note:** In Fedora, without `:z` suffix there will be SELinux permission denial accessing `workload-config.json`. SELinux is used most often in Red Hat Enterprise Linux and its forks Alma/Rocky, Fedora, and Amazon Linux, and is required by CIS/DISA/STIG compliance. SELinux is rarely used in Ubuntu/Debian, which is why SELinux support might not have been tested.

https://oneuptime.com/blog/post/2026-03-17-selinux-volume-options-podman/view

I change:

```
  workload:
    image: localhost/antithesis-kafka-workload:latest
    container_name: workload
    volumes:
      - ./workload-config.json:/app/workload-config.json
    networks:
      - cluster-net
    depends_on:
      - kafka-1
      - kafka-2
      - kafka-3
```

To:

```
  workload:
    image: localhost/antithesis-kafka-workload:latest
    container_name: workload
    volumes:
      - ./workload-config.json:/app/workload-config.json:z
    networks:
      - cluster-net
    depends_on:
      - kafka-1
      - kafka-2
      - kafka-3
```

Workload then starts inspecting the kafka cluster as intended.

> **Note:** Although multiple containers don't actually need to read this config file, its likely that if modified in the future it might, so I use `:z` instead of `:Z` .

### Default open file limit `ulimit 1024` is too low.

Then next problem is that the workload gradually informs that there are too many open files. Perhaps on Fedora 44, the ulimit has been set lower than other distros would.

```
workload  | {"level":"ERROR","message":"librdkafka: Global error: BrokerTransportFailure (Local: Broker transport failure): 10.0.0.15:9092/1: Failed to create socket: Too many open files (after 0ms in state CONNECT, 4 identical error(s) suppressed)","target":"rdkafka::client"}
```

Typically the default limit is 1024, which is too low for these sorts of applications. I increase the ulimits per container as follows to a typical higher value:

```
  workload:
    image: localhost/antithesis-kafka-workload:latest
    container_name: workload
    volumes:
      - ./workload-config.json:/app/workload-config.json:z
    networks:
      - cluster-net
    depends_on:
      - kafka-1
      - kafka-2
      - kafka-3
    ulimits:
      nofile: 65536
      nproc: 4096
```

https://oneuptime.com/blog/post/2026-02-08-how-to-use-docker-compose-ulimits-configuration/view

Full Logs
=========

To follow my workflow for fixing regressions in the code, here are the full build logs.

## Initial Reading

As this is the first time I'm using antithesis, I read the following documentation:

https://antithesis.com/docs/introduction/welcome_to_antithesis/

https://antithesis.com/docs/introduction/how_antithesis_works/

https://antithesis.com/docs/getting_started/setup/

https://antithesis.com/docs/tutorials/cluster-setup/

## Preparation without workload-config.json modification

I use Fedora Linux, as it is a stable OS similar to what I use at home. I install docker as follows.

```
sudo dnf install docker docker-compose
```

Then I start the docker service. As an optional additional step, I add my username `user` as a `docker` group member, so I can run docker commands without sudo. 

> **Warning:** Practically, `docker` group membership should be considered escalated privileges like sudo, so I will only grant this to users I trust, and I trust myself. I know this because I am a proponent of using podman rootless where possible, but I follow the readme to use docker for now.

```
sudo systemctl enable --now docker
sudo usermod -a -G docker user
```

## Build docker container

I first followed the README exactly, to build the docker container with the given tag successfully.

```
$ docker build . -t antithesis-kafka-workload:latest
[+] Building 175.6s (21/21) FINISHED                                                                                                                                                                                   docker:default
 => [internal] load build definition from Dockerfile                                                                                                                                                                             0.2s
 => => transferring dockerfile: 684B                                                                                                                                                                                             0.0s
 => WARN: FromAsCasing: 'as' and 'FROM' keywords' casing do not match (line 1)                                                                                                                                                   0.2s
 => [internal] load metadata for docker.io/library/debian:bookworm-slim                                                                                                                                                          1.4s
 => [internal] load metadata for docker.io/library/rust:1.73                                                                                                                                                                     2.9s
 => [internal] load .dockerignore                                                                                                                                                                                                0.2s
 => => transferring context: 2B                                                                                                                                                                                                  0.0s
 => [internal] load build context                                                                                                                                                                                                0.6s
 => => transferring context: 143.71kB                                                                                                                                                                                            0.0s
 => [builder 1/7] FROM docker.io/library/rust:1.73@sha256:25fa7a9aa4dadf6a466373822009b5361685604dbe151b030182301f1a3c2f58                                                                                                      64.3s
 => => resolve docker.io/library/rust:1.73@sha256:25fa7a9aa4dadf6a466373822009b5361685604dbe151b030182301f1a3c2f58                                                                                                               0.2s
 => => sha256:37d11998c69399c57572851f6829c8ac8b0da35035dac2c74046f1d13a67d7e9 187.42MB / 187.42MB                                                                                                                              35.5s
 => => sha256:7e18a660069fd7f87a7a6c49ddb701449bfb929c066811777601d36916c7f674 211.06MB / 211.06MB                                                                                                                              54.1s
 => => sha256:325c5bf4c2f26c11380501bec4b6eef8a3ea35b554aa1b222cbcd1e1fe11ae1d 64.13MB / 64.13MB                                                                                                                                30.7s
 => => sha256:13baa2029dde87a21b87127168a0fb50a007c07da6b5adc8864e1fe1376c86ff 24.05MB / 24.05MB                                                                                                                                 6.1s
 => => sha256:8457fd5474e70835e4482983a5662355d892d5f6f0f90a27a8e9f009997e8196 49.58MB / 49.58MB                                                                                                                               159.2s
 => => extracting sha256:8457fd5474e70835e4482983a5662355d892d5f6f0f90a27a8e9f009997e8196                                                                                                                                        2.7s
 => => extracting sha256:13baa2029dde87a21b87127168a0fb50a007c07da6b5adc8864e1fe1376c86ff                                                                                                                                        0.6s
 => => extracting sha256:325c5bf4c2f26c11380501bec4b6eef8a3ea35b554aa1b222cbcd1e1fe11ae1d                                                                                                                                        2.3s
 => => extracting sha256:7e18a660069fd7f87a7a6c49ddb701449bfb929c066811777601d36916c7f674                                                                                                                                        5.7s
 => => extracting sha256:37d11998c69399c57572851f6829c8ac8b0da35035dac2c74046f1d13a67d7e9                                                                                                                                        3.3s
 => [stage-1 1/8] FROM docker.io/library/debian:bookworm-slim@sha256:0104b334637a5f19aa9c983a91b54c89887c0984081f2068983107a6f6c21eeb                                                                                            7.9s
 => => resolve docker.io/library/debian:bookworm-slim@sha256:0104b334637a5f19aa9c983a91b54c89887c0984081f2068983107a6f6c21eeb                                                                                                    0.2s
 => => sha256:068fedd6b0f109b8186d00d49327b6fc6747c428fd3c9a8739424ff5f38d7531 28.23MB / 28.23MB                                                                                                                                 6.3s
 => => extracting sha256:068fedd6b0f109b8186d00d49327b6fc6747c428fd3c9a8739424ff5f38d7531                                                                                                                                        1.0s
 => [builder 2/7] WORKDIR /app                                                                                                                                                                                                   2.3s
 => [builder 3/7] RUN apt-get update && apt-get install -y cmake                                                                                                                                                                 8.2s
 => [builder 4/7] COPY src /app/src                                                                                                                                                                                              0.3s
 => [builder 5/7] COPY Cargo.toml /app/Cargo.toml                                                                                                                                                                                0.3s
 => [builder 6/7] COPY Cargo.lock /app/Cargo.lock                                                                                                                                                                                0.3s
 => [builder 7/7] RUN cargo install --locked --path .                                                                                                                                                                           89.9s
 => [stage-1 2/8] COPY --from=builder /usr/local/cargo/bin/workload /app/workload                                                                                                                                                0.2s
 => [stage-1 3/8] COPY --from=builder /usr/local/cargo/bin/validation /app/validation                                                                                                                                            0.3s
 => [stage-1 4/8] COPY --from=builder /usr/local/cargo/bin/load /app/load                                                                                                                                                        0.3s
 => [stage-1 5/8] COPY scripts/docker-entrypoint.sh /app/entrypoint.sh                                                                                                                                                           0.3s
 => [stage-1 6/8] RUN chmod +x /app/entrypoint.sh                                                                                                                                                                                0.6s
 => [stage-1 7/8] RUN mkdir -p /app/logs                                                                                                                                                                                         0.5s
 => [stage-1 8/8] WORKDIR /app                                                                                                                                                                                                   0.3s
 => exporting to image                                                                                                                                                                                                           3.3s
 => => exporting layers                                                                                                                                                                                                          2.2s
 => => exporting manifest sha256:51e6476424044d963cb26bb8e795b74e739c69fbd0b8fa50a0089f9b16eb64ea                                                                                                                                0.1s
 => => exporting config sha256:095ba7b55bffb2421ce12ecab475d21b247fad0d16ba3dacc5b2d93f81aa691e                                                                                                                                  0.1s
 => => exporting attestation manifest sha256:7b869d32c1a75d2932cea3ecb860b2c55c0486ba7c56f7206c99447c0be10b8f                                                                                                                    0.1s
 => => exporting manifest list sha256:21a8d5ee154672801ed21de396d93c0e6a8c0ec0db0c5e82e47de17de78ad9b9                                                                                                                           0.1s
 => => naming to docker.io/library/antithesis-kafka-workload:latest                                                                                                                                                              0.0s
 => => unpacking to docker.io/library/antithesis-kafka-workload:latest                                                                                                                                                           0.7s

 1 warning found (use docker --debug to expand):
 - FromAsCasing: 'as' and 'FROM' keywords' casing do not match (line 1)
```

> **Note:** I noticed that `docker.io/library/antithesis-kafka-workload:latest` was used as the tag, the default rendering of `antithesis-kafka-workload:latest` for docker. As I am familiar with podman myself, I know that setting `docker.io/library` is likely to cause problems, but I follow the README stringently for now.

## Start Docker Compose

I run docker compose with the built `antithesis-kafka-workload:latest` image for the first time. I notice that the config directory has docker-compose.yml , but it would be more intuitive if `cd` commands were specified in the README to point out to the customer where to run which command.

```
$ cd ../
$ cd workload
$ docker-compose up
WARN[0000] /var/home/lvw5264/git/kafka-workload/config/docker-compose.yaml: the attribute `version` is obsolete, it will be ignored, please remove it to avoid potential confusion 
[+] up 2/2
 ! Image docker.io/bitnami/kafka:3.7.0              Interrupted                                                                                                                                                                   0.1s
 ✘ Image localhost/antithesis-kafka-workload:latest Error failed to resolve reference "localhost/antithesis-kafka-workload:latest": failed to do request: Head "https://localhost/v2/antithesis-kafka-workload/manifests...       0.1s
Error response from daemon: failed to resolve reference "localhost/antithesis-kafka-workload:latest": failed to do request: Head "https://localhost/v2/antithesis-kafka-workload/manifests/latest": dial tcp [::1]:443: connect: connection refused
```

The output points out that:

* `localhost/antithesis-kafka-workload:latest` was not found. My hunch was right that it was better to specify the `localhost/` tag on the header for local usage. 
    * Perhaps in the past, older docker versions ambiguously pointed `localhost/antithesis-kafka-workload:latest` to `antithesis-kafka-workload:latest` if it existed, and it used to work. But that's not the behavior that podman allowed, and it seems newer docker versions stopped allowing it too.
* `docker.io/bitnami/kafka:3.7.0` is missing, as it was replaced by Bitnami Secure Images, some of which are a paid product. Rather than leave old versions up, bitnami moved all their old tags to docker.io/bitnamilegacy .
    * The immediate fix to stay compatible with the tests is to switch to docker.io/bitnamilegacy/kafka:3.7.0 https://github.com/bitnami/containers/issues/88895
    * If I was a customer, I would create a GitHub Pull Request or a support ticket to Antithesis informing them that the docker compose logs indicate that this image needs to be replaced with a free version by upstream vendor decision. 
    * If this me working for Antithesis for a customer, create ad merge that same pull request for now. But we should consider asking the customer for a copy of their latest successfully built docker container image rather than just the Dockerfile so we can test using exactly what they have. Then after all testing is successful, only then would we suggest them to change to a different kafka image. Depending on customer discussions, we may end up having to consider buying the matching image from Bitnami:
        * Bitnami Apache Kafka 4.3.0 : Best to try the latest version, but there might be incompatibilities so consider 3.9.2. https://app-catalog.vmware.com/bitnami/releases/f349e9df-0c80-4200-81b5-b6e0e8c4bc09
        * Bitnami Apache Kafka 3.9.2 : not exactly the same as 3.7, but more likely to be backwards compatible than 3.6. https://app-catalog.vmware.com/bitnami/releases/f2465948-8d6a-42ca-8b71-8b1ce285189d
        * Bitnami Apache Kafka 3.6.0: 3.7.0 isn't directly available anymore, if 3.9.2 doesn't work, use this.  https://app-catalog.vmware.com/bitnami/releases/84e899ab-6657-4745-9acd-f6a212492865

To avoid having to rebuild `antithesis-kafka-workload:latest`, I tag the workload container with the full name `localhost/antithesis-kafka-workload:latest`:

```
docker tag antithesis-kafka-workload:latest localhost/antithesis-kafka-workload:latest
```

I run docker compose a second time after tagging `localhost/antithesis-kafka-workload:latest` , and it was successful.

### Kafka workload logs

I see that Kafka provides the following output:

```
workload  | starting workload 4
workload  | thread 'main' panicked at /app/src/config.rs:62:14:
workload  | failed to read config file: Os { code: 13, kind: PermissionDenied, message: "Permission denied" }
workload  | note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace
workload  | starting workload 5
workload  | thread 'main' panicked at /app/src/config.rs:62:14:
workload  | failed to read config file: Os { code: 13, kind: PermissionDenied, message: "Permission denied" }
workload  | note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace
workload  | starting workload 6
workload  | thread 'main' panicked at /app/src/config.rs:62:14:
workload  | failed to read config file: Os { code: 13, kind: PermissionDenied, message: "Permission denied" }
workload  | note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace
workload  | starting workload 7
workload  | thread 'main' panicked at /app/src/config.rs:62:14:
workload  | failed to read config file: Os { code: 13, kind: PermissionDenied, message: "Permission denied" }
workload  | note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace
workload  | starting workload 8
workload  | thread 'main' panicked at /app/src/config.rs:62:14:
workload  | failed to read config file: Os { code: 13, kind: PermissionDenied, message: "Permission denied" }
workload  | note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace
workload  | starting workload 9
workload  | thread 'main' panicked at /app/src/config.rs:62:14:
workload  | failed to read config file: Os { code: 13, kind: PermissionDenied, message: "Permission denied" }
workload  | note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace
workload  | starting workload 10
workload  | thread 'main' panicked at /app/src/config.rs:62:14:
workload  | failed to read config file: Os { code: 13, kind: PermissionDenied, message: "Permission denied" }
workload  | note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace
workload  | validation check
workload  | validation check
workload  | validation check
workload  | validation check
workload  | validation check
workload  | validation check
```

It still appears to run some validation checks at the end.

Looking at the `workload/src/config.rs` , it looks like it is meant to read the `config/workload-config.json` , but might be failing due to permissions problems.

```rust
impl WorkloadConfig {
    pub fn new(config_path: impl AsRef<Path>) -> Result<WorkloadConfig> {
        let config_file = OpenOptions::new()
            .read(true)
            .open(config_path)
            .expect("failed to read config file");
        let config: WorkloadConfig = serde_json::from_reader(config_file)?;
        Ok(config)
    }
```

This turns out on Fedora to be because of SELinux permission denial. SELinux is used most often in Red Hat Enterprise Linux and its forks Alma/Rocky, Fedora, and Amazon Linux, and is required by CIS/DISA/STIG compliance. SELinux is rarely used in Ubuntu/Debian, which is why SELinux support might not have been tested.

I'm familiar with the root cause often from podman rootless containers: We need to make sure that `:z` is added to the volume mount path, so it can be accessed by this and other containers. Adding `:z` should be harmless for non SELinux systems.

https://oneuptime.com/blog/post/2026-03-17-selinux-volume-options-podman/view

I change:

```
  workload:
    image: localhost/antithesis-kafka-workload:latest
    container_name: workload
    volumes:
      - ./workload-config.json:/app/workload-config.json
    networks:
      - cluster-net
    depends_on:
      - kafka-1
      - kafka-2
      - kafka-3
```

To:

```
  workload:
    image: localhost/antithesis-kafka-workload:latest
    container_name: workload
    volumes:
      - ./workload-config.json:/app/workload-config.json:z
    networks:
      - cluster-net
    depends_on:
      - kafka-1
      - kafka-2
      - kafka-3
```

Workload then starts inspecting the kafka cluster as intended.

Then next problem is that the workload gradually informs that there are too many open files.

```
workload  | {"level":"ERROR","message":"librdkafka: Global error: BrokerTransportFailure (Local: Broker transport failure): 10.0.0.15:9092/1: Failed to create socket: Too many open files (after 0ms in state CONNECT, 4 identical error(s) suppressed)","target":"rdkafka::client"}
```

Typically the default limit is 1024, which is too low for these sorts of applications. I increase the ulimits per container as follows to a typical higher value:

```
  workload:
    image: localhost/antithesis-kafka-workload:latest
    container_name: workload
    volumes:
      - ./workload-config.json:/app/workload-config.json:z
    networks:
      - cluster-net
    depends_on:
      - kafka-1
      - kafka-2
      - kafka-3
    ulimits:
      nofile: 65536
      nproc: 4096
```

https://oneuptime.com/blog/post/2026-02-08-how-to-use-docker-compose-ulimits-configuration/view

After this, workload containers outputs messages like these without issue:

```
workload  | {"level":"INFO","timestamp":"2026-06-09T20:40:09.562Z","event":"message_read_succeeded","consumer_group_id":"c57a673d-0b3d-4eb5-b3e4-ae20593047da-cg7","consumer_id":"c5","topic_name":"a15964f0-6c1d-464b-95f2-fbe925f46637","topic_partition":2,"topic_partition_offset":3,"message_payload":"c57a673d-0b3d-4eb5-b3e4-ae20593047da-p1:1|c75ad450-814e-48b6-bedd-4190c3f7d2fb:body:7:1","target":"antithesis_kafka_workload::kafka::test_consumer"}
workload  | {"level":"INFO","timestamp":"2026-06-09T20:40:09.564Z","event":"message_read_succeeded","consumer_group_id":"b059d471-98aa-4cd9-b37c-e16c6cb9962e-cg5","consumer_id":"c1","topic_name":"4777738b-735a-4e35-b37a-fed4e748befe","topic_partition":3,"topic_partition_offset":10,"message_payload":"b059d471-98aa-4cd9-b37c-e16c6cb9962e-p3:1|1975f8d1-85d0-4e80-9820-80e3017259a6:body:10:2","target":"antithesis_kafka_workload::kafka::test_consumer"}
workload  | {"level":"INFO","timestamp":"2026-06-09T20:40:09.565Z","event":"message_read_succeeded","consumer_group_id":"745217a8-6db5-4c2b-be0b-3d53d889a139-cg2","consumer_id":"c2","topic_name":"f656f290-a1b9-4ebe-b2cd-4e89125d699b","topic_partition":6,"topic_partition_offset":0,"message_payload":"745217a8-6db5-4c2b-be0b-3d53d889a139-p1:1|bbab04dc-38ea-4f03-bad2-3ffbbadd0fcb:body:6:3","target":"antithesis_kafka_workload::kafka::test_consumer"}
workload  | {"level":"INFO","timestamp":"2026-06-09T20:40:09.565Z","event":"message_read_succeeded","consumer_group_id":"45969469-16bc-40d5-9f1b-5f8401105f24-cg1","consumer_id":"c4","topic_name":"8b5d912f-aff7-48ca-b2ba-d499fe39a0e5","topic_partition":1,"topic_partition_offset":0,"message_payload":"45969469-16bc-40d5-9f1b-5f8401105f24-p2:1|8eabf5a1-d786-4a51-9d69-ca255f3f41f3:body:10:2","target":"antithesis_kafka_workload::kafka::test_consumer"}
```

A validation check finally appears after about 5 minutes as follows:

```
}
workload  | {"level":"INFO","timestamp":"2026-06-09T20:40:45.586Z","event":"message_read_succeeded","consumer_group_id":"10150e18-bd84-43e7-b6dd-5158ea0f768b-cg4","consumer_id":"c2","topic_name":"e345c1b7-08f5-4cd1-a349-415b98321f98","topic_partition":5,"topic_partition_offset":15,"message_key":"9b7db651-b11e-4ceb-ad00-c9fa7aa1823e","message_payload":"10150e18-bd84-43e7-b6dd-5158ea0f768b-p1:1|cc8dbbcd-ff21-4dff-8827-6be491c2d41c:footer:7","target":"antithesis_kafka_workload::kafka::test_consumer"}
workload  | validation check
workload  | verifying workload log: logs/kafka-workload-c57a673d-0b3d-4eb5-b3e4-ae20593047da.log from line 2815
workload  | verified workload log: logs/kafka-workload-c57a673d-0b3d-4eb5-b3e4-ae20593047da.log, checked 10 lines up to line 2825
workload  | verifying workload log: logs/kafka-workload-9b75c265-d989-441a-b2bb-d5194d44c6d1.log from line 4313
workload  | verified workload log: logs/kafka-workload-9b75c265-d989-441a-b2bb-d5194d44c6d1.log, checked 5 lines up to line 4318
workload  | verifying workload log: logs/kafka-workload-10150e18-bd84-43e7-b6dd-5158ea0f768b.log from line 2478
workload  | verified workload log: logs/kafka-workload-10150e18-bd84-43e7-b6dd-5158ea0f768b.log, checked 420 lines up to line 2898
workload  | verifying workload log: logs/kafka-workload-d61bd98f-7391-4bfa-83aa-75b2b6d20908.log from line 2075
workload  | verified workload log: logs/kafka-workload-d61bd98f-7391-4bfa-83aa-75b2b6d20908.log, checked 41 lines up to line 2116
workload  | verifying workload log: logs/kafka-workload-745217a8-6db5-4c2b-be0b-3d53d889a139.log from line 3238
workload  | verified workload log: logs/kafka-workload-745217a8-6db5-4c2b-be0b-3d53d889a139.log, checked 32 lines up to line 3270
workload  | verifying workload log: logs/kafka-workload-45969469-16bc-40d5-9f1b-5f8401105f24.log from line 3867
workload  | verified workload log: logs/kafka-workload-45969469-16bc-40d5-9f1b-5f8401105f24.log, checked 240 lines up to line 4107
workload  | verifying workload log: logs/kafka-workload-8e5fe1b4-dd5f-4c4c-9b2d-cf98719bdf9b.log from line 2719
workload  | verified workload log: logs/kafka-workload-8e5fe1b4-dd5f-4c4c-9b2d-cf98719bdf9b.log, checked 559 lines up to line 3278
workload  | {"level":"INFO","timestamp":"2026-06-09T20:40:50.497Z","event":"consumer_stopped","consumer_group_id":"45969469-16bc-40d5-9f1b-5f8401105f24-cg4","consumer_id":"c4","target":"antithesis_kafka_workload::kafka::test_consumer"}
workload  | {"level":"INFO","timestamp":"2026-06-09T20:40:50.497Z","event":"workload_ended","target":"workload"}
workload  | {"level":"INFO","timestamp":"2026-06-09T20:40:50.683Z","event":"consumer_stopping","consumer_group_id":"10150e18-bd84-43e7-b6dd-5158ea0f768b-cg4","consumer_id":"c2","reason":"all_messages_received","target":"antithesis_kafka_workload::kafka::test_consumer"}
workload  | {"level":"INFO","timestamp":"2026-06-09T20:40:50.684Z","event":"consumer_post_rebalance_revoke","consumer_group_id":"10150e18-bd84-43e7-b6dd-5158ea0f768b-cg4","consumer_id":"c2","topic_partition_list":"{\"5ca3f90b-91af-454b-bd7d-882df1fdd8f3\":{\"0\":[-1001],\"1\":[-1001]},\"7be3405c-465a-4ee7-a5cb-8421169a71e9\":{\"0\":[-1001],\"1\":[-1001],\"2\":[-1001],\"3\":[-1001],\"4\":[-1001],\"5\":[-1001],\"6\":[-1001],\"7\":[-1001],\"8\":[-1001],\"9\":[-1001]},\"b5d50b69-25f9-403e-b896-0652036caa4a\":{\"0\":[-1001],\"1\":[-1001],\"2\":[-1001],\"3\":[-1001],\"4\":[-1001],\"5\":[-1001],\"6\":[-1001]},\"c29e2fae-4f9d-49ef-a6d8-01467904f471\":{\"0\":[-1001],\"1\":[-1001],\"2\":[-1001],\"3\":[-1001],\"4\":[-1001]},\"e345c1b7-08f5-4cd1-a349-415b98321f98\":{\"0\":[-1001],\"1\":[-1001],\"2\":[-1001],\"3\":[-1001],\"4\":[-1001],\"5\":[-1001],\"6\":[-1001]},\"f9dab305-ba67-419e-89af-0a605f43c6dc\":{\"0\":[-1001],\"1\":[-1001],\"2\":[-1001],\"3\":[-1001],\"4\":[-1001],\"5\":[-1001]}}","target":"antithesis_kafka_workload::kafka::test_consumer"}
workload  | {"level":"INFO","timestamp":"2026-06-09T20:40:50.688Z","event":"consumer_messages_committed","consumer_group_id":"10150e18-bd84-43e7-b6dd-5158ea0f768b-cg4","consumer_id":"c2","topic_partition_list":"{\"5ca3f90b-91af-454b-bd7d-882df1fdd8f3\":{\"0\":[18],\"1\":[16]},\"7be3405c-465a-4ee7-a5cb-8421169a71e9\":{\"0\":[-1001],\"1\":[-1001],\"2\":[-1001],\"3\":[2],\"4\":[14],\"5\":[1],\"6\":[1],\"7\":[3],\"8\":[2],\"9\":[7]},\"b5d50b69-25f9-403e-b896-0652036caa4a\":{\"0\":[-1001],\"1\":[-1001],\"2\":[-1001],\"3\":[3],\"4\":[-1001],\"5\":[3],\"6\":[2]},\"c29e2fae-4f9d-49ef-a6d8-01467904f471\":{\"0\":[10],\"1\":[18],\"2\":[10],\"3\":[9],\"4\":[14]},\"e345c1b7-08f5-4cd1-a349-415b98321f98\":{\"0\":[-1001],\"1\":[-1001],\"2\":[-1001],\"3\":[7],\"4\":[16],\"5\":[16],\"6\":[14]},\"f9dab305-ba67-419e-89af-0a605f43c6dc\":{\"0\":[-1001],\"1\":[-1001],\"2\":[19],\"3\":[10],\"4\":[21],\"5\":[-1001]}}","target":"antithesis_kafka_workload::kafka::test_consumer"}


workload  | {"level":"INFO","timestamp":"2026-06-09T20:40:50.788Z","event":"consumer_stopped","consumer_group_id":"10150e18-bd84-43e7-b6dd-5158ea0f768b-cg4","consumer_id":"c2","target":"antithesis_kafka_workload::kafka::test_consumer"}
workload  | {"level":"INFO","timestamp":"2026-06-09T20:40:50.788Z","event":"workload_ended","target":"workload"}
workload  | validation check
workload  | verifying workload log: logs/kafka-workload-10150e18-bd84-43e7-b6dd-5158ea0f768b.log from line 2898
workload  | verified workload log: logs/kafka-workload-10150e18-bd84-43e7-b6dd-5158ea0f768b.log, checked 6 lines up to line 2904
workload  | verifying workload log: logs/kafka-workload-45969469-16bc-40d5-9f1b-5f8401105f24.log from line 4107
workload  | verified workload log: logs/kafka-workload-45969469-16bc-40d5-9f1b-5f8401105f24.log, checked 6 lines up to line 4113
workload  | verifying workload log: logs/kafka-workload-8e5fe1b4-dd5f-4c4c-9b2d-cf98719bdf9b.log from line 3278
workload  | verified workload log: logs/kafka-workload-8e5fe1b4-dd5f-4c4c-9b2d-cf98719bdf9b.log, checked 5 lines up to line 3283
```

The workload then outputs only the following messages. Therefore, I got it back up to the likely intended working state with the same software versions.

```
workload  | validation check
workload  | validation check
```

After seeing only those messages for 2 minutes, I pressed Ctrl-C to exit, and docker-compose down just to be sure.

```
$ docker-compose down
WARN[0000] /var/home/lvw5264/git/kafka-workload/config/docker-compose.yaml: the attribute `version` is obsolete, it will be ignored, please remove it to avoid potential confusion 
[+] down 5/5
 ✔ Container workload         Removed                                                                                                                                                                                             0.1s
 ✔ Container kafka-3          Removed                                                                                                                                                                                             0.1s
 ✔ Container kafka-2          Removed                                                                                                                                                                                             0.2s
 ✔ Container kafka-1          Removed                                                                                                                                                                                             0.2s
 ✔ Network config_cluster-net Removed 
```

---


## Amended README

The readme needs clarifications as docker versions have changed since 6 months ago, so I have added them.

### Start the Kafka Cluster

By default, tagging a container `antithesis-kafka-workload:latest` refers to `docker.io/library/antithesis-kafka-workload:latest`, a namespace we don't own and can't push to. 

So instead, its best practice in newer docker versions, or especially podman, to specify the full namespace `localhost/antithesis-kafka-workload:latest`

```bash
docker build . -t localhost/antithesis-kafka-workload:latest
cd ../
```

A `cd ../` indicates to the user to go back up to the git root directory.

I also add a clarification regarding the docker container image tag:

> **Note:** When you are an Antithesis customer, [we provide you a container registry to use instead of relying on the local registry.](https://antithesis.com/docs/getting_started/setup/#push-your-images) But you may push the image to another container registry instead of leaving it locally such as Github/Gitlab Container Registry or AWS ECR, make sure to change the docker image tag accordingly.

### Start the Docker Compose

I clarified to the user to go to the config directory as follows:

> The docker-compose.yaml is stored in the config/ directory in this repo.

```bash
cd config
```

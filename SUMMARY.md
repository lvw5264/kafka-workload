> The purpose of this technical screening is to allow you to experience some of the common tasks expected for our SEs. In this scenario, a customer has created an initial setup of their system for testing in Antithesis and we are providing examples of how to enhance them  to increase their chances of finding critical issues.

> Create your own fork of one of the following open-source example repositories from the Antithesis GitHub: <https://github.com/antithesishq/kafka-workload> <https://github.com/antithesishq/etcd-test-composer>

> Within your fork, add an anytime or eventually driver command. Within that driver command you should add at least one SDK assertion.  

> Please assume that the software under test (SUT) will be run in a chaotic environment with [random faults](https://antithesis.com/docs/environment/fault_injection), this should impact your decision on the type of driver command and SDK assertion to use.

I, the author Lawrence Wu on 2026-06-09, am summarizing the amendments and updates made to this git repository as part of the "Antithesis Solutions Engineering (SE) Technical Take Home Challenge".

I assume that we are enhancing our existing example for the customer to better understand how Antithesis helps make critical but rare issues easier to find.

## Regressions

> **Note:** The following regressions were found when running docker on my Fedora Linux 44 x86_64 laptop with SELinux enforcing. This is a distro configuration akin to Red Hat Enterprise Linux 10 with SELinux enforcing, which is common in the US Government.

I see that the kafka public repository is only 9-10 months old in recent commits and is meant to be a public template for customer documentation. But I found some regressions in the public template preventing it from starting up. This can add to customer frustration that distracts them from the value of the Antithesis product. 

If I were tasked to try out and see if there were issues with this customer facing template, I'd make the following recommendations for fixes.

* In the README's docker build line, change `antithesis-kafka-workload:latest` to `localhost/antithesis-kafka-workload:latest` just like in docker-compose.yaml, appears to be required on the latest docker versions and in podman primarily.
* Switch `docker.io/bitnami/kafka:3.7.0` to `docker.io/bitnamilegacy/kafka:3.7.0`
    * `docker.io/bitnami/kafka:3.7.0` was replaced by Bitnami Secure Images, some of which are a paid product. Rather than leave old versions up, bitnami moved all their old tags to docker.io/bitnamilegacy . https://github.com/bitnami/containers/issues/88895
* SELinux systems like Fedora requires volumes to be mounted with `:z` to be readable by multiple containers, even if rootless mode or podman is not used.
    * We need to make sure that `:z` is added to the volume mount path, so it can be accessed by this and other containers. Adding `:z` should be harmless for non SELinux systems.
* Default open file limit for containers, `ulimit 1024` is too low in Fedora. As such out of the box, the workload gradually informs that there are too many open files. Perhaps on Fedora 44, the ulimit has been set lower than other distros would.

Refer to this git commit for more info:

https://github.com/lvw5264/kafka-workload/commit/e917770266a64174bd81dc8e0c2db46140c2945c

## Summary

I noticed that unlike the etcd repo: 

1. The kafka repo did not have **Kafka Quorum Leader election checking**. 
2. And for **Kafka node uptime checking**, it only had a simple check in `workload/src/load.rs` . 

These are essential conditions to watch out for in customer applications, very vulnerable to network, time sync, node hang, or CPU/RAM *fault conditions that Antithesis would inject.*

[After some googling](https://kafka.apache.org/41/operations/kraft/#metadata-quorum-tool), I created two bash scripts with the `eventually_` driver, which allows time for the system to recover to reach an ideal state.  

* `config/scripts/eventually_kafka_leader.sh` - `eventually_` driver is a good pick for Leader election because at first all nodes will come up, but it takes time for a leader to eventually be selected.
    * Equivalent of checking LeaderId from this command in a kafka container:
`# "/opt/bitnami/kafka/bin/kafka-metadata-quorum.sh" --bootstrap-server "localhost:9092" --describe --status`
* `config/scripts/eventually_all_nodes_up.sh` - `eventually_` driver is also a good pick for kafka node liveliness, since they may go up and down in normal operation without issue, not just injected failures, they just need to come up eventually.

    * Equivalent of checking from this command in a kafka container, where since 3 are hardcoded it should report 3: `# "/opt/bitnami/kafka/bin/kafka-broker-api-versions.sh" --bootstrap-server "localhost:9092" | grep -i ApiVersions`

These changes are recorded in the following git commit:

https://github.com/lvw5264/kafka-workload/commit/1ce5ddcf86180990d6b1343bcc6cb97fec362526

I then ran the test scripts as per these instructions, and they worked great:

```bash
$ docker-compose -f config/docker-compose.yaml exec kafka-1 /opt/antithesis/test/v1/kafka/eventually_kafka_leader.sh 
PASS: controller leader is 1
$ echo $?
0

$ docker-compose -f config/docker-compose.yaml exec kafka-1 /opt/antithesis/test/v1/kafka/eventually_all_nodes_up.sh
FAIL: only 2/3 brokers reachable
$ echo $?
1

$ docker-compose -f config/docker-compose.yaml exec kafka-1 /opt/antithesis/test/v1/kafka/eventually_all_nodes_up.sh
PASS: all 3 brokers reachable

$ echo $?
0
```

https://antithesis.com/docs/test_templates/testing_locally/#how-to-check-test-templates-locally

## ANTITHESIS_SDK_LOCAL_OUTPUT

Finally, I noticed for bash scripts, Antithesis does not appear to provide an equivalent of the rust SDK environment variable `ANTITHESIS_SDK_LOCAL_OUTPUT` . I have to write JSONL lines manually with bash using the "fallback SDK".

https://antithesis.com/docs/using_antithesis/sdk/fallback/

The changes needed to support this are recorded in the following git commit:

https://github.com/lvw5264/kafka-workload/commit/f4b4a19b3c20c951b56ee3494380329dd38bb6bc

I report the bash script's `ANTITHESIS_SDK_LOCAL_OUTPUT` with similar commands as above, which for kafka-1 container I set to `ANTITHESIS_SDK_LOCAL_OUTPUT: /opt/bitnami/kafka/logs/sdk_output.jsonl` , below. I see that as per the guide, antithesis_sdk JSONL line does not need to be reported, and there is always a declaration message with `"hit": false` and `condition: false` JSONL before each assertion evaluation `"hit": true` .

```
{"antithesis_assert":{"hit":false,"must_hit":true,"assert_type":"always","display_type":"Always","message":"Controller leader is elected","condition":false,"id":"Controller leader is elected","location":{"class":"","function":"main","file":"eventually_kafka_leader.sh","begin_line":1,"begin_column":0},"details":null}}
{"antithesis_assert":{"hit":true,"must_hit":true,"assert_type":"always","display_type":"Always","message":"Controller leader is elected","condition":true,"id":"Controller leader is elected","location":{"class":"","function":"main","file":"eventually_kafka_leader.sh","begin_line":1,"begin_column":0},"details":null}}
{"antithesis_assert":{"hit":false,"must_hit":true,"assert_type":"always","display_type":"Always","message":"All cluster nodes are reachable","condition":false,"id":"All cluster nodes are reachable","location":{"class":"","function":"main","file":"eventually_all_nodes_up.sh","begin_line":1,"begin_column":0},"details":null}}
{"antithesis_assert":{"hit":true,"must_hit":true,"assert_type":"always","display_type":"Always","message":"All cluster nodes are reachable","condition":false,"id":"All cluster nodes are reachable","location":{"class":"","function":"main","file":"eventually_all_nodes_up.sh","begin_line":1,"begin_column":0},"details":null}}
{"antithesis_assert":{"hit":false,"must_hit":true,"assert_type":"always","display_type":"Always","message":"All cluster nodes are reachable","condition":false,"id":"All cluster nodes are reachable","location":{"class":"","function":"main","file":"eventually_all_nodes_up.sh","begin_line":1,"begin_column":0},"details":null}}
{"antithesis_assert":{"hit":true,"must_hit":true,"assert_type":"always","display_type":"Always","message":"All cluster nodes are reachable","condition":true,"id":"All cluster nodes are reachable","location":{"class":"","function":"main","file":"eventually_all_nodes_up.sh","begin_line":1,"begin_column":0},"details":null}}
```

Also, while I didn't modify the rust workload, I report it's `ANTITHESIS_SDK_LOCAL_OUTPUT` below for informational purposes.

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

# Antithesis Kafka Workload

## Workload Objectives

This workload is intended for use with any system that uses the [Apache Kafka](https://kafka.apache.org/) protocol to verify common Kafka properties that should always hold true, even in the presence of faults. 

This workload uses the [rdkafka](https://crates.io/crates/rdkafka) client library; as of August 2025, it's using version [0.36.2](https://crates.io/crates/rdkafka/0.36.2), which is compatible with librdkafka v1.9.2+.

As of August 2025, this workload expects to run against a 3-node Kafka cluster. The cluster configuration (set via the producer) strives for the strongest consistency guarantees- the number of in-sync replicas and the replication factor are both 3, with the producer expected acknowledgements from all nodes to consider a write to be successful. 

As you can see in the image's entrypoint, the workload executable will be executed some number of times in the background. The validation executable will be continually executed as well.

## Workload Flow

The workload does a series of producing and consuming against the cluster. 

### Producer

Each invocation of the workload will create a random number of producers. Each producer produces a sequence of messages to random test topics, with configurable, random delay between, retrying each send until its successful. Producers update shared global state to track topic offsets. All the while, it's logging it's successful attempts. 

### Consumer

Each invocation of the workload will create a random number of consumer groups with a random number of consumers in each. Each consumer joins a group; subscribes to topics; handles rebalances (partitions assigned/revoked). It then polls for messages, with configurable processing delays and commits offsets (manual or auto). 

Consumers similarly updates global state so the test harness knows which offsets have been consumed and then stop consuming once all producer messages have been read (or if no messages were ever written). Similar to the producer, the consumer logs structured events at every step.

## Validation Flow

Based on the logged output from the producers and consumers, a series of validation steps are performed. These validation steps are described in more detail below.

## Workload Test Properties

As of August 2025, there are five properties being asserted via the Antithesis SDK: 

1. There are no "message integrity" violations (specifically, there are no "lost" messages or messages that were read but never written). 
2. Messages don't change partitions.
3. Previously-committed consumer offsets are never seen again. 
4. The producer doesn't "double-write" messages. 
5. Sequential messages are in sequential offsets. 

## How to Use (and Validate!)

This workload is intended to run in any environment with a Kafka-compatible cluster. Try it locally with your own Kafka-compatible system!

You can build the workload image from within the workload directory and run it locally.

The Dockerfile is stored in the workloads/ directory in this repo.

```bash
cd workloads
```

Then build the container locally. 

> **Note:** When you are an Antithesis customer, [we provide you a container registry to use instead of relying on the local registry.](https://antithesis.com/docs/getting_started/setup/#push-your-images) But you may push the image to another container registry instead of leaving it locally such as Github/Gitlab Container Registry or AWS ECR, make sure to change the docker image tag accordingly.

```bash
cd workload
docker build . -t localhost/antithesis-kafka-workload:latest
```

Once finished, return to the top level of the git repository before running the docker-compose.yaml

```
cd ../
```

### Start the Kafka Cluster

The docker-compose.yaml is stored in the config/ directory in this repo.

```bash
cd config
```

You can then run the entire Kafka cluster from within the config directory by running the following command:

```bash
docker-compose up -d
cd ../
```

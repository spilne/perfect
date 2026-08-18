# @perfect/kafka-kafkajs

KafkaJS driver adapter for `@perfect/kafka`.

## Install

```bash
bun add @perfect/kafka @perfect/kafka-kafkajs kafkajs
```

> Not yet published to npm — install from the workspace for now.

## Quickstart

```ts
import { kafkaConfig } from "@perfect/kafka";
import { createKafkajsClient } from "@perfect/kafka-kafkajs";

const orders = kafkaConfig<unknown>()
  .client(createKafkajsClient(["localhost:9092"]))
  .topic("orders")
  .group("order-workers")
  .build();
```

`createKafkajsClient` also accepts a KafkaJS-style object with required
`brokers` plus `clientId`, SSL, SASL, log level, and other `KafkaConfig`
options. The adapter supports callback `eachMessage` and `eachBatch` modes,
replay/seek, commits, topic administration, and managed partition lifecycle.
`createKafkajsTopic(config, topic, partitions?)` is the convenience helper for
creating a topic in tests, examples, or application bootstrap code.

See [Messaging contracts and Kafka](../../documentation/16-messaging.md).

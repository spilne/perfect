# @perfect/kafka-platformatic

Platformatic Kafka driver adapter for `@perfect/kafka`. The native message stream is bridged into Perfect through a bounded `data`/`error`/`end` event queue with pause/resume backpressure.

`@platformatic/kafka` 2.9 requires Node 22.22+ or Node 24.6+. Its consumer is not supported on Bun; use `@perfect/kafka-kafkajs` for a Bun-native Kafka client.

## Install

```bash
bun add @perfect/kafka @perfect/kafka-platformatic @platformatic/kafka
```

> Not yet published to npm — install from the workspace for now.

## Quickstart

```ts
import { kafkaConfig } from "@perfect/kafka";
import { createPlatformaticClient } from "@perfect/kafka-platformatic";

const orders = kafkaConfig<unknown>()
  .client(
    createPlatformaticClient({
      bootstrapBrokers: ["localhost:9092"],
      clientId: "order-service",
      streamBufferCapacity: 128,
    }),
  )
  .topic("orders")
  .group("order-workers")
  .build();
```

The adapter also accepts a broker string or array. Configure native producer,
consumer, and admin options through `PlatformaticAdapterConfig`; group IDs are
owned by `KafkaTopic` and injected per subscription.

See [Messaging contracts and Kafka](../../documentation/16-messaging.md).

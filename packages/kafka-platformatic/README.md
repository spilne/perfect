# @perfect/kafka-platformatic

Platformatic Kafka driver adapter for `@perfect/kafka`. The native message stream is bridged into Perfect through a bounded `data`/`error`/`end` event queue with pause/resume backpressure.

`@platformatic/kafka` 2.9 requires Node 22.22+ or Node 24.6+. Its consumer is not supported on Bun; use `@perfect/kafka-kafkajs` for a Bun-native Kafka client.

```ts
import { KafkaTopic } from "@perfect/kafka";
import { createPlatformaticClient } from "@perfect/kafka-platformatic";

const orders = new KafkaTopic({
  kafka: createPlatformaticClient(["localhost:9092"]),
  topic: "orders",
  groupId: "order-workers",
});
```

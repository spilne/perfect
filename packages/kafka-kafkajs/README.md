# @perfect/kafka-kafkajs

KafkaJS driver adapter for `@perfect/kafka`.

```ts
import { KafkaTopic } from "@perfect/kafka";
import { createKafkajsClient } from "@perfect/kafka-kafkajs";

const orders = new KafkaTopic({
  kafka: createKafkajsClient(["localhost:9092"]),
  topic: "orders",
  groupId: "order-workers",
});
```

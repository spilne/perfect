import { describe, expect, it } from "bun:test";
import type { KafkaClient } from "../src/kafka-types";
import { kafkaConfig } from "../src/kafka-config-builder";

const client = {} as KafkaClient;

describe("KafkaConfigBuilder", () => {
  it("builds a KafkaTopic from fluent configuration", () => {
    const topic = kafkaConfig<{ id: string }>()
      .client(client)
      .topic("events")
      .group("workers")
      .consumer({ sessionTimeout: 30_000, heartbeatInterval: 3_000 })
      .batchEmit()
      .build();

    expect(topic.partitions).toBe(1);
  });

  it("reports missing required settings", () => {
    expect(() => kafkaConfig().topic("events").group("workers").toConfig()).toThrow(
      "requires a client",
    );
  });
});

import { run, type Eff } from "@perfect/core";
import { GroupId, KafkaTopic, TopicName } from "@perfect/kafka";
import { createPlatformaticClient } from "@perfect/kafka-platformatic";

const broker = process.argv[2];
const rawTopic = process.argv[3];
if (!broker || !rawTopic) throw new Error("broker and topic arguments are required");
const unsafeRun = <A>(effect: Eff<A, unknown>): Promise<A> => run(effect as any);

const topic = TopicName(rawTopic);
const kafka = createPlatformaticClient({
  bootstrapBrokers: [broker],
  consumerOptions: { groupProtocol: "consumer" },
});
const messages = new KafkaTopic<{ key: string; sequence: number }>({
  kafka,
  topic,
  groupId: GroupId(`platformatic-${crypto.randomUUID()}`),
});

console.error("platformatic-probe:publish:start");
await unsafeRun(
  messages.publishBatch([
    { value: { key: "user-1", sequence: 1 }, key: "user-1" },
    { value: { key: "user-1", sequence: 2 }, key: "user-1" },
    { value: { key: "user-1", sequence: 3 }, key: "user-1" },
  ]),
);
console.error("platformatic-probe:publish:done");

console.error("platformatic-probe:consume:start");
const received = await unsafeRun(
  messages
    .subscribeFrom({ offset: { type: "earliest" } })
    .take(3)
    .toArray(),
);
console.error("platformatic-probe:consume:done");
console.error("platformatic-probe:disconnect:start");
await messages.disconnect();
console.error("platformatic-probe:disconnect:done");

console.log(`PERFECT_PLATFORMATIC_RESULT=${JSON.stringify(received)}`);

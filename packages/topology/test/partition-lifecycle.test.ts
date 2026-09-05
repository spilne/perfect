import { expect, spyOn, test } from "bun:test";
import {
  InMemoryPartitionedState,
  Partition,
  StageId,
  StateCheckpointId,
  TopologyId,
  TopologyInstanceId,
} from "@spilne/perfect-core/connect";
import { PartitionLifecycle } from "../src/partition-lifecycle";

function fixture() {
  const backend = new InMemoryPartitionedState();
  const lifecycle = new PartitionLifecycle({
    topologyId: TopologyId("orders"),
    stageId: StageId("sink"),
    instanceId: TopologyInstanceId("worker"),
    leaseMs: 30_000,
    stateBackend: backend,
    nextCheckpointId: () => StateCheckpointId("checkpoint"),
  });
  return { backend, lifecycle, partition: Partition(0) };
}

test("concurrent activation shares one acquisition and restored context", async () => {
  const { backend, lifecycle, partition } = fixture();
  const acquire = spyOn(backend, "acquire");
  const [first, second] = await Promise.all([
    lifecycle.activate(partition),
    lifecycle.activate(partition),
  ]);
  expect(first).toBe(second);
  expect(acquire).toHaveBeenCalledTimes(1);
  expect(await lifecycle.activate(partition)).toBe(first);
  await lifecycle.revoke(partition);
  expect(lifecycle.contexts.size).toBe(0);
});

test("revocation checkpoints the latest offset before releasing ownership", async () => {
  const { backend, lifecycle, partition } = fixture();
  const context = await lifecycle.activate(partition);
  context.sourceOffset = "42";
  const events: string[] = [];
  const commit = backend.commit.bind(backend);
  const release = backend.release.bind(backend);
  spyOn(backend, "commit").mockImplementation(async (params) => {
    events.push("checkpoint");
    expect(params.sourceOffset).toBe("42");
    expect(params.checkpointId).toBe(StateCheckpointId("checkpoint"));
    return commit(params);
  });
  spyOn(backend, "release").mockImplementation(async (lease) => {
    events.push("release");
    return release(lease);
  });
  await lifecycle.renew();
  await lifecycle.revoke(partition);
  expect(events).toEqual(["checkpoint", "release"]);
  expect(lifecycle.contexts.has(partition)).toBe(false);
});

test("fenced checkpoints do not release or forget a partition", async () => {
  const { backend, lifecycle, partition } = fixture();
  await lifecycle.activate(partition);
  spyOn(backend, "commit").mockResolvedValue("fenced");
  const release = spyOn(backend, "release");
  await expect(lifecycle.revoke(partition)).rejects.toThrow("fenced during revocation checkpoint");
  expect(release).not.toHaveBeenCalled();
  expect(lifecycle.contexts.has(partition)).toBe(true);
});

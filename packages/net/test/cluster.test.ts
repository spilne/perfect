import { afterEach, describe, expect, test } from "bun:test";
import {
  Clock,
  Random,
  TestClock,
  TestRandom,
  fail,
  provide,
  runFiber,
  scoped,
  sleep,
  succeed,
  sync,
  type Eff,
  type Fiber,
} from "@spilne/perfect-core";
import { Cluster, SimulatedNetwork, Transport, type Address } from "../src";
import type { Cluster as ClusterHandle } from "../src/cluster.js";

const timing = { intervalMs: 100, pingTimeoutMs: 30, suspicionMs: 300 };
const address = (id: string): Address => ({ host: id, port: 7946 });

class TestCluster {
  readonly clock = new TestClock();
  readonly handles = new Map<string, ClusterHandle>();
  private readonly fibers = new Map<string, Fiber<void, never>>();
  private network!: SimulatedNetwork;

  async start(): Promise<void> {
    await this.run(
      SimulatedNetwork.make().flatMap((network) =>
        sync(() => {
          this.network = network;
        }),
      ),
    ).await();
  }

  async add(id: string, seeds: string[], service: string, commands: string[] = ["get"]) {
    const program = scoped(
      provide(
        Cluster.builder({ id, address: address(id), seeds: seeds.map(address), ...timing })
          .service({ name: service, version: "v1", address: { host: id, port: 8080 }, commands })
          .join()
          .flatMap((handle) =>
            sync(() => {
              this.handles.set(id, handle);
            }).flatMap(() => sleep(1e12)),
          ),
        Transport,
        this.network.node(id),
      ),
    );
    this.fibers.set(id, this.run(program.orDie() as Eff<void, never>));
    await this.settle();
  }

  async stop(id: string): Promise<void> {
    const fiber = this.fibers.get(id)!;
    fiber.interrupt();
    await fiber.await();
    this.fibers.delete(id);
    this.handles.delete(id);
  }

  partition(a: string, b: string): void {
    this.network.partition(a, b);
  }

  async pass(ms: number): Promise<void> {
    for (let t = 0; t < ms; t += 10) {
      this.clock.advance(10);
      await this.settle();
    }
  }

  run<A>(program: Eff<A, never>): Fiber<A, never> {
    return runFiber(provide(provide(program, Clock, this.clock), Random, new TestRandom(3)));
  }

  stopAll(): void {
    for (const fiber of this.fibers.values()) fiber.interrupt();
  }

  private settle(): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, 1));
  }
}

let cluster: TestCluster;
afterEach(() => cluster?.stopAll());

describe("Cluster", () => {
  test("builder advertises service commands and selects only matching peers", async () => {
    cluster = new TestCluster();
    await cluster.start();
    await cluster.add("api", [], "api", ["list"]);
    await cluster.add("orders", ["api"], "orders", ["get", "create"]);
    await cluster.pass(300);

    const api = cluster.handles.get("api")!;
    const found = await cluster
      .run(api.select({ name: "orders", command: "get", version: "v1" }).orDie())
      .await();
    expect(found._tag).toBe("Success");
    if (found._tag === "Success")
      expect(found.value.service.address).toEqual({ host: "orders", port: 8080 });
    expect(await cluster.run(api.peers({ name: "orders", command: "delete" })).await()).toEqual({
      _tag: "Success",
      value: [],
    });
  });

  test("drain removes a service, and a replacement instance can join", async () => {
    cluster = new TestCluster();
    await cluster.start();
    await cluster.add("api", [], "api");
    await cluster.add("orders-1", ["api"], "orders");
    await cluster.pass(300);
    const api = cluster.handles.get("api")!;
    expect((await cluster.run(api.peers({ name: "orders" })).await())._tag).toBe("Success");
    await cluster.run(cluster.handles.get("orders-1")!.drain).await();
    await cluster.pass(300);
    expect(await cluster.run(api.peers({ name: "orders" })).await()).toEqual({
      _tag: "Success",
      value: [],
    });
    await cluster.stop("orders-1");
    await cluster.add("orders-2", ["api"], "orders");
    await cluster.pass(300);
    const selected = await cluster.run(api.select({ name: "orders" }).orDie()).await();
    if (selected._tag === "Success") expect(selected.value.member.id).toBe("orders-2");
    else throw new Error("replacement not selected");
  });

  test("suspected peers are excluded after a partition", async () => {
    cluster = new TestCluster();
    await cluster.start();
    await cluster.add("api", [], "api");
    await cluster.add("orders", ["api"], "orders");
    await cluster.pass(300);
    cluster.partition("api", "orders");
    await cluster.pass(600);
    const peers = await cluster.run(cluster.handles.get("api")!.peers({ name: "orders" })).await();
    expect(peers).toEqual({ _tag: "Success", value: [] });
  });

  test("call enforces a deadline and retries only when permitted", async () => {
    cluster = new TestCluster();
    await cluster.start();
    await cluster.add("api", [], "api");
    const api = cluster.handles.get("api")!;
    let calls = 0;
    const called = api.call(
      { name: "api", command: "get" },
      () => {
        calls++;
        return calls === 1 ? fail("transient") : succeed(42);
      },
      { timeoutMs: 50, maxAttempts: 2, retryOn: (error) => error === "transient" },
    );
    expect(await cluster.run(called.orDie()).await()).toEqual({ _tag: "Success", value: 42 });
    expect(calls).toBe(2);

    const missing = await cluster
      .run(api.select({ name: "api", command: "delete" }).either())
      .await();
    if (missing._tag === "Success" && missing.value._tag === "Left") {
      expect(missing.value.left._tag).toBe("ServiceUnavailable");
    } else throw new Error("missing command was selected");

    calls = 0;
    const rejected = await cluster
      .run(
        api
          .call(
            { name: "api", command: "get" },
            () => {
              calls++;
              return fail("business");
            },
            { timeoutMs: 50, maxAttempts: 3, retryOn: () => false },
          )
          .either(),
      )
      .await();
    expect(rejected._tag).toBe("Success");
    expect(calls).toBe(1);

    const timed = cluster.run(
      api
        .call({ name: "api", command: "get" }, () => sleep(1_000).map(() => 1), { timeoutMs: 20 })
        .either(),
    );
    await cluster.pass(30);
    const exit = await timed.await();
    expect(exit._tag).toBe("Success");
    if (exit._tag === "Success") expect(exit.value._tag).toBe("Left");
  });
});

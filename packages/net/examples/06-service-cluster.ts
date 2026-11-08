// Run in two terminals:
//   bun packages/net/examples/06-service-cluster.ts orders
//   bun packages/net/examples/06-service-cluster.ts api
// Set CLUSTER_HOST to the address other machines can reach and CLUSTER_SEED
// on the api process to the orders machine's address.

import { eff, provide, scoped, sleep, succeed } from "@spilne/perfect-core";
import {
  BinaryCodec,
  Cluster,
  ConnectError,
  type NetError,
  ServiceTimeout,
  Tcp,
  Transport,
} from "../src";

type Command =
  | { readonly command: "get"; readonly id: string }
  | { readonly command: "result"; readonly value: string };

const codec = BinaryCodec.json<Command>();
const role = process.argv[2];
const host = process.env.CLUSTER_HOST ?? "127.0.0.1";
const seed = process.env.CLUSTER_SEED ?? "127.0.0.1";

if (role === "orders") {
  await scoped(
    eff(function* () {
      yield* Tcp.listen(
        { host: "0.0.0.0", port: 9001 },
        {
          codec,
          handle: (connection) =>
            connection.messages.forEach((message) =>
              message.command === "get"
                ? connection.send({ command: "result", value: `order:${message.id}` })
                : succeed(undefined),
            ),
        },
      );
      yield* provide(
        Cluster.builder({
          id: `orders-${process.pid}`,
          address: { host: "0.0.0.0", port: 7946 },
          advertise: { host, port: 7946 },
        })
          .service({
            name: "orders",
            version: "v1",
            commands: ["get"],
            address: { host, port: 9001 },
          })
          .join(),
        Transport,
        Tcp,
      );
      console.log(`orders service ready at ${host}:9001`);
      while (true) yield* sleep(60_000);
    }),
  )
    .orDie()
    .run();
} else if (role === "api") {
  const result = await scoped(
    provide(
      eff(function* () {
        const cluster = yield* Cluster.join({
          id: `api-${process.pid}`,
          address: { host: "0.0.0.0", port: 7947 },
          advertise: { host, port: 7947 },
          seeds: [{ host: seed, port: 7946 }],
          services: [],
        });
        yield* sleep(1_000);
        return yield* cluster.call<Command | undefined, ConnectError | NetError>(
          { name: "orders", version: "v1", command: "get" },
          (peer) =>
            scoped(
              Tcp.connect(peer.service.address, { codec }).flatMap((connection) =>
                connection
                  .send({ command: "get", id: "42" })
                  .flatMap(() => connection.messages.take(1).toArray())
                  .map((messages) => messages[0]),
              ),
            ),
          {
            timeoutMs: 1_000,
            maxAttempts: 2,
            retryOn: (error) => error instanceof ConnectError || error instanceof ServiceTimeout,
          },
        );
      }),
      Transport,
      Tcp,
    ),
  )
    .orDie()
    .run();
  console.log(result);
} else {
  throw new Error("Pass orders or api");
}

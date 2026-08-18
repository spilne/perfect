import { describe, expect, test } from "bun:test";
import { run, succeed } from "@perfect/core";
import { PgRef } from "../src/lib/pg-ref";
import { fakeDb } from "./fake-db";

describe("PostgresError", () => {
  test("PgRef exposes driver failures in the typed channel", async () => {
    const { db } = fakeDb(() => {
      throw new Error("database unavailable");
    });
    const ref = new PgRef({ db, name: "counter", initial: 0 });

    const error = await run(ref.get.catchTag("PostgresError", (failure) => succeed(failure)));

    expect(error._tag).toBe("PostgresError");
    expect(error.operation).toBe("ref.get");
    expect(error.cause).toBeInstanceOf(Error);
  });
});

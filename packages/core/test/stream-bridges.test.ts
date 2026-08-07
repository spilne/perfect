import { describe, test, expect } from "bun:test";
import { Stream, run, sync } from "../src";
import { EventEmitter } from "node:events";

describe("Stream.fromCallback", () => {
  test("synchronous emits drain into chunks", async () => {
    const s = Stream.fromCallback<number>((emit, close) => {
      emit(1);
      emit(2);
      emit(3);
      close();
    });
    const result = await run(s.toArray() as any);
    expect(result).toEqual([1, 2, 3]);
  });

  test("async emits (setTimeout) arrive over time", async () => {
    const s = Stream.fromCallback<number>((emit, close) => {
      let i = 0;
      const id = setInterval(() => {
        emit(++i);
        if (i >= 3) {
          clearInterval(id);
          close();
        }
      }, 5);
      return () => clearInterval(id);
    });
    const result = await run(s.toArray() as any);
    expect(result).toEqual([1, 2, 3]);
  });

  test("close before any emit → empty stream", async () => {
    const s = Stream.fromCallback<number>((_emit, close) => {
      close();
    });
    expect(await run(s.toArray() as any)).toEqual([]);
  });

  test("overflow past bufferSize silently drops", async () => {
    // emit 10 items synchronously with a 3-item buffer; consumer sees first 3
    const s = Stream.fromCallback<number>((emit, close) => {
      for (let i = 1; i <= 10; i++) emit(i);
      close();
    }, 3);
    const result = await run(s.toArray() as any);
    expect(result).toEqual([1, 2, 3]);
  });

  test("cleanup fires when close() is called", async () => {
    let cleanedUp = false;
    const s = Stream.fromCallback<number>((emit, close) => {
      emit(1);
      emit(2);
      close();
      return () => {
        cleanedUp = true;
      };
    });
    await run(s.toArray() as any);
    expect(cleanedUp).toBe(true);
  });

  test("onFinalize runs exactly once for multi-pull streams", async () => {
    let finalized = 0;
    const s = Stream.fromArray([1, 2, 3])
      .rechunk(1)
      .onFinalize(
        sync(() => {
          finalized++;
        }),
      );

    expect(await run(s.toArray() as any)).toEqual([1, 2, 3]);
    expect(finalized).toBe(1);
  });
});

describe("Stream.fromEventEmitter", () => {
  test("receives data events until 'end'", async () => {
    const ee = new EventEmitter();
    const stream = Stream.fromEventEmitter<string>(ee, "data");

    // kick emissions after the fiber has attached its listeners
    setTimeout(() => {
      ee.emit("data", "a");
      ee.emit("data", "b");
      ee.emit("data", "c");
      ee.emit("end");
    }, 10);

    const result = await run(stream.toArray() as any);
    expect(result).toEqual(["a", "b", "c"]);
  });

  test("'close' also ends the stream", async () => {
    const ee = new EventEmitter();
    const stream = Stream.fromEventEmitter<number>(ee, "tick");
    setTimeout(() => {
      ee.emit("tick", 1);
      ee.emit("close");
    }, 10);
    expect(await run(stream.toArray() as any)).toEqual([1]);
  });

  test("listener is removed on cleanup", async () => {
    const ee = new EventEmitter();
    expect(ee.listenerCount("data")).toBe(0);
    const stream = Stream.fromEventEmitter<number>(ee, "data");
    setTimeout(() => {
      ee.emit("data", 42);
      ee.emit("end");
    }, 10);
    await run(stream.toArray() as any);
    // After stream termination, all listeners should have been removed.
    expect(ee.listenerCount("data")).toBe(0);
    expect(ee.listenerCount("end")).toBe(0);
    expect(ee.listenerCount("close")).toBe(0);
  });

  test("listener is removed when consumer stops early", async () => {
    const ee = new EventEmitter();
    const stream = Stream.fromEventEmitter<number>(ee, "data");

    setTimeout(() => {
      ee.emit("data", 1);
      ee.emit("data", 2);
    }, 10);

    await run(stream.take(1).drain() as any);
    expect(ee.listenerCount("data")).toBe(0);
    expect(ee.listenerCount("end")).toBe(0);
    expect(ee.listenerCount("close")).toBe(0);
  });
});

describe("Stream.async", () => {
  test("effectful setup, plain emits", async () => {
    let setup = 0;
    const s = Stream.async<number, never>((emit, close) =>
      sync(() => {
        setup++;
        emit(1);
        emit(2);
        close();
        return undefined;
      }),
    );
    expect(await run(s.toArray() as any)).toEqual([1, 2]);
    expect(setup).toBe(1);
  });

  test("effectful cleanup runs when close() is called", async () => {
    let cleaned = 0;
    const s = Stream.async<number, never>((emit, close) =>
      sync(() => {
        emit(1);
        emit(2);
        close();
        return () => {
          cleaned++;
        };
      }),
    );
    await run(s.toArray() as any);
    expect(cleaned).toBe(1);
  });
});

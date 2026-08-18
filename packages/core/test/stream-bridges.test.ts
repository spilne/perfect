import { describe, test, expect } from "bun:test";
import { Chunk, Stream, TaggedError, run, succeed, sync, type Throws } from "../src";
import { EventEmitter } from "node:events";

class BridgeError extends TaggedError("BridgeError")<{ readonly message: string }>() {}

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

  test("push failures enter the typed stream channel after buffered values", async () => {
    const seen: number[] = [];
    let cleaned = 0;
    const stream = Stream.async<number, Throws<BridgeError>>((emit, _close, failStream) =>
      sync(() => {
        emit(1);
        failStream(new BridgeError({ message: "source failed" }));
        return () => {
          cleaned++;
        };
      }),
    );

    const message = await run(
      stream
        .tap((value) => seen.push(value))
        .drain()
        .map(() => "unexpected")
        .catchTag("BridgeError", (error) => succeed(error.message)),
    );

    expect(message).toBe("source failed");
    expect(seen).toEqual([1]);
    expect(cleaned).toBe(1);
  });
});

describe("Stream.asyncChunks", () => {
  test("preserves callback batch boundaries", async () => {
    const stream = Stream.asyncChunks<number, never>((emit, close) =>
      sync(() => {
        emit(Chunk.fromArray([1, 2]));
        emit(Chunk.fromArray([3, 4, 5]));
        close();
      }),
    );

    const sizes = await run(stream.mapChunks((chunk) => Chunk.single(chunk.length)).toArray());
    expect(sizes).toEqual([2, 3]);
  });

  test("drops empty chunks", async () => {
    const stream = Stream.asyncChunks<number, never>((emit, close) =>
      sync(() => {
        emit(Chunk.empty());
        emit(Chunk.single(1));
        close();
      }),
    );

    expect(await run(stream.toArray())).toEqual([1]);
  });

  test("push failures preserve emitted chunks before failing", async () => {
    const seen: number[] = [];
    const stream = Stream.asyncChunks<number, Throws<BridgeError>>((emit, _close, failStream) =>
      sync(() => {
        emit(Chunk.fromArray([1, 2]));
        failStream(new BridgeError({ message: "batch failed" }));
      }),
    );

    const message = await run(
      stream
        .tap((value) => seen.push(value))
        .drain()
        .map(() => "unexpected")
        .catchTag("BridgeError", (error) => succeed(error.message)),
    );

    expect(message).toBe("batch failed");
    expect(seen).toEqual([1, 2]);
  });
});

describe("Stream.async cleanup on normal completion", () => {
  test("cleanup runs when a take(n) completes exactly on the nth emit", async () => {
    let cleanedUp = false;

    const s = Stream.async<number, never>((emit) => {
      emit(1);
      emit(2);
      emit(3);
      return sync(() => () => {
        cleanedUp = true;
      }) as any;
    });

    const result = await run((s as any).take(3).toArray());
    expect(result).toEqual([1, 2, 3]);
    expect(cleanedUp).toBe(true);
  });

  test("cleanup still runs exactly once when close() ends the stream", async () => {
    let cleanups = 0;

    const s = Stream.async<number, never>((emit, close) => {
      emit(1);
      close();
      return sync(() => () => {
        cleanups++;
      }) as any;
    });

    const result = await run((s as any).toArray());
    expect(result).toEqual([1]);
    expect(cleanups).toBe(1);
  });
});

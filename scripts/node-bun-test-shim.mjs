import * as nodeTest from "node:test";
import { spawnSync as nodeSpawnSync, spawn as nodeSpawn } from "node:child_process";
import { Worker as NodeWorker } from "node:worker_threads";
import { Readable } from "node:stream";
import { createReadStream } from "node:fs";
import { ReadableStream } from "node:stream/web";
import { expect as jestExpect } from "expect";
import { formatEachTitle } from "./node-bun-test-titles.mjs";

const { describe, it, test, before, beforeEach, after, afterEach } = nodeTest;

export { describe, it, test, before, beforeEach, after, afterEach };

export const beforeAll = nodeTest.beforeAll ?? before;
export const afterAll = nodeTest.afterAll ?? after;

export { spyOn } from "jest-mock";

// ── expect(value, message) ─────────────────────────────────────────
// Bun accepts a custom failure message as expect's second argument; the
// `expect` package rejects a second argument outright. The message leads the
// matcher's own message, as it does in Bun.

const withFailureMessage = (error, message) => {
  if (error instanceof Error) error.message = `${message}\n\n${error.message}`;
  return error;
};

const withMessage = (target, message) =>
  new Proxy(target, {
    get(object, key) {
      const value = Reflect.get(object, key);
      if (typeof value === "function") {
        return (...args) => {
          let result;
          try {
            result = value.apply(object, args);
          } catch (error) {
            throw withFailureMessage(error, message);
          }
          return result instanceof Promise
            ? result.catch((error) => {
                throw withFailureMessage(error, message);
              })
            : result;
        };
      }
      return value !== null && typeof value === "object" ? withMessage(value, message) : value;
    },
  });

export const expect = new Proxy(jestExpect, {
  apply(target, thisArg, args) {
    const matchers = Reflect.apply(target, thisArg, args.slice(0, 1));
    return args.length > 1 && args[1] !== undefined
      ? withMessage(matchers, String(args[1]))
      : matchers;
  },
});

// ── test.each / describe.each ──────────────────────────────────────
// Bun semantics: an array row is spread into the callback's arguments, any
// other row is passed as the only argument, and titles are formatted by
// node-bun-test-titles.mjs.

const rowArguments = (row) => (Array.isArray(row) ? row : [row]);

// A callback declaring more parameters than the row supplies receives a
// `done` callback, as in Bun and Jest.
const callWithRow = (fn, args) => {
  if (fn.length <= args.length) return fn(...args);
  return new Promise((resolve, reject) => {
    fn(...args, (error) => (error ? reject(error) : resolve()));
  });
};

const testOptions = (options) => (typeof options === "number" ? { timeout: options } : options);

const eachTest = (register) => (table) => (title, fn, options) => {
  Array.from(table).forEach((row, index) => {
    const args = rowArguments(row);
    register(formatEachTitle(title, args, index), testOptions(options) ?? {}, () =>
      callWithRow(fn, args),
    );
  });
};

const eachDescribe = (register) => (table) => (title, fn) => {
  Array.from(table).forEach((row, index) => {
    const args = rowArguments(row);
    register(formatEachTitle(title, args, index), () => fn(...args));
  });
};

for (const register of [test, it]) {
  register.each = eachTest(register);
  for (const variant of ["skip", "only", "todo"]) {
    if (typeof register[variant] === "function") {
      register[variant].each = eachTest(register[variant]);
    }
  }
}

describe.each = eachDescribe(describe);
for (const variant of ["skip", "only", "todo"]) {
  if (typeof describe[variant] === "function") {
    describe[variant].each = eachDescribe(describe[variant]);
  }
}

if (!globalThis.Worker) {
  globalThis.Worker = NodeWorker;
}

if (expect.extend) {
  expect.extend({
    toStartWith(received, expected) {
      const pass = typeof received === "string" && received.startsWith(expected);
      return {
        pass,
        message: () =>
          `expected ${this.utils.printReceived(received)} toStartWith ${this.utils.printExpected(expected)}`,
      };
    },
  });
}

export const skipIf = (condition) => {
  if (condition) {
    return describe.skip;
  }

  return describe;
};

export const testSkipIf = (condition) => {
  if (condition) {
    return test.skip;
  }

  return test;
};

it.skipIf = testSkipIf;
test.skipIf = testSkipIf;
describe.skipIf = skipIf;

const toBuffer = (value) => {
  if (typeof value === "string") {
    return Buffer.from(value);
  }

  if (value instanceof Buffer) {
    return value;
  }

  if (value === undefined || value === null) {
    return Buffer.alloc(0);
  }

  if (value instanceof Uint8Array) {
    return Buffer.from(value);
  }

  return Buffer.from(String(value));
};

const createReadableBody = (stream) => {
  if (!stream) {
    return undefined;
  }

  if (typeof Readable.toWeb === "function") {
    return Readable.toWeb(stream);
  }

  return new ReadableStream({
    start(controller) {
      stream.on("data", (chunk) => {
        controller.enqueue(toBuffer(chunk));
      });

      stream.on("end", () => {
        controller.close();
      });
      stream.on("error", (error) => {
        controller.error(error);
      });
    },
  });
};

const createSpawnResult = (child) => ({
  exited: new Promise((resolve, reject) => {
    child.once("error", reject);
    child.once("exit", (code, signal) => {
      if (signal) {
        resolve(`signal:${signal}`);
        return;
      }

      resolve(code === null ? 0 : code);
    });
  }),
  kill: child.kill.bind(child),
  stdout: child.stdout ? createReadableBody(child.stdout) : undefined,
  stderr: child.stderr ? createReadableBody(child.stderr) : undefined,
  stdin: child.stdin,
  nodeProcess: child,
});

const normalizeArgs = (args) => {
  if (Array.isArray(args)) {
    return { cmd: args };
  }

  return args;
};

export const Bun = {
  sleep: (ms) => new Promise((resolve) => setTimeout(resolve, ms)),
  file: (path) => {
    const target = `${path instanceof URL ? path.pathname : String(path)}`;

    return {
      text: () => Bun.fileReadText(target),
      stream: () => {
        const stream = Readable.toWeb(createReadStream(target));
        return Promise.resolve(stream);
      },
    };
  },
  spawnSync: (input) => {
    const { cmd, stdout = "pipe", stderr = "pipe", timeout } = normalizeArgs(input);
    const stdio = [
      "ignore",
      stdout === "pipe" ? "pipe" : stdout === "ignore" ? "ignore" : stdout,
      stderr === "pipe" ? "pipe" : stderr === "ignore" ? "ignore" : stderr,
    ];

    const result = nodeSpawnSync(cmd[0], cmd.slice(1), {
      stdio,
      timeout,
      encoding: "utf8",
      shell: false,
    });

    return {
      exitCode: result.status,
      stdout: result.stdout,
      stderr: result.stderr,
      signal: result.signal,
    };
  },
  spawn: (input) => {
    const { cmd, stdout = "pipe", stderr = "pipe" } = normalizeArgs(input);
    const stdio = [
      "ignore",
      stdout === "pipe" ? "pipe" : stdout === "ignore" ? "ignore" : stdout,
      stderr === "pipe" ? "pipe" : stderr === "ignore" ? "ignore" : stderr,
    ];

    const child = nodeSpawn(cmd[0], cmd.slice(1), {
      stdio,
    });

    return createSpawnResult(child);
  },
};

Bun.fileReadText = async (path) => {
  const fs = await import("node:fs/promises");
  return fs.readFile(path, "utf8");
};

Object.assign(globalThis, { Bun });

export const setDefaultTimeout = () => {};

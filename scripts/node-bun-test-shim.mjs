import * as nodeTest from 'node:test';
import { spawnSync as nodeSpawnSync, spawn as nodeSpawn } from 'node:child_process';
import { Worker as NodeWorker } from 'node:worker_threads';
import { Readable } from 'node:stream';
import { createReadStream } from 'node:fs';
import { ReadableStream } from 'node:stream/web';
import { expect } from 'expect';

export const {
  describe,
  it,
  test,
  before,
  beforeEach,
  beforeAll,
  after,
  afterEach,
  afterAll,
} = nodeTest;

export { expect };

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
  if (typeof value === 'string') {
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

  if (typeof Readable.toWeb === 'function') {
    return Readable.toWeb(stream);
  }

  return new ReadableStream({
    start(controller) {
      stream.on('data', (chunk) => {
        controller.enqueue(toBuffer(chunk));
      });

      stream.on('end', () => {
        controller.close();
      });
      stream.on('error', (error) => {
        controller.error(error);
      });
    },
  });
};

const createSpawnResult = (child) => ({
  exited: new Promise((resolve, reject) => {
    child.once('error', reject);
    child.once('exit', (code, signal) => {
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
        const stream = Readable.toWeb(
          createReadStream(target),
        );
        return Promise.resolve(stream);
      },
    };
  },
  spawnSync: (input) => {
    const { cmd, stdout = 'pipe', stderr = 'pipe', timeout } = normalizeArgs(input);
    const stdio = [
      'ignore',
      stdout === 'pipe' ? 'pipe' : stdout === 'ignore' ? 'ignore' : stdout,
      stderr === 'pipe' ? 'pipe' : stderr === 'ignore' ? 'ignore' : stderr,
    ];

    const result = nodeSpawnSync(cmd[0], cmd.slice(1), {
      stdio,
      timeout,
      encoding: 'utf8',
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
    const { cmd, stdout = 'pipe', stderr = 'pipe' } = normalizeArgs(input);
    const stdio = [
      'ignore',
      stdout === 'pipe' ? 'pipe' : stdout === 'ignore' ? 'ignore' : stdout,
      stderr === 'pipe' ? 'pipe' : stderr === 'ignore' ? 'ignore' : stderr,
    ];

    const child = nodeSpawn(cmd[0], cmd.slice(1), {
      stdio,
    });

    return createSpawnResult(child);
  },
};

Bun.fileReadText = async (path) => {
  const fs = await import('node:fs/promises');
  return fs.readFile(path, 'utf8');
};

Object.assign(globalThis, { Bun });

export const setDefaultTimeout = () => {};

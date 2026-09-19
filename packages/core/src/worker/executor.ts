// Worker-side executor — this file runs INSIDE each worker thread.
// Bun uses the web-worker surface (`onmessage`/`postMessage`), while
// Node worker_threads uses `parentPort`.

type WorkerMessageData = {
  id: number;
  fnSource: string;
  arg: unknown;
};

type WorkerNodePort = {
  on(event: "message", handler: (value: WorkerMessageData) => void): void;
  postMessage(message: unknown): void;
};

// `require` does not exist in an ESM module, which is exactly what the published
// `dist/worker/executor.js` is ("type": "module"), so the old `require("node:…")`
// always threw and this fell through to the web branch — leaving a Node worker
// that listened on nothing. `process.getBuiltinModule` reaches the builtin
// synchronously from ESM; browsers have no `process` and take the web branch.
const nodeWorkerPort: WorkerNodePort | null = (() => {
  try {
    const proc = (globalThis as { process?: { getBuiltinModule?: (id: string) => unknown } })
      .process;
    const workerThreads = proc?.getBuiltinModule?.("node:worker_threads") as
      | { parentPort?: WorkerNodePort | null }
      | undefined;
    return workerThreads?.parentPort ?? null;
  } catch {
    return null;
  }
})();

const postBack = (message: unknown) => {
  if (nodeWorkerPort?.postMessage) {
    nodeWorkerPort.postMessage(message);
    return;
  }

  (globalThis as { postMessage?: (message: unknown) => void }).postMessage?.(message);
};

const handleMessage = async (event: { data: WorkerMessageData }) => {
  const { id, fnSource, arg } = event.data;

  try {
    // reconstruct the function from source
    const fn = new Function("return " + fnSource)();
    const result = await fn(arg);
    postBack({ id, ok: true, value: result });
  } catch (error) {
    postBack({
      id,
      ok: false,
      error: error instanceof Error ? error.message : String(error),
    });
  }
};

if (nodeWorkerPort) {
  nodeWorkerPort.on("message", (payload: WorkerMessageData) => {
    void handleMessage({ data: payload });
  });
} else {
  // In a worker this global is a DedicatedWorkerGlobalScope, but the DOM lib
  // types it as Window — whose `onmessage` takes a full MessageEvent. Only the
  // `data` field is ever read, so narrow it through unknown.
  (globalThis as unknown as { onmessage: (event: { data: WorkerMessageData }) => void }).onmessage =
    handleMessage;
}

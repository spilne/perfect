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

const nodeWorkerPort: WorkerNodePort | null = (() => {
  try {
    // eslint-disable-next-line @typescript-eslint/no-require-imports
    const { parentPort } = require("node:worker_threads");
    return parentPort;
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
  (globalThis as { onmessage: (event: { data: WorkerMessageData }) => void }).onmessage =
    handleMessage;
}

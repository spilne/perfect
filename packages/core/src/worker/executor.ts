// Worker-side executor — this file runs INSIDE each worker thread
// It receives tasks via postMessage and sends results back

type WorkerGlobal = {
  onmessage: ((event: MessageEvent) => void | Promise<void>) | null;
  postMessage(message: unknown): void;
};

const workerSelf = globalThis as unknown as WorkerGlobal;

workerSelf.onmessage = async (event: MessageEvent) => {
  const { id, fnSource, arg } = event.data as {
    id: number;
    fnSource: string;
    arg: unknown;
  };

  try {
    // reconstruct the function from source
    const fn = new Function("return " + fnSource)();
    const result = await fn(arg);
    workerSelf.postMessage({ id, ok: true, value: result });
  } catch (error) {
    workerSelf.postMessage({
      id,
      ok: false,
      error: error instanceof Error ? error.message : String(error),
    });
  }
};

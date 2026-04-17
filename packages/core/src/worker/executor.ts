// Worker-side executor — this file runs INSIDE each worker thread
// It receives tasks via postMessage and sends results back

declare var self: Worker;

self.onmessage = async (event: MessageEvent) => {
  const { id, fnSource, arg } = event.data as {
    id: number;
    fnSource: string;
    arg: unknown;
  };

  try {
    // reconstruct the function from source
    const fn = new Function("return " + fnSource)();
    const result = await fn(arg);
    self.postMessage({ id, ok: true, value: result });
  } catch (error) {
    self.postMessage({
      id,
      ok: false,
      error: error instanceof Error ? error.message : String(error),
    });
  }
};

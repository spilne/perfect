interface WorkerResponse {
  readonly id: number;
  readonly ok: boolean;
  readonly value?: unknown;
  readonly error?: { readonly name: string; readonly message: string };
}

let nextRunId = 0;

export function runSource(source: string, timeoutMs = 5_000): Promise<unknown> {
  const id = ++nextRunId;
  const worker = new Worker(new URL("./playground-worker.ts", import.meta.url), { type: "module" });

  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => {
      worker.terminate();
      reject(new Error(`Execution exceeded ${timeoutMs} ms`));
    }, timeoutMs);

    const finish = (result: () => void) => {
      clearTimeout(timeout);
      worker.terminate();
      result();
    };

    worker.addEventListener("message", (event: MessageEvent<WorkerResponse>) => {
      if (event.data.id !== id) return;
      if (event.data.ok) {
        finish(() => resolve(event.data.value));
        return;
      }
      const error = event.data.error;
      finish(() =>
        reject(
          new Error(error === undefined ? "Execution failed" : `${error.name}: ${error.message}`),
        ),
      );
    });

    worker.addEventListener("error", (event) => {
      finish(() => reject(new Error(event.message || "Playground worker failed")));
    });

    worker.postMessage({ id, source });
  });
}

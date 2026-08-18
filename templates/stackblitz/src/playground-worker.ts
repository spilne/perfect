import { evaluateSource } from "./runtime";

interface RunRequest {
  readonly id: number;
  readonly source: string;
}

type RunResponse =
  | { readonly id: number; readonly ok: true; readonly value: unknown }
  | {
      readonly id: number;
      readonly ok: false;
      readonly error: { readonly name: string; readonly message: string };
    };

const workerScope = globalThis as unknown as {
  addEventListener(type: "message", listener: (event: MessageEvent<RunRequest>) => void): void;
  postMessage(response: RunResponse): void;
};

workerScope.addEventListener("message", (event) => {
  const { id, source } = event.data;
  void evaluateSource(source).then(
    (value) => workerScope.postMessage({ id, ok: true, value }),
    (error: unknown) => workerScope.postMessage({ id, ok: false, error: describeError(error) }),
  );
});

function describeError(error: unknown): { name: string; message: string } {
  if (error instanceof Error) return { name: error.name, message: error.message };
  return { name: "Error", message: String(error) };
}

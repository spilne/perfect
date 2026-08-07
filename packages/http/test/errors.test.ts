import { describe, test, expect } from "bun:test";
import {
  HttpNetworkError,
  HttpTimeoutError,
  HttpStatusError,
  HttpParseError,
  HTTP_RETRYABLE,
} from "../src";

describe("HttpClientError — tagged error classes", () => {
  test("HttpNetworkError — instance has _tag + props + Error semantics", () => {
    const e = new HttpNetworkError({
      url: "https://x",
      cause: new Error("ECONNREFUSED"),
      message: "connection refused",
    });
    expect(e._tag).toBe("HttpNetworkError");
    expect(e.url).toBe("https://x");
    expect(e instanceof Error).toBe(true);
    expect(e instanceof HttpNetworkError).toBe(true);
    expect(e.name).toBe("HttpNetworkError");
  });

  test("HttpTimeoutError — static _tag", () => {
    expect(HttpTimeoutError._tag).toBe("HttpTimeoutError");
  });

  test("HttpStatusError — isRetryable / isClientError / isServerError", () => {
    const fiveHundred = new HttpStatusError({ url: "x", status: 500, body: "", message: "" });
    expect(fiveHundred.isRetryable).toBe(true);
    expect(fiveHundred.isServerError).toBe(true);
    expect(fiveHundred.isClientError).toBe(false);

    const tooMany = new HttpStatusError({ url: "x", status: 429, body: "", message: "" });
    expect(tooMany.isRetryable).toBe(true);

    const fourOhFour = new HttpStatusError({ url: "x", status: 404, body: "", message: "" });
    expect(fourOhFour.isRetryable).toBe(false);
    expect(fourOhFour.isClientError).toBe(true);
    expect(fourOhFour.isServerError).toBe(false);
  });

  test("HTTP_RETRYABLE — classifies transient vs permanent", () => {
    expect(HTTP_RETRYABLE(new HttpNetworkError({ url: "x", cause: null, message: "" }))).toBe(true);
    expect(HTTP_RETRYABLE(new HttpTimeoutError({ url: "x", timeoutMs: 30, message: "" }))).toBe(
      true,
    );
    expect(
      HTTP_RETRYABLE(new HttpStatusError({ url: "x", status: 503, body: "", message: "" })),
    ).toBe(true);
    expect(
      HTTP_RETRYABLE(new HttpStatusError({ url: "x", status: 400, body: "", message: "" })),
    ).toBe(false);
    expect(HTTP_RETRYABLE(new HttpParseError({ url: "x", cause: null, message: "" }))).toBe(false);
  });
});

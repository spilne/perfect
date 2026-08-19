import { describe, test, expect } from "bun:test";
import { RetryAttempt } from "../src";

describe("RetryAttempt", () => {
  test("constructors and guards for success/error/thrown", () => {
    const s = RetryAttempt.success(1);
    const e = RetryAttempt.error("boom");
    const t = RetryAttempt.thrown(new Error("x"));

    expect(RetryAttempt.isSuccess(s)).toBe(true);
    expect(RetryAttempt.isError(e)).toBe(true);
    expect(RetryAttempt.isThrown(t)).toBe(true);
    expect(RetryAttempt.isSuccess(e)).toBe(false);

    switch (s._tag) {
      case "success":
        expect(s.value).toBe(1);
        break;
      default:
        expect.fail("unexpected tag");
    }

    switch (e._tag) {
      case "error":
        expect(e.error).toBe("boom");
        break;
      default:
        expect.fail("unexpected tag");
    }

    expect(t._tag).toBe("thrown");
  });
});

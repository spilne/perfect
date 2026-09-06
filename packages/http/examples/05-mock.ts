// MockHttpClient — record calls, return canned responses, assert behavior.
//
// Run: bun packages/http/examples/05-mock.ts

import { type ResponseParser, HttpStatusError, MockHttpClient } from "../src";
import { assertEq } from "./_assert";

interface User {
  id: number;
  name: string;
}
const UserSchema: ResponseParser<User> = {
  safeParse: (d: unknown) =>
    d !== null &&
    typeof d === "object" &&
    "id" in d &&
    "name" in d &&
    typeof d.id === "number" &&
    typeof d.name === "string"
      ? { success: true, data: { id: d.id, name: d.name } }
      : { success: false, error: "no" },
};

// >>> example: mock-basic
// Set up route → response, run the program, assert what was called.
const mock = new MockHttpClient();
mock.on("GET", "/users/1", { id: 1, name: "alice" });

const user = await mock.get("/users/1", UserSchema).orDie().run();
assertEq(user, { id: 1, name: "alice" });
assertEq(mock.calledTimes("GET", "/users/1"), 1);
// <<< example

// >>> example: mock-failure
// MockHttpClient.fail builds an HttpStatusError for use as a route response.
mock.reset();
mock.on("GET", "/users/999", MockHttpClient.fail(404, "not found"));

let caught: unknown;
try {
  await mock.get("/users/999", UserSchema).orDie().run();
} catch (e) {
  caught = e;
}
if (!(caught instanceof HttpStatusError)) throw new Error("Expected HttpStatusError");
assertEq(caught._tag, "HttpStatusError");
assertEq(caught.status, 404);
// <<< example

// >>> example: mock-sequence
// onSequence consumes responses in order; the last item is reused after the
// queue exhausts. Useful for simulating retry-then-succeed scenarios.
mock.reset();
mock.onSequence("GET", "/u", [MockHttpClient.fail(503, "down"), { id: 7, name: "after-retry" }]);

let firstErr: unknown;
try {
  await mock.get("/u", UserSchema).orDie().run();
} catch (e) {
  firstErr = e;
}
if (!(firstErr instanceof HttpStatusError)) throw new Error("Expected HttpStatusError");
assertEq(firstErr.status, 503);

const second = await mock.get("/u", UserSchema).orDie().run();
assertEq(second, { id: 7, name: "after-retry" });
// <<< example

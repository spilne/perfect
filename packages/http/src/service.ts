// HttpClient as a Perfect service — Layer-injected DI for handlers,
// business logic, etc.
//
//   const HttpClientLive = succeed({ HttpClient: new DefaultHttpClient({...}) });
//   const program = eff(function* () {
//     const client = yield* HttpClient.get;
//     return yield* client.get("/users/1", UserSchema);
//   });
//   await run(program.with(HttpClientLive));
//
// You can also use a DefaultHttpClient directly without Layer — the service
// tag is just for when you want DI.

import { service } from "@spilne/perfect-core";
import type { HttpClient as HttpClientT } from "./client";

export const HttpClient = service<HttpClientT>("HttpClient");

// Opt-in: `import "@spilne/perfect-core/thenable"` makes every effect
// awaitable, so `await eff` runs it like `run(eff)` (without run's check
// that every error is handled).
//
// It is not on by default because it is easy to run an effect by accident:
// returning an effect from an async function, or passing it to
// Promise.resolve / Promise.all, runs it too. Without this import, use
// run(eff) or eff.run().
import "./syntax/thenable.js";

export {};

// StateBackend moved to @spilne/perfect-core/connect so durable backends can
// implement it without depending on topology. Re-exported here for compat.
export { type StateBackend, InMemoryState } from "@spilne/perfect-core/connect";

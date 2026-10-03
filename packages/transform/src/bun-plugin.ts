import { plugin } from "bun";
import { createTransformPlugin } from "./transform-plugin.js";

// The Rust CLI transformer (crates/perfect-transform) is intentionally NOT
// used here: its output diverges from the TS rewriter (guard handling, yield
// desugaring), so silently preferring it when a local binary happens to be
// built would change program semantics. One transformer, one behavior.

plugin(createTransformPlugin("perfect-effect-transform"));

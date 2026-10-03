import { plugin } from "bun";
import { createTransformPlugin } from "./transform-plugin.js";

await plugin(createTransformPlugin("perfect-for-comprehension"));

export {};

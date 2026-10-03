import { plugin } from "bun";
import { createTransformPlugin } from "./transform-plugin.js";

plugin(createTransformPlugin("spilne-eff-transform"));

// Titles of test.each / describe.each rows, formatted the way Bun 1.4 does.
//
// Bun reads the title once, left to right:
// - `%` followed by another character, while row arguments remain:
//   - `%s` a string argument as is; `%d` / `%f` a number; `%i` an integer
//     (int52 range, not -0). Each takes the next argument, and prints the
//     placeholder itself when the argument has another type.
//   - `%j` / `%o` take the next argument as JSON (`""` when JSON has none).
//   - `%p` takes the next argument, pretty-printed.
//   - `%#` is the row index and `%%` a percent sign; neither takes one.
//   - Any other character drops the `%` and is read normally.
//   Once the arguments run out, `%` is an ordinary character.
// - `$path`, when the first argument is an object: `path` is a JavaScript
//   identifier, optionally followed by `.segment`s. Its value is printed (a
//   string as is, anything else pretty-printed). A missing, null or undefined
//   value, or a `$` not followed by an identifier, prints `$` and what was
//   read, and the character after it is skipped.
//
// The pretty format matches Bun for primitives, functions, dates, regular
// expressions, arrays, plain and class objects, Map and Set. Other values
// fall back to node:util's inspect.

import { inspect } from "node:util";

const IDENTIFIER_START = /[\p{ID_Start}$_]/u;
const IDENTIFIER_CONTINUE = /[\p{ID_Continue}$\u200c\u200d]/u;
const PLAIN_KEY = /^[\p{ID_Start}$_][\p{ID_Continue}$\u200c\u200d]*$/u;

const isIdentifierStart = (char) => char !== undefined && IDENTIFIER_START.test(char);
const isIdentifierContinue = (char) => char !== undefined && IDENTIFIER_CONTINUE.test(char);

const isObject = (value) =>
  value !== null && (typeof value === "object" || typeof value === "function");

const isAnyInt = (value) =>
  Number.isInteger(value) && !Object.is(value, -0) && Math.abs(value) <= 2 ** 51;

const indent = (level) => "  ".repeat(level);

const formatKey = (key) =>
  typeof key === "symbol" ? `[${String(key)}]` : PLAIN_KEY.test(key) ? key : JSON.stringify(key);

const formatFunction = (fn) => {
  const source = Function.prototype.toString.call(fn);
  if (source.startsWith("class")) return fn.name ? `[class ${fn.name}]` : "[class (anonymous)]";
  return fn.name ? `[Function: ${fn.name}]` : "[Function]";
};

const formatEntries = (prefix, entries, level) =>
  entries.length === 0
    ? `${prefix}{}`
    : `${prefix}{\n${entries.map((entry) => `${indent(level + 1)}${entry},\n`).join("")}${indent(level)}}`;

const formatArray = (array, level) => {
  if (array.length === 0) return "[]";
  const items = array.map((item) => pretty(item, level + 1));
  // Bun breaks the line when the first element is an object.
  if (isObject(array[0]) && typeof array[0] !== "function") {
    return `[\n${indent(level + 1)}${items.join(", ")}\n${indent(level)}]`;
  }
  return `[ ${items.join(", ")} ]`;
};

const formatObject = (object, level) => {
  const proto = Object.getPrototypeOf(object);
  const prefix =
    proto === null
      ? "[Object: null prototype] "
      : proto === Object.prototype
        ? ""
        : `${proto.constructor?.name ?? "Object"} `;
  const keys = [
    ...Object.keys(object),
    ...Object.getOwnPropertySymbols(object).filter((symbol) =>
      Object.prototype.propertyIsEnumerable.call(object, symbol),
    ),
  ];
  const entries = keys.map((key) => {
    const descriptor = Object.getOwnPropertyDescriptor(object, key);
    const value =
      descriptor && !("value" in descriptor)
        ? descriptor.get && descriptor.set
          ? "[Getter/Setter]"
          : descriptor.get
            ? "[Getter]"
            : "[Setter]"
        : pretty(object[key], level + 1);
    return `${formatKey(key)}: ${value}`;
  });
  return formatEntries(prefix, entries, level);
};

const isPlainArray = (value) =>
  Array.isArray(value) &&
  value.length <= 100 &&
  Object.getPrototypeOf(value) === Array.prototype &&
  Array.from({ length: value.length }, (_, i) => i).every((i) => i in value);

export function pretty(value, level = 0) {
  switch (typeof value) {
    case "string":
      return JSON.stringify(value);
    case "number":
      return Object.is(value, -0) ? "-0" : String(value);
    case "bigint":
      return `${value}n`;
    case "symbol":
    case "boolean":
    case "undefined":
      return String(value);
    case "function":
      return formatFunction(value);
  }
  if (value === null) return "null";
  if (value instanceof Date) return value.toISOString();
  if (value instanceof RegExp) return String(value);
  if (isPlainArray(value)) return formatArray(value, level);
  if (value instanceof Map) {
    const entries = Array.from(
      value,
      ([key, item]) => `${pretty(key, level + 1)}: ${pretty(item, level + 1)}`,
    );
    return formatEntries(value.size === 0 ? "Map " : `Map(${value.size}) `, entries, level);
  }
  if (value instanceof Set) {
    const entries = Array.from(value, (item) => pretty(item, level + 1));
    return formatEntries(value.size === 0 ? "Set " : `Set(${value.size}) `, entries, level);
  }
  const proto = Object.getPrototypeOf(value);
  if (proto === null || proto === Object.prototype || proto.constructor?.prototype === proto) {
    if (!Array.isArray(value) && !ArrayBuffer.isView(value) && !(value instanceof Error)) {
      return formatObject(value, level);
    }
  }
  return inspect(value, { depth: 4 });
}

// A path read from the first argument, as Bun's getIfPropertyExistsFromPath
// does: every segment must exist, and primitives expose their properties.
function lookup(root, path) {
  let current = root;
  for (const key of path.split(".")) {
    if (current === null || current === undefined) return undefined;
    const holder = isObject(current) ? current : Object(current);
    if (!(key in holder)) return undefined;
    current = holder[key];
  }
  return current;
}

export function formatEachTitle(title, args, index) {
  const label = String(title);
  let out = "";
  let next = 0;
  let i = 0;
  while (i < label.length) {
    const char = label[i];
    if (char === "$" && args.length > 0 && isObject(args[0])) {
      const start = i + 1;
      let end = start;
      if (isIdentifierStart(label[end])) {
        end++;
        while (end < label.length) {
          if (label[end] === "." && isIdentifierContinue(label[end + 1])) end++;
          else if (isIdentifierContinue(label[end])) end++;
          else break;
        }
        const value = lookup(args[0], label.slice(start, end));
        if (value !== undefined && value !== null) {
          out += typeof value === "string" ? value : pretty(value);
          i = end;
          continue;
        }
      } else {
        while (end < label.length && isIdentifierContinue(label[end]) && label[end] !== "$") end++;
      }
      out += `$${label.slice(start, end)}`;
      i = end + 1;
      continue;
    }
    if (char === "%" && i + 1 < label.length && next < args.length) {
      const spec = label[i + 1];
      const arg = args[next];
      switch (spec) {
        case "s":
          out += typeof arg === "string" ? arg : "%s";
          break;
        case "d":
        case "f":
          out += typeof arg === "number" ? String(arg) : `%${spec}`;
          break;
        case "i":
          out += isAnyInt(arg) ? String(arg) : "%i";
          break;
        case "j":
        case "o":
          out += JSON.stringify(arg) ?? "";
          break;
        case "p":
          out += pretty(arg);
          break;
        case "#":
          out += String(index);
          i += 2;
          continue;
        case "%":
          out += "%";
          i += 2;
          continue;
        default:
          i += 1;
          continue;
      }
      next++;
      i += 2;
      continue;
    }
    out += char;
    i++;
  }
  return out;
}

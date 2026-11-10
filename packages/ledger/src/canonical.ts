// ---------------------------------------------------------------------------
// Canonical encoding: one byte string per value, so equal data always hashes
// equal. Supported: null, booleans, integers (number or bigint), strings,
// bytes, arrays, and records with string keys. Floats, undefined, NaN and
// non-plain objects are rejected rather than guessed at.
//
//   0x00 null   0x01 false   0x02 true
//   0x03 integer   zigzag LEB128
//   0x04 string    length LEB128, UTF-8
//   0x05 bytes     length LEB128, raw
//   0x06 array     count LEB128, items
//   0x07 record    count LEB128, (key string, value) pairs, keys sorted by
//                  their UTF-8 bytes and unique
//
// `decodeCanonical` accepts only what `encodeCanonical` produces: a
// non-minimal varint, unsorted or repeated keys, or trailing bytes throw.
// ---------------------------------------------------------------------------
import { compareBytes, concatBytes } from "./bytes.js";

export type CanonicalValue =
  | null
  | boolean
  | number
  | bigint
  | string
  | Uint8Array
  | ReadonlyArray<CanonicalValue>
  | { readonly [key: string]: CanonicalValue };

const enc = new TextEncoder();
const dec = new TextDecoder("utf-8", { fatal: true });

const MAX_DEPTH = 64;

function varint(value: bigint): Uint8Array {
  const out: number[] = [];
  let v = value;
  do {
    const byte = Number(v & 0x7fn);
    v >>= 7n;
    out.push(v > 0n ? byte | 0x80 : byte);
  } while (v > 0n);
  return Uint8Array.from(out);
}

const zigzag = (n: bigint): bigint => (n >= 0n ? n * 2n : -n * 2n - 1n);
const unzigzag = (n: bigint): bigint => (n % 2n === 0n ? n / 2n : -(n + 1n) / 2n);

export function encodeCanonical(value: CanonicalValue): Uint8Array {
  return encodeValue(value, 0);
}

function encodeValue(value: CanonicalValue, depth: number): Uint8Array {
  if (depth > MAX_DEPTH) throw new RangeError("value is nested too deeply");
  if (value === null) return Uint8Array.of(0x00);
  if (typeof value === "boolean") return Uint8Array.of(value ? 0x02 : 0x01);
  if (typeof value === "number") {
    if (!Number.isSafeInteger(value)) throw new TypeError(`not a safe integer: ${value}`);
    return concatBytes(Uint8Array.of(0x03), varint(zigzag(BigInt(value))));
  }
  if (typeof value === "bigint") return concatBytes(Uint8Array.of(0x03), varint(zigzag(value)));
  if (typeof value === "string") {
    const bytes = enc.encode(value);
    return concatBytes(Uint8Array.of(0x04), varint(BigInt(bytes.length)), bytes);
  }
  if (value instanceof Uint8Array) {
    return concatBytes(Uint8Array.of(0x05), varint(BigInt(value.length)), value);
  }
  if (Array.isArray(value)) {
    const items = (value as ReadonlyArray<CanonicalValue>).map((v) => encodeValue(v, depth + 1));
    return concatBytes(Uint8Array.of(0x06), varint(BigInt(items.length)), ...items);
  }
  const proto = typeof value === "object" ? Object.getPrototypeOf(value) : undefined;
  if (typeof value === "object" && (proto === Object.prototype || proto === null)) {
    const record = value as { readonly [key: string]: CanonicalValue };
    const entries = Object.keys(record)
      .map((key) => ({ key: enc.encode(key), value: record[key] as CanonicalValue }))
      .sort((a, b) => compareBytes(a.key, b.key));
    const parts = entries.map((e) =>
      concatBytes(
        Uint8Array.of(0x04),
        varint(BigInt(e.key.length)),
        e.key,
        encodeValue(e.value, depth + 1),
      ),
    );
    return concatBytes(Uint8Array.of(0x07), varint(BigInt(parts.length)), ...parts);
  }
  throw new TypeError("value cannot be encoded canonically");
}

export function decodeCanonical(bytes: Uint8Array): CanonicalValue {
  const reader = { bytes, at: 0 };
  const value = decodeValue(reader, 0);
  if (reader.at !== bytes.length) throw new RangeError("trailing bytes after value");
  return value;
}

interface Reader {
  readonly bytes: Uint8Array;
  at: number;
}

function readVarint(r: Reader): bigint {
  let result = 0n;
  let shift = 0n;
  for (;;) {
    if (r.at >= r.bytes.length) throw new RangeError("truncated varint");
    const byte = r.bytes[r.at++]!;
    result |= BigInt(byte & 0x7f) << shift;
    if ((byte & 0x80) === 0) {
      if (byte === 0 && shift > 0n) throw new RangeError("non-minimal varint");
      return result;
    }
    shift += 7n;
    if (shift > 700n) throw new RangeError("varint too long");
  }
}

function readLength(r: Reader): number {
  const n = readVarint(r);
  if (n > BigInt(r.bytes.length - r.at)) throw new RangeError("length exceeds input");
  return Number(n);
}

function readString(r: Reader): { text: string; raw: Uint8Array } {
  if (r.bytes[r.at++] !== 0x04) throw new RangeError("expected a string");
  const n = readLength(r);
  const raw = r.bytes.slice(r.at, r.at + n);
  r.at += n;
  return { text: dec.decode(raw), raw };
}

function decodeValue(r: Reader, depth: number): CanonicalValue {
  if (depth > MAX_DEPTH) throw new RangeError("value is nested too deeply");
  if (r.at >= r.bytes.length) throw new RangeError("truncated value");
  const tag = r.bytes[r.at]!;
  switch (tag) {
    case 0x00:
      r.at++;
      return null;
    case 0x01:
      r.at++;
      return false;
    case 0x02:
      r.at++;
      return true;
    case 0x03: {
      r.at++;
      const n = unzigzag(readVarint(r));
      return n >= BigInt(Number.MIN_SAFE_INTEGER) && n <= BigInt(Number.MAX_SAFE_INTEGER)
        ? Number(n)
        : n;
    }
    case 0x04:
      return readString(r).text;
    case 0x05: {
      r.at++;
      const n = readLength(r);
      const out = r.bytes.slice(r.at, r.at + n);
      r.at += n;
      return out;
    }
    case 0x06: {
      r.at++;
      const count = readLength(r);
      const items: CanonicalValue[] = [];
      for (let i = 0; i < count; i++) items.push(decodeValue(r, depth + 1));
      return items;
    }
    case 0x07: {
      r.at++;
      const count = readLength(r);
      const out: { [key: string]: CanonicalValue } = Object.create(null);
      let previous: Uint8Array | undefined;
      for (let i = 0; i < count; i++) {
        const key = readString(r);
        if (previous && compareBytes(previous, key.raw) >= 0) {
          throw new RangeError("record keys are not sorted and unique");
        }
        previous = key.raw;
        out[key.text] = decodeValue(r, depth + 1);
      }
      return out;
    }
    default:
      throw new RangeError(`unknown tag 0x${tag.toString(16)}`);
  }
}

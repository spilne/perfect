// Typed response wrapper + pluggable body decoders.
//
// Decoders decouple "what shape is the body?" from "how do we get bytes?".
// Each decoder is a `(Response) => Promise<T>` — trivial to add protobuf,
// msgpack, CBOR, etc.

/** Decode a fetch `Response` body into a typed value. */
export type ResponseDecoder<T> = (response: Response) => Promise<T>;

/**
 * Fetch response with decoded body + metadata headers pre-read.
 *
 * Returned by `getResponse(path, { decoder })` when callers need both the
 * body AND response metadata (status, content-type, etc.).
 */
export interface HttpResponse<T> {
  readonly status: number;
  readonly headers: Headers;
  readonly contentType: string | null;
  readonly contentLength: number | null;
  readonly body: T;
}

/** Returns the raw body stream — no buffering, no copying. */
export const binaryDecoder: ResponseDecoder<ReadableStream<Uint8Array>> = async (response) =>
  response.body!;

/** Reads the full body as a UTF-8 string. */
export const textDecoder: ResponseDecoder<string> = (response) => response.text();

/** Reads the full body as parsed JSON (unknown — validate separately). */
export const jsonDecoder: ResponseDecoder<unknown> = (response) => response.json();

/** Returns the full body as an ArrayBuffer — for binary files. */
export const arrayBufferDecoder: ResponseDecoder<ArrayBuffer> = (response) =>
  response.arrayBuffer();

/** Returns the full body as a Blob. */
export const blobDecoder: ResponseDecoder<Blob> = (response) => response.blob();

/**
 * Generic "safeParse" shape. Zod schemas satisfy this out of the box.
 *
 *     const UserSchema = z.object({ id: z.number() });
 *     httpRequest({ url: "/u", schema: UserSchema })  // UserSchema IS a ResponseParser<User>
 *
 * For other libraries (valibot, arktype, @effect/schema, plain functions),
 * wrap their validate call in `{ safeParse: (d) => ... }`.
 */
export interface ResponseParser<T> {
  safeParse(data: unknown):
    | { readonly success: true; readonly data: T }
    | { readonly success: false; readonly error: unknown };
}

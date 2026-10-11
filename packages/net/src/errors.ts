import { TaggedError } from "@spilne/perfect-core";

/** Opening a connection failed: nothing listening, refused, timed out, a TLS error... */
export class ConnectError extends TaggedError("ConnectError")<{
  /** Where we tried to connect, like "localhost:4000" or "/tmp/app.sock". */
  readonly address: string;
  readonly message: string;
  readonly cause?: unknown;
}>() {}

/** Starting a listener failed, for example because the port is taken. */
export class ListenError extends TaggedError("ListenError")<{
  readonly address: string;
  readonly message: string;
  readonly cause?: unknown;
}>() {}

/**
 * An open connection broke: the socket errored, or a message was sent after
 * the connection closed.
 */
export class ConnectionError extends TaggedError("ConnectionError")<{
  readonly address: string;
  readonly message: string;
  readonly cause?: unknown;
}>() {}

/** A received message couldn't be read: too big, or the codec rejected it. */
export class MessageError extends TaggedError("MessageError")<{
  readonly address: string;
  readonly message: string;
  readonly cause?: unknown;
}>() {}

/** What can go wrong once a connection is open. */
export type NetError = ConnectionError | MessageError;

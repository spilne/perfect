// Brand<T, B> — nominal typing over structural primitives.
//
// Two same-primitive identifiers must not be mutually assignable: a Kafka
// group id is not a topic name, a partition is not an offset. Branding adds
// a phantom marker the compiler checks and the runtime never sees.
//
//   type TopicName = Brand<string, "TopicName">;
//   const TopicName = nominal<TopicName>();
//
//   TopicName("orders")            // TopicName
//   const t: TopicName = "orders"  // ✗ compile error
//   send(groupId, topicName)       // ✗ swapped args are a compile error
//
// Scope discipline: brand CONFUSABLE IDENTIFIERS, not free text. Log
// messages, descriptions, and user-facing prose stay plain strings —
// ceremony there buys nothing.
//
// The phantom marker is a string-literal property key (not a unique
// symbol): an unexported symbol breaks declaration emit for anything whose
// inferred type mentions it (TS4023), and an exported one invites runtime
// imports of a value that doesn't exist.

export type Brand<T, B extends string> = T & { readonly ["~perfect/brand"]: B };

/**
 * The underlying primitive of a branded type. Widens to the primitive
 * rather than inferring through the intersection — a naked `infer` against
 * `T & marker` would just give the branded type back.
 */
export type Unbrand<A> = A extends string
  ? string
  : A extends number
    ? number
    : A extends bigint
      ? bigint
      : A extends boolean
        ? boolean
        : never;

/**
 * Zero-cost nominal constructor — brands the value with no validation.
 * Use at the boundary where a raw value first enters typed code.
 */
export function nominal<A extends Brand<any, string>>(): (value: Unbrand<A>) => A {
  return (value) => value as A;
}

/**
 * Validating constructor — brands only values that pass `validate`;
 * throws `BrandError` otherwise. Use for identifiers with invariants
 * (non-empty names, non-negative offsets).
 */
export function refined<A extends Brand<any, string>>(
  validate: (value: Unbrand<A>) => boolean,
  describe: (value: Unbrand<A>) => string,
): (value: Unbrand<A>) => A {
  return (value) => {
    if (!validate(value)) throw new BrandError(describe(value));
    return value as A;
  };
}

export class BrandError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "BrandError";
  }
}

// TaggedError — sugar for declaring discriminated error types.
//
// Equivalent to writing `{ _tag: "Name", ...props }` literals manually,
// with these benefits:
//   - Constructor that auto-sets _tag
//   - Real Error subclass (gets `name`, `message`, `stack` for free)
//   - `instanceof` works for runtime discrimination
//   - `_tag` is a literal type for `.catchTag` narrowing
//
// The `{ _tag: "X" }` literal pattern still works everywhere — this is
// purely opt-in sugar for codebases (or migration from effect-ts's
// `Data.TaggedError` which uses the same double-call shape).
//
//   class NotFound extends TaggedError("NotFound")<{ id: number }>() {}
//   //                                                              ^^ note the trailing ()
//
//   fail(new NotFound({ id: 42 }));
//   eff.catchTag("NotFound", (e) => succeed(`missing ${e.id}`));

interface TaggedErrorClass<Tag extends string, Props> {
  new (props: Props): TaggedErrorInstance<Tag, Props>;
  readonly _tag: Tag;
}

type TaggedErrorInstance<Tag extends string, Props> = Error & Props & { readonly _tag: Tag };

export function TaggedError<Tag extends string>(tag: Tag) {
  return <Props extends object = {}>(): TaggedErrorClass<Tag, Props> => {
    const Cls = class extends Error {
      static readonly _tag = tag;
      readonly _tag = tag;
      constructor(props: Props) {
        super(`${tag}: ${safeStringify(props)}`);
        Object.assign(this, props);
        this.name = tag;
        // Maintain prototype chain through transpilation
        Object.setPrototypeOf(this, new.target.prototype);
      }
    };
    // Brand the class name so devtools / stack traces show `NotFound` not `Cls`
    Object.defineProperty(Cls, "name", { value: tag });
    return Cls as unknown as TaggedErrorClass<Tag, Props>;
  };
}

function safeStringify(props: unknown): string {
  try {
    return JSON.stringify(props);
  } catch {
    return String(props);
  }
}

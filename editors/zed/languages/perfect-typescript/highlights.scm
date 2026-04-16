; Perfect Effect — syntax highlighting queries
; Extends TypeScript's Tree-sitter grammar with Perfect-specific patterns

; ── eff() as a keyword ──────────────────────────────────────────────
((call_expression
  function: (identifier) @keyword.control.effect
  (#eq? @keyword.control.effect "eff")))

; ── $() bind operator ──────────────────────────────────────────────
((call_expression
  function: (identifier) @keyword.operator.bind
  (#eq? @keyword.operator.bind "$")))

; ── Effect constructors ─────────────────────────────────────────────
((call_expression
  function: (identifier) @support.function.effect
  (#match? @support.function.effect "^(succeed|fail|die|sync|suspend|async|tryPromise|sleep|delay|fork|forkDaemon|join|interrupt|awaitFiber|uninterruptible|interruptible|yieldNow|race|raceFirst|raceEither|raceAll|timeout|timeoutFail|timeoutOption|all|provide|ensuring|onExit|acquireRelease|scoped|retry)$")))

; ── Stream constructors ─────────────────────────────────────────────
((member_expression
  object: (identifier) @support.class
  (#match? @support.class "^(Stream|Chunk|Queue|Deferred|Semaphore|Ref|Schedule|WorkerPool|Cause|Exit|Fiber)$")))

; ── Effect type annotations ─────────────────────────────────────────
((type_identifier) @support.type.effect
  (#match? @support.type.effect "^(Eff|Throws|Needs|Stream|Chunk|Pipe|Fiber|Exit|Cause|Scope|Schedule)$"))

; ── Fluent effect methods ───────────────────────────────────────────
((member_expression
  property: (property_identifier) @keyword.operator.effect
  (#match? @keyword.operator.effect "^(flatMap|map|flatten|tap|tapError|tapErrorCause|tapBoth|as|asVoid|zip|zipWith|zipLeft|zipRight|parZip|parZipWith|parMap|parFlatMap|filter|catch|catchTag|catchSome|catchAllCause|orElse|orDie|option|either|mapError|provide|ensuring|onExit|fork|forkDaemon|uninterruptible|interruptible|timeoutFail|timeoutOption|race|delay|when|unless|merge|through|parEvalMap|parEvalMapUnordered|grouped|groupWithin|debounce|throttle|runCollect|runDrain|runForEach|runFold|runHead|runLast|runCount|toArray|concat|interleave)$")))

; ── service() as a special constructor ──────────────────────────────
((call_expression
  function: (identifier) @support.function.service
  (#eq? @support.function.service "service")))

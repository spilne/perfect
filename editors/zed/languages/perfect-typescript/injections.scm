; Inject "perfect_effect" language into for-comprehension blocks
; This allows custom highlighting within for { ... } yield ... constructs
; without the TS parser rejecting the <- syntax

; Note: This is a placeholder. Full for-comprehension support requires
; a custom Tree-sitter grammar that can parse `for { x <- expr } yield expr`.
; For now, the eff($) syntax works natively in TypeScript.
;
; The for-comprehension support will land with the SWC pre-save transformer
; that rewrites `for { <- }` to valid TS before the language server sees it.

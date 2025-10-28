!!!!No fallbacks strict policy!!!: do not invent default values to mask missing data.
!!no try catch except on asynchronous requests!!!
No silent except: catch only expected exceptions, log with context, then re-raise.
No chained defaults in business logic: a or b or c only for UI labels; never for required config/data.
No hidden retries: allowed only if explicitly requested, idempotent, transient errors, bounded attempts, logged.
Fail fast: on invalid input or state — raise; do not continue with partial results.
Observability: include structured logging on failure; do not downgrade severity (no silent warning where error is due).

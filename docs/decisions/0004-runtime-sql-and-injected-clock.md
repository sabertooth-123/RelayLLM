# ADR 0004: Runtime-bound SQL and an injected clock

**Status:** accepted (Phase 1)

## Runtime SQL instead of `sqlx::query!`
The compile-time-checked macros need a live `DATABASE_URL` or a committed
`.sqlx/` offline cache at build time. That adds friction for a new
contributor on Windows. We use `sqlx::query(...).bind(...)` with parameterized
statements (no string interpolation of values) and cover every statement with
integration tests against a real SQLite file.

## Integer-millisecond timestamps from a `Clock` trait
All times are `i64` milliseconds since the Unix epoch, produced by
`relay_core::clock::Clock`. Production uses `SystemClock`; tests use
`ManualClock` and *advance* time. A test can then expire a 30-second lease in
microseconds, with no `sleep` and no flakiness. Integers also compare
correctly inside SQLite. Text dates would need careful formatting to do that.

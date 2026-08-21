# tia converter test fixtures

Synthetic TIA Portal DB source files. Neither describes a real device — each
exists to make specific converter paths fire. Expected offsets are recorded in
an appendix at the bottom of each file, so the fixtures double as golden
references.

| File | Purpose |
|---|---|
| `sample.scl` | Coverage baseline — every supported type, bool bit-packing, string padding, skip types, arrays, nested struct, IEC timer/counters, name sanitizing |
| `udt.scl` | UDT — multiple `TYPE` blocks, nested UDT, definition-before-dependency (two-phase parse), same UDT referenced twice |

Only these two live here. Error cases (optimized access, unknown type, array of
struct, circular UDT) each abort the whole conversion, so they cannot share a
file with the success path — but they're only a handful of lines each, so they
belong inline in table-driven tests rather than as separate fixtures. This
matches `pkg/profileconv/eds`, which keeps one large coverage EDS in `testdata/`
and writes small fixtures as string literals in `_test.go`.

## Offset rules these fixtures encode

S7 non-optimised (standard access) data block layout. The DB source carries no
offsets, so every address is derived by accumulating type sizes:

- every type except `Bool` starts on an even (word) byte
- 1-byte types (`SInt`/`USInt`/`Byte`) need no alignment — odd addresses are fine
- `Bool`s are bit-packed, 8 per byte, LSB first; any non-`Bool` type closes the
  current bool byte before allocating
- `String[n]` occupies `2+n` bytes (2-byte header), rounded up to even
- arrays: `element_size × count`; only the first element is word-aligned
- each bool array starts on a new even byte
- struct members are **each** word-aligned (unlike array elements), and a struct
  closes with word-align padding

Only non-optimised blocks can be derived this way. An optimized-access block
(`S7_Optimized_Access := 'TRUE'`) has a layout chosen internally by TIA Portal
and must be rejected.

## Current behaviour (verified 2026-08-21)

These are regression fixtures — they do **not** pass yet:

| Fixture | Expected | Current |
|---|---|---|
| `sample.scl` | 51 resources, `Int16`-style valueTypes | 63 resources at wrong offsets, lower-case valueTypes |
| `udt.scl` | profile `udt_db`, 2 resources (`lead_in` @0, `tail` @82) | profile `UnknownBlock`, 1 resource `station_id` @0 |

`udt.scl` is the sharpest signal: the parser stops at the first `STRUCT` it
sees, which belongs to a `TYPE` block, so it emits a profile for the wrong
block entirely — silently, with plausible-looking offsets.

## Inline cases to cover in `_test.go`

Each must return `errors.KindContractInvalid`:

- `{ S7_Optimized_Access := 'TRUE' }` — unless `allowOptimized` is set
- unknown type between two `Int`s — asserts the second `Int` is *not* emitted at
  a misaligned offset (currently it is, because the cursor never advances past
  the unknown type)
- `Array[..] of Struct` — currently warns and emits a bogus top-level `x` with
  the prefix lost
- circular UDT reference (`A` → `B` → `A`) — must error, not recurse forever
- a UDT reference with no matching `TYPE` block — the error must name the UDT
  rather than falling through to the generic "unknown type" message

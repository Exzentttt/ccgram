# Implementation and verification

The mobile backend is an adapter around existing terminal sessions. It has no
agent SDK or wrapper runtime.

| Module | Responsibility |
| --- | --- |
| `session_protocol/models.py` | Dataclasses, wire serialization, and protocol errors. |
| `session_protocol/backend.py` | Consumer-owned backend Protocol and its Multiplexer adapter. |
| `session_protocol/service.py` | Session discovery, snapshots, history, live replacements, and delivery correlations. |
| `session_protocol/messages.py` | One provider-neutral message builder for history and events. |
| `session_protocol/actions.py` | Schema boundary checks, persistent deduplication, binding checks, and input execution. |
| `session_protocol/input_lock.py` | Task-reentrant per-target locks shared with Telegram text and interactive input. |
| `session_protocol/requests.py` | Open requests, hook evidence, supported options, and close events. |
| `session_protocol/events.py` | Epochs, durable replay, subscription filtering, and overflow detection. |
| `session_protocol/store.py` | SQLite identity, device, action, source-generation, and delivery metadata. |
| `session_protocol/uploads.py` | Private uploaded files and opaque references. |
| `api/server.py` | Host/Origin checks, bearer auth, rate limits, HTTP routes, and WebSocket lifecycle. |
| `api/runtime.py` | Optional listener, process lock, hook queue, and polling lifecycle. |

`bootstrap.py` owns creation and teardown. The listener starts only when enabled.
The API and core have no imports from Telegram or its handlers. The worktree helpers
now live in `worktrees.py`, where both session creation and the Telegram directory
flow can use them without a handler dependency.

## Compatibility and migration boundary

History uses only the transcript binding already established by ccgram. It does
not guess a conversation from the latest transcript in the same directory. Claude,
Codex, Gemini, and Pi have structured history. Other or unbound transcripts use the
terminal view.

The mobile builder reads native transcript records alongside the existing Telegram
parser. The provider renderers, Telegram message queue, watermark logic, and delivery
receipts remain the existing implementation. Mobile polling uses no Telegram
watermark or acknowledgement. A slow remote client can therefore affect only its
own bounded queue.

The shared lock covers Telegram text dismissal plus sending, shared Telegram
message injection, and interactive callbacks. Existing toolbar, recovery, launch,
and other direct-input paths remain explicit migration exceptions. The architecture
fitness test records that baseline and rejects newly introduced direct handler send
sites. Three P0 refactors remain: route all Telegram input through `ActionExecutor`,
render Telegram from structured parts, and move its receipts into `EventHub`. They are not prerequisites for enabling
this separate mobile backend, and this change does not claim the full P0 exit gate.

## Contract corrections

The implementation makes these clarifications to the proposed design:

- Persistent state uses `protocol.sqlite3`, separate from delayed `state.json` saves.
  Full commits reserve an action before any input and preserve an unknown outcome
  after a crash. Device and action storage use the Python standard library.
- `bindingVersion` remains a persisted increasing integer. The active pane,
  foreground process, and transcript generation also participate in the binding.
- Message revision counts repeated native records and result records. Counting
  only tool results misses Gemini's repeated message updates.
- `historyGeneration` marks source replacement. Clients clear the old conversation
  cache when it changes. A restart retains the generation.
- A resume at the current sequence succeeds even with an empty ring. `welcome`
  precedes replay. Subscription queues are installed before reading replay.
- Cursors use opaque conversation/generation/message-boundary tokens. The server
  reads and pairs the source transcript before paging. It has no persistent message
  index or message database.
- Hook events retain a tool-input digest instead of raw input. Matching reads the
  authoritative input from the transcript. Hooks remain observations and return no
  permission decision.

The current history builder reads the transcript before paging. Large conversations
therefore cost a full parse when content changes or history is requested. Add an
incremental source index if profiling shows that cost dominates API response time.
The single builder and identity rules must remain unchanged.

## Tests

Run the focused conformance and integration checks:

```console
uv run pytest tests/ccgram/session_protocol -q
uv run pytest tests/integration/test_session_protocol.py -q
```

The unit suite covers message identity, revisions, source replacement, pagination,
and partial JSONL writes. Action tests cover crash reservations, retries, binding
changes, request safety, and uncertain delivery. API tests cover uploads, pairing,
revocation, Host/Origin guards, rate limits, replay, queue overflow, and schemas.

Golden payloads live in `protocol/v1/fixtures/`. `_manifest.json` maps each fixture
to its shared JSON Schema. The server's builder output is compared with the golden
message fixture. The iOS package can decode the same payload files. Update schemas
and affected fixtures together.

The integration test starts an isolated tmux server and a deterministic terminal
program, then uses real capture/input through the Multiplexer adapter and real
HTTP/WebSocket connections. It checks list, history, send, verified hook response,
resume, and durable replay. It does not call an LLM or require a paid agent account.
It skips only when tmux is unavailable.

Run `make check` for formatting, lint, types, dependency checks, unit tests, and
integration tests. APNs and the iOS UI require their own later implementation and
verification.

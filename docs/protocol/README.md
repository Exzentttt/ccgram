# ccgram/1 mobile protocol

The optional mobile API exposes the terminal sessions already running on the Mac.
Telegram keeps its existing transcript renderer, queue, and delivery receipts.
The API uses device tokens independent of the Telegram bot token.

For operators: [setup and pairing](operations.md).
For client developers: [HTTP, events, and recovery](client-guide.md).
For contributors: [implementation and tests](implementation.md).

The wire contract is in [`protocol/v1/`](../../protocol/v1/README.md): OpenAPI 3.1,
AsyncAPI 3, shared JSON Schemas, and golden fixtures. It defines protocol version
`ccgram/1`. The [architecture design](../architecture-design/2026-10-20-session-protocol.md)
explains the requirements and the longer migration plan.

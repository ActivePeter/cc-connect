# Messages during a running task

For Claude Code sessions, messages accepted by the platform while a task is
running now use the same mid-turn input as `cc-connect inject`. In a group that
requires mentions, users still need to @ the bot. Existing command, permission,
rate-limit, workspace, and stale-message checks run before this delivery path.

The bot acknowledges a successful write with “Message added to the running
task.” Claude Code incorporates the input at its next tool boundary. This does
not cancel the task, create another foreground turn, or promise immediate
interruption of an already running tool. The combined response remains attached
to the original task; each follow-up is saved in conversation history.

Quotes, sender context, images, files, and platform message IDs are preserved.
Only agent sessions implementing `MidTurnMessageSender` opt in; Claude Code is
the only production adapter that currently implements it. Other backends retain
their existing queue behavior.

Messages also queue if the first prompt has not been written, the turn has
already ended, an earlier message is queued, or a mid-turn write fails. This
preserves the existing delivery fallback and ordering. A per-turn permission
mode override also follows the normal queue path. The acknowledgement means the
input was written to the agent, not that the model has already acted on it.

## Validation

- `go test ./...`
- `go test ./core -run TestCUJ`
- `go build ./...`
- `go test -race ./core ./agent/claudecode -run 'Test(Inject|HandleMessage_BusyInterjection|HandleMessage_Interjection|CUJ_N2|QueueMessage|ProcessInteractiveEvents_Drains)'`

The new CUJ starts a task, inserts two corrections, supplies only one terminal
result, and verifies that the next normal request completes. Reverting only the
user-message routing to its former queue-only path makes both new regression
scenarios fail. Build the existing web assets before the full suite, as required
by `web/embed.go`.

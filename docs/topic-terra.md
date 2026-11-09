# Terra topic sessions

This integrates topic1 (`9fdbdd99`) with the complete goal3-steer3 history
(`e70a82e1`). Goal mode remains opt-in; this does not turn `goal_mode=always` on.

Add these options to each executor's existing Feishu platform options:

```toml
thread_isolation_mode = "topic_only"
topic_concurrency = 4
```

Keep `thread_isolation` false/unset and retain the current app-server/stdio and
mainline sharing configuration. A commander retains its existing session keys.

A topic keeps its admission slot while its foreground session is busy, a native
turn is running, its native goal is active, or deferred input is queued/in flight.
Platform delivery and closing receipts also keep an existing slot until the
corresponding operation completes. Same-topic input continues through steer/FIFO.
Native state callbacks wake the engine; no periodic polling is used.

There is one explicit exception: an otherwise idle native goal releases its slot
after 60 seconds without a new turn. This uses the same timeout constant as the
native deferred-input watchdog. It does not clear or pause that goal. If the
native process later starts autonomous work, the engine accounts for that work
again before admitting more topics. The timeout does not give cc-connect control
over when an already running native process starts its own future rounds.

Non-model control commands bypass topic data admission. Compression, unknown
slash commands/skills, shell commands and scheduling new work retain admission.
`/goal status` is a topic-only alias for `/goal`; `/goal clear` invalidates pending
objectives in that topic's admission inbox as well as the existing native queue.
Messages with attachments are not mistaken for plain control commands.

`cc-connect session new` rejects active goals or deferred input before recording
an operation. It rechecks at finalization: input accepted after registration is
preserved on the source session rather than discarded by closing that process.
A short per-session gate prevents native writes racing with finalization; slow
startup, close and platform delivery do not hold that gate. Ordinary queued
input migrates through the existing rotation transaction and handoff outbox.
Rotation keeps its topic reservation through candidate publication and handoff.

The test boundaries use fake app-server JSON-RPC and fake platform delivery;
no live CLI, account, model or Feishu call is required. The regression suite
includes automatic native continuation after the foreground lock is released,
steer isolation, queued control commands, the idle timer, deferred in-flight
ownership, and rotation's registration/finalization boundaries.

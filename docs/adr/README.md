# Architecture decision records

Each record says what was decided about the service, why, and what it changed. A record is
never rewritten after it is accepted. A later decision **amends** it, or **supersedes** it as
a whole, and both headers say so.

These four record decisions taken while the service was built; they were written down on
2026-10-06 from the code, the commits and the pages that held them until then. The records
that travel on the wire (`RunStart`, `Interrupt`, `ScheduleSpec`, ...) have their decisions
in [agent-contracts' ADRs](https://github.com/amitmohapatra/agent-contracts/blob/main/docs/adr/README.md).

| ADR | Decision | Since | Status | Superseded by |
|---|---|---|---|---|
| [0001](0001-leases-and-fencing.md) | Workers hold runs under a lease, and every write is fenced to the holder | 0.2.0 | accepted | — |
| [0002](0002-the-ticker-owns-time.md) | The ticker owns time | 0.2.0 | accepted | — |
| [0003](0003-webhooks-live-here.md) | Notifications are webhooks sent from here, not from the harness | 0.2.0 | accepted | — |
| [0004](0004-schedules-merged-in.md) | Schedules live in agent-runs, and a fire is a queued run | 0.2.0 | accepted | — |

None is superseded. What each version changed: [CHANGELOG.md](../../CHANGELOG.md).

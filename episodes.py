"""The alarm-episode state machine — Phase 3, Session 2.

Folds a firehose of violations into INCIDENTS with lifecycles:

    (nothing) --warn--> open --critical--> escalated --quiet--> cleared
    (nothing) --critical--> open(critical) ----------quiet--> cleared

Three design positions, each earning its keep:

1. THE DATABASE IS THE ARBITER. Workers keep no authoritative state.
   Every transition is ONE atomic upsert, made race-proof by a partial
   unique index (migration 0005): at most one non-cleared episode per
   sensor. Two workers processing the same sensor concurrently cannot
   double-open; the loser's INSERT becomes an UPDATE. This is also what
   makes worker restarts free — there is no state to rebuild.

2. IDEMPOTENT WHERE IT MATTERS, HONEST WHERE IT ISN'T. Redelivered
   violations (at-least-once, remember) re-apply harmlessly to state,
   severity, peak_value, and timestamps — GREATEST() and CASE make the
   transition functions idempotent. n_readings, however, will overcount
   on redelivery. That is a documented trade: it is an advisory
   intensity signal, not an audit figure. The audit figures (state
   transitions, peaks, times) are exactly-once by construction.

3. ALARMS BUY FULL DURABILITY. The state node runs
   synchronous_commit=off globally (bulk telemetry tolerates <1s loss).
   Episode writes override it per-transaction with SET LOCAL — the
   Phase 1 README made this promise; this file keeps it. An episode
   acked to Redis is on disk in the WAL, fsync'd, before we XACK.

Clearing is inferred, not reported: the gateway only emits violations,
so "the danger passed" is the ABSENCE of violations. A sweeper clears
episodes quiet for HAZ_CLEAR_AFTER_S (event-time last_seen vs now).
That interval is the debounce: a sensor oscillating across its
threshold updates one episode instead of strobing open/clear pairs.
"""
import json
import os
from datetime import datetime, timezone


def parse_ts(ts: str) -> datetime:
    """Violation timestamps arrive as ISO-8601 strings ('...Z'); asyncpg
    is strictly typed and wants a real datetime for timestamptz params.
    fromisoformat handles 'Z' natively on Python 3.11+."""
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))

# One statement, both transitions. The ON CONFLICT target is the partial
# unique index; xmax=0 distinguishes fresh INSERT from UPDATE so the
# caller can log "opened" vs "updated" without a second query.
UPSERT = """
INSERT INTO alarm_events (sensor_id, zone_id, severity, state,
                          opened_at, last_seen, peak_value)
VALUES ($1, $2, $3::alarm_severity, 'open', $4, $4, $5)
ON CONFLICT (sensor_id) WHERE state <> 'cleared'
DO UPDATE SET
    last_seen  = GREATEST(alarm_events.last_seen, EXCLUDED.last_seen),
    peak_value = GREATEST(alarm_events.peak_value, EXCLUDED.peak_value),
    n_readings = alarm_events.n_readings + 1,
    severity   = CASE WHEN EXCLUDED.severity = 'critical'
                      THEN 'critical' ELSE alarm_events.severity END,
    state      = CASE WHEN EXCLUDED.severity = 'critical'
                       AND alarm_events.severity = 'warn'
                      THEN 'escalated' ELSE alarm_events.state END
RETURNING alarm_id, state, severity, n_readings, (xmax = 0) AS opened
"""

SWEEP = """
UPDATE alarm_events
SET state = 'cleared', cleared_at = now()
WHERE state <> 'cleared' AND last_seen < now() - ($1 * interval '1 second')
RETURNING alarm_id, sensor_id, severity, peak_value, n_readings
"""


class Episodes:
    def __init__(self, pool, log, f):
        self.pool = pool
        self.log = log
        self.f = f
        self.clear_after = int(os.environ.get("HAZ_CLEAR_AFTER_S", "60"))
        # Log-dedup cache ONLY (correctness lives in the DB): remembers
        # each sensor's last state so we log "escalated" once, not on
        # every critical hit. After a restart, at most one duplicate
        # log line per episode. Acceptable.
        self._last_state = {}

    async def apply(self, v: dict):
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                # The Phase 1 promise: alarms are durable even though
                # the cluster default is synchronous_commit=off.
                await conn.execute("SET LOCAL synchronous_commit = on")
                row = await conn.fetchrow(
                    UPSERT, v["sensor_id"], v["zone_id"], v["severity"],
                    parse_ts(v["ts"]), float(v["value"]))

        state, prev = row["state"], self._last_state.get(v["sensor_id"])
        self._last_state[v["sensor_id"]] = state
        if row["opened"]:
            self.log.info("episode opened", extra=self.f(
                alarm=str(row["alarm_id"])[:8], sensor=v["sensor_id"][:8],
                severity=row["severity"]))
        elif state == "escalated" and prev != "escalated":
            self.log.info("episode ESCALATED", extra=self.f(
                alarm=str(row["alarm_id"])[:8], sensor=v["sensor_id"][:8],
                peak=float(v["value"])))

    async def sweep(self):
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute("SET LOCAL synchronous_commit = on")
                rows = await conn.fetch(SWEEP, self.clear_after)
        for r in rows:
            self._last_state.pop(str(r["sensor_id"]), None)
            self.log.info("episode cleared", extra=self.f(
                alarm=str(r["alarm_id"])[:8], sensor=str(r["sensor_id"])[:8],
                severity=r["severity"], peak=r["peak_value"],
                n_readings=r["n_readings"]))
        return len(rows)

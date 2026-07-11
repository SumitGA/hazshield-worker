"""The hazard context builder — Phase 3, Session 3.

Turns an alarm episode into the document a planner (human or LLM)
needs: WHAT is happening, WHERE it is, what the WHERE connects to, and
what levers exist to isolate it. Three positions:

1. DETERMINISM IS THE CACHE KEY. The rendered context is byte-stable:
   fixed field order, sorted neighbors and assets, rounded numbers.
   sha256 of the text becomes prompt_hash — identical hazards (same
   sensor kind, zone, severity, topology) hash identically, so the
   Session 4 generator can reuse a cached plan instead of burning an
   Ollama slot. One flapping sensor = one generation, not fifty.
   Corollary: the LIVE value is NOT in the hashed document (it would
   bust the cache every reading); severity + peak BAND carry intensity.

2. THE GRAPH IS THE POINT. An isolation plan that only knows the
   burning zone is a plan to trap people. Adjacency edges carry their
   kind — a 'duct' neighbor means airflow coupling (gas travels), a
   'conveyor' neighbor means material feed (stopping upstream matters),
   a 'passage' neighbor is an evacuation route. The planner gets all
   three, labeled.

3. PLANS ARE REQUESTED IDEMPOTENTLY. One pending/ready plan per
   (alarm, prompt_hash): redelivered violations or escalation retries
   do not enqueue duplicate work. The dedupe is a WHERE NOT EXISTS in
   the insert — the database arbitrates, as always.
"""
import hashlib
import json

CONTEXT_SQL = """
SELECT s.kind AS sensor_kind, s.unit, s.warn_threshold, s.crit_threshold,
       z.zone_id, z.name AS zone_name, z.kind AS zone_kind,
       a.severity::text, a.state::text, a.peak_value, a.opened_at
FROM alarm_events a
JOIN sensor s ON s.sensor_id = a.sensor_id
JOIN zone z   ON z.zone_id = a.zone_id
WHERE a.alarm_id = $1
"""

NEIGHBORS_SQL = """
SELECT zn.name, zn.kind, za.link_kind
FROM zone_adjacency za JOIN zone zn ON zn.zone_id = za.adjacent_id
WHERE za.zone_id = $1
ORDER BY za.link_kind, zn.name
"""

ASSETS_SQL = """
SELECT name, kind, isolation_actions
FROM asset WHERE zone_id = $1 ORDER BY name
"""

# Dedupe + enqueue in one statement: a plan is requested at most once
# per (alarm, identical-context). 'unassigned' model is claimed by the
# Session 4 generator when it picks the row up.
ENQUEUE_SQL = """
INSERT INTO isolation_plans (alarm_id, model, prompt_hash, status)
SELECT $1, 'unassigned', $2, 'pending'
WHERE NOT EXISTS (
    SELECT 1 FROM isolation_plans
    WHERE alarm_id = $1 AND prompt_hash = $2
      AND status IN ('pending','generating','ready')
)
RETURNING plan_id
"""


def peak_band(peak: float, warn: float, crit: float) -> str:
    """Intensity as a band, not a number — cache-stable, still useful.
    Handles low-alarm sensors (crit < warn) by working in 'badness'
    distance past the critical line."""
    if crit >= warn:  # high-alarm
        if peak < crit: return "above warn"
        ratio = peak / crit if crit else 1.0
    else:             # low-alarm: smaller is worse
        if peak > crit: return "below warn"
        ratio = crit / peak if peak else 2.0
    if ratio >= 2.0: return "extreme (>=2x critical)"
    if ratio >= 1.5: return ">=1.5x critical"
    return "past critical"


def render(ctx: dict) -> str:
    """The document itself. Byte-stable: never reorder, never add
    timestamps or live values. Change it only with the understanding
    that every cached plan invalidates."""
    L = [
        "INDUSTRIAL HAZARD CONTEXT",
        f"alarm: {ctx['sensor_kind']} sensor, severity {ctx['severity']}, "
        f"intensity {ctx['band']}",
        f"zone: {ctx['zone_name']} ({ctx['zone_kind']})",
        "adjacent zones:",
    ]
    for n in ctx["neighbors"]:
        L.append(f"  - {n['name']} ({n['kind']}) via {n['link_kind']}")
    L.append("assets in affected zone:")
    for a in ctx["assets"]:
        acts = "; ".join(f"step {s['step']}: {s['action']}"
                         for s in a["isolation_actions"])
        L.append(f"  - {a['name']} [{a['kind']}]: {acts}")
    return "\n".join(L)


class ContextBuilder:
    def __init__(self, pool, log, f):
        self.pool, self.log, self.f = pool, log, f

    async def build(self, alarm_id) -> tuple[str, str]:
        """-> (rendered document, prompt_hash)"""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(CONTEXT_SQL, alarm_id)
            neighbors = await conn.fetch(NEIGHBORS_SQL, row["zone_id"])
            assets = await conn.fetch(ASSETS_SQL, row["zone_id"])
        ctx = {
            "sensor_kind": row["sensor_kind"],
            "severity": row["severity"],
            "band": peak_band(row["peak_value"], row["warn_threshold"],
                              row["crit_threshold"]),
            "zone_name": row["zone_name"], "zone_kind": row["zone_kind"],
            "neighbors": [dict(n) for n in neighbors],
            "assets": [{"name": a["name"], "kind": a["kind"],
                        "isolation_actions": json.loads(a["isolation_actions"])}
                       for a in assets],
        }
        doc = render(ctx)
        return doc, hashlib.sha256(doc.encode()).hexdigest()[:16]

    async def request_plan(self, alarm_id) -> bool:
        """Build context, enqueue a pending plan unless an equivalent
        one already exists. Returns True if newly enqueued."""
        doc, phash = await self.build(alarm_id)
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute("SET LOCAL synchronous_commit = on")
                row = await conn.fetchrow(ENQUEUE_SQL, alarm_id, phash)
        if row:
            self.log.info("isolation plan requested", extra=self.f(
                alarm=str(alarm_id)[:8], prompt_hash=phash,
                context_lines=doc.count("\n") + 1))
            return True
        self.log.info("plan request deduped (identical context pending/ready)",
                      extra=self.f(alarm=str(alarm_id)[:8], prompt_hash=phash))
        return False

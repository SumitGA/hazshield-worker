"""The isolation-plan generator — Phase 3, Session 4.

Consumes 'pending' rows from isolation_plans and produces plans, via
(in preference order): the cache, the LLM, or the rulebook. Positions:

1. CLAIM VIA SKIP LOCKED. Multiple workers may run generators; a plan
   is claimed by an atomic UPDATE over FOR UPDATE SKIP LOCKED, so two
   generators never fight over one row. The database arbitrates,
   as always. A crashed generator's 'generating' rows are reaped back
   to 'pending' by the staleness sweep below.

2. THE CACHE IS THE FIRST MODEL. Before any LLM call: is there a
   'ready' plan with the same prompt_hash? Copy it (model='cache',
   latency 0). Identical hazards cost one generation, ever. On a 10GB
   VM sharing space with the model weights this is not an optimization,
   it is the budget.

3. SEMAPHORE(1) AROUND OLLAMA. One generation at a time. The model IS
   the bottleneck; queueing in our process (visible, bounded,
   cancellable) beats queueing inside Ollama (opaque, memory-hungry).

4. THE BREAKER SERVES THE RULEBOOK. After consecutive failures the
   circuit opens and pending plans get a deterministic rule-based
   fallback built from the SAME hazard context: stop every asset in
   the zone (their own isolation_actions), seal duct-linked neighbors,
   halt conveyor-linked feeds, evacuate via passages. Status
   'fallback', clearly labeled. The safety system must never return
   nothing — a mediocre plan NOW beats a brilliant plan that never
   arrives.
"""
import asyncio
import json
import os
import time

import aiohttp

CLAIM_SQL = """
UPDATE isolation_plans
SET status = 'generating', model = $1
WHERE plan_id = (
    SELECT plan_id FROM isolation_plans
    WHERE status = 'pending'
    ORDER BY created_at
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
RETURNING plan_id, alarm_id, prompt_hash
"""

CACHE_SQL = """
SELECT plan FROM isolation_plans
WHERE prompt_hash = $1 AND status = 'ready' AND plan IS NOT NULL
ORDER BY created_at DESC LIMIT 1
"""

FINISH_SQL = """
UPDATE isolation_plans
SET status = $2::plan_status, plan = $3, latency_ms = $4, model = $5
WHERE plan_id = $1
"""

# Generator crashed mid-generation? Its rows sit in 'generating'
# forever. Reap them back to pending after a staleness window —
# the plan-table twin of XAUTOCLAIM.
REAP_SQL = """
UPDATE isolation_plans SET status = 'pending'
WHERE status = 'generating'
  AND created_at < now() - interval '5 minutes'
RETURNING plan_id
"""

PROMPT_TEMPLATE = """You are an industrial safety planner. Given the hazard context below, produce an isolation plan as JSON ONLY (no prose, no markdown fences) with this exact shape:
{{"summary": "<one sentence>", "steps": [{{"order": 1, "action": "<imperative action>", "target": "<asset or zone>", "reason": "<why>"}}], "evacuation_route": "<via which adjacent zone>", "ventilation_note": "<duct-linked zones handling>"}}

{context}

JSON:"""


def fallback_plan(ctx_doc: str) -> dict:
    """Deterministic rulebook from the rendered context. Parses the
    same document the LLM would see — one source of truth."""
    steps, order = [], 1
    evac, ducts = None, []
    for line in ctx_doc.splitlines():
        line = line.strip()
        if line.startswith("- ") and " via duct" in line:
            ducts.append(line[2:].split(" (")[0])
        elif line.startswith("- ") and " via passage" in line and evac is None:
            evac = line[2:].split(" (")[0]
        elif line.startswith("- ") and " via conveyor" in line:
            steps.append({"order": 0, "action": "halt inbound conveyor feed",
                          "target": line[2:].split(" (")[0],
                          "reason": "stop material transport into hazard zone"})
        elif line.startswith("- ") and "]:" in line:
            name = line[2:].split(" [")[0]
            steps.append({"order": 0, "action": f"stop and lockout {name}",
                          "target": name,
                          "reason": "de-energize all equipment in hazard zone"})
    for d in ducts:
        steps.append({"order": 0, "action": "close ventilation dampers",
                      "target": d, "reason": "duct-linked: prevent gas migration"})
    for i, s in enumerate(steps, 1):
        s["order"] = i
    return {
        "summary": "Rule-based fallback: de-energize zone, seal duct links, "
                   "evacuate via passage.",
        "steps": steps,
        "evacuation_route": evac or "nearest passage",
        "ventilation_note": f"dampers closed toward: {', '.join(ducts) or 'none'}",
    }


class Generator:
    def __init__(self, pool, context_builder, log, f):
        self.pool, self.context, self.log, self.f = pool, context_builder, log, f
        self.ollama_url = os.environ.get("HAZ_OLLAMA_URL", "http://127.0.0.1:11434")
        self.model = os.environ.get("HAZ_MODEL", "phi3:mini")
        self.timeout = int(os.environ.get("HAZ_GEN_TIMEOUT_S", "120"))
        self.sem = asyncio.Semaphore(1)          # position 3
        # circuit breaker: closed(0 fails) -> open after 3 -> retry after 60s
        self.fail_count = 0
        self.breaker_open_until = 0.0
        self.breaker_threshold = int(os.environ.get("HAZ_BREAKER_FAILS", "3"))
        self.breaker_cooldown = int(os.environ.get("HAZ_BREAKER_COOLDOWN_S", "60"))

    async def loop(self, stop_event):
        while not stop_event.is_set():
            try:
                reaped = await self.pool.fetch(REAP_SQL)
                if reaped:
                    self.log.warning("reaped stale generating plans",
                                     extra=self.f(count=len(reaped)))
                worked = await self.tick()
            except Exception as e:
                self.log.warning("generator tick failed", extra=self.f(error=str(e)))
                worked = False
            if not worked:
                await asyncio.sleep(2)

    async def tick(self) -> bool:
        row = await self.pool.fetchrow(CLAIM_SQL, self.model)
        if not row:
            return False
        plan_id, alarm_id, phash = row["plan_id"], row["alarm_id"], row["prompt_hash"]

        # position 2: cache first
        cached = await self.pool.fetchval(CACHE_SQL, phash)
        if cached:
            await self.pool.execute(FINISH_SQL, plan_id, "ready", cached, 0, "cache")
            self.log.info("plan served from cache", extra=self.f(
                plan=str(plan_id)[:8], prompt_hash=phash))
            return True

        ctx_doc, _ = await self.context.build(alarm_id)

        # position 4: breaker open -> rulebook immediately
        if time.monotonic() < self.breaker_open_until:
            await self.finish_fallback(plan_id, ctx_doc, "breaker open")
            return True

        t0 = time.monotonic()
        try:
            async with self.sem:                  # position 3
                plan = await self.call_ollama(ctx_doc)
            latency = int((time.monotonic() - t0) * 1000)
            await self.pool.execute(FINISH_SQL, plan_id, "ready",
                                    json.dumps(plan), latency, self.model)
            self.fail_count = 0
            self.log.info("plan generated", extra=self.f(
                plan=str(plan_id)[:8], model=self.model, latency_ms=latency,
                steps=len(plan.get("steps", []))))
        except Exception as e:
            self.fail_count += 1
            self.log.error("generation failed", extra=self.f(
                plan=str(plan_id)[:8], error=str(e)[:200],
                consecutive=self.fail_count))
            if self.fail_count >= self.breaker_threshold:
                self.breaker_open_until = time.monotonic() + self.breaker_cooldown
                self.log.error("CIRCUIT BREAKER OPEN", extra=self.f(
                    cooldown_s=self.breaker_cooldown))
            await self.finish_fallback(plan_id, ctx_doc, str(e)[:100])
        return True

    async def finish_fallback(self, plan_id, ctx_doc, reason):
        plan = fallback_plan(ctx_doc)
        plan["fallback_reason"] = reason
        await self.pool.execute(FINISH_SQL, plan_id, "fallback",
                                json.dumps(plan), 0, "rulebook")
        self.log.warning("plan served from RULEBOOK", extra=self.f(
            plan=str(plan_id)[:8], reason=reason))

    async def call_ollama(self, ctx_doc: str) -> dict:
        prompt = PROMPT_TEMPLATE.format(context=ctx_doc)
        async with aiohttp.ClientSession() as s:
            async with s.post(f"{self.ollama_url}/api/generate",
                              json={"model": self.model, "prompt": prompt,
                                    "stream": False,
                                    "options": {"temperature": 0.2}},
                              timeout=aiohttp.ClientTimeout(total=self.timeout)) as r:
                r.raise_for_status()
                body = await r.json()
        text = body.get("response", "").strip()
        # models love to wrap JSON in fences despite instructions
        if text.startswith("```"):
            text = text.strip("`").lstrip("json").strip()
        plan = json.loads(text)
        if "steps" not in plan or not isinstance(plan["steps"], list):
            raise ValueError("plan missing steps[]")
        return plan

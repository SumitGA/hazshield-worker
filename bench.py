#!/usr/bin/env python3
"""The bake-off harness — Phase 3, Session 5.

Runs the SAME hazard context through multiple Ollama models, N trials
each, and scores what actually matters for this system:

  latency       p50/max wall time per generation (the alarm is waiting)
  json_valid    did constrained decoding + our schema check pass?
  steps         plan length (proxy for coverage; rulebook = 12 here)
  hazard_named  does the plan text mention the actual hazard (methane/
                gas)? phi3's first plan said "overheating" — this metric
                exists because of that.

Usage:
  ./bench.py --models phi3:mini,llama3:8b --trials 3
Writes bench_results.jsonl; prints a comparison table.
"""
import argparse, asyncio, json, os, statistics, sys, time

import aiohttp, asyncpg

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from context import ContextBuilder          # same doc the prod path uses
from generator import PROMPT_TEMPLATE


async def one_trial(session, url, model, prompt, timeout):
    t0 = time.monotonic()
    try:
        async with session.post(f"{url}/api/generate",
                                json={"model": model, "prompt": prompt,
                                      "stream": False, "format": "json",
                                      "options": {"temperature": 0.2}},
                                timeout=aiohttp.ClientTimeout(total=timeout)) as r:
            r.raise_for_status()
            body = await r.json()
        text = body.get("response", "")
        latency = time.monotonic() - t0
        try:
            plan = json.loads(text)
            steps = len(plan.get("steps", []))
            blob = json.dumps(plan).lower()
            hazard = any(w in blob for w in ("gas", "methane", "ch4", "leak"))
            return {"ok": True, "latency_s": round(latency, 1), "steps": steps,
                    "hazard_named": hazard, "plan": plan}
        except (json.JSONDecodeError, AttributeError) as e:
            return {"ok": False, "latency_s": round(latency, 1),
                    "error": f"invalid json: {e}"}
    except Exception as e:
        return {"ok": False, "latency_s": round(time.monotonic() - t0, 1),
                "error": f"{type(e).__name__}: {str(e)[:100]}"}


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--models", default="phi3:mini")
    ap.add_argument("--trials", type=int, default=3)
    ap.add_argument("--timeout", type=int, default=600)
    ap.add_argument("--pg", default=os.environ.get(
        "HAZ_DATABASE_URL", "postgresql://hazshield:devpw@127.0.0.1/hazshield"))
    ap.add_argument("--ollama", default=os.environ.get(
        "HAZ_OLLAMA_URL", "http://127.0.0.1:11434"))
    args = ap.parse_args()

    pool = await asyncpg.create_pool(args.pg, min_size=1, max_size=2)
    alarm_id = await pool.fetchval(
        "SELECT alarm_id FROM alarm_events ORDER BY opened_at DESC LIMIT 1")
    if not alarm_id:
        sys.exit("no alarm episodes in DB — blast one first")
    import logging
    doc, phash = await ContextBuilder(pool, logging.getLogger(),
                                      lambda **k: {}).build(alarm_id)
    prompt = PROMPT_TEMPLATE.format(context=doc)
    print(f"benchmark context: alarm {str(alarm_id)[:8]}, hash {phash}\n{'='*64}")

    results = {}
    async with aiohttp.ClientSession() as session:
        for model in args.models.split(","):
            model = model.strip()
            trials = []
            for i in range(args.trials):
                r = await one_trial(session, args.ollama, model, prompt,
                                    args.timeout)
                trials.append(r)
                status = (f"ok {r['latency_s']}s steps={r['steps']} "
                          f"hazard={r['hazard_named']}") if r["ok"] \
                         else f"FAIL {r.get('error')}"
                print(f"  {model} trial {i+1}: {status}")
            results[model] = trials
            with open("bench_results.jsonl", "a") as fh:
                for t in trials:
                    fh.write(json.dumps({"model": model, **{
                        k: v for k, v in t.items() if k != "plan"}}) + "\n")

    print(f"\n{'model':<14}{'valid':<8}{'p50 s':<8}{'max s':<8}"
          f"{'steps':<8}{'hazard named'}")
    for m, ts in results.items():
        ok = [t for t in ts if t["ok"]]
        lat = sorted(t["latency_s"] for t in ok) or [0]
        print(f"{m:<14}{len(ok)}/{len(ts):<6}"
              f"{lat[len(lat)//2]:<8}{max(lat):<8}"
              f"{round(statistics.mean(t['steps'] for t in ok), 1) if ok else '-':<8}"
              f"{sum(t.get('hazard_named', False) for t in ok)}/{len(ok)}")
    await pool.close()

if __name__ == "__main__":
    asyncio.run(main())

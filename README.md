# Hazsheild-Worker End-to-end demo: plume -> episodes -> plans

Phase 2 demo proved nothing gets lost. This one proves the
system THINKS: a gas plume becomes a handful of episodes becomes
machine-written isolation plans, unattended.

## Pre-flight (compute VM)
    ollama list                       # both models pulled for the bake-off
    ollama pull llama3:8b             # ~4.7GB; phi3 unloads when idle
    systemctl is-active ollama hazshield-worker@1

## The bake-off (compute VM, before the demo — needs a recent episode)
    cd /opt/hazshield-workers
    HAZ_DATABASE_URL=... ./venv/bin/python3 bench.py \
        --models phi3:mini,llama3:8b --trials 3
    # judge: valid rate, p50 latency, steps coverage, hazard_named.
    # Set the winner via drop-in: Environment=HAZ_MODEL=<winner>

## The demo (edge VM)
    # baselines:
    X=$(redis-cli -h 10.10.0.12 XLEN hazshield:violations)
    psql "$PG" -c "SELECT count(*) FROM alarm_events; SELECT count(*) FROM isolation_plans;"
    # ignite (sim.py from Phase 2 — 3 min, plume only, gentler rate):
    ./sim.py --pg "$PG" --rate 2000 --duration 180 --scenario plume

## The audit (predict, then look)
    1. Stream grew by (warn+crit) from sim's FINAL line — Phase 2 invariant.
    2. alarm_events: ~34 new episodes (one per plume CH4 sensor), most
       escalated, peaks near 1.5x crit. 4,000+ violations -> ~34 incidents.
    3. isolation_plans: FEWER plans than episodes — same zone => same
       context => same hash => cache hits. Expect a handful of real
       generations, the rest 'cache' at 0ms. That ratio is the headline.
    4. DLQ: 0 (real traffic is never poison).
    5. journalctl: episodes opening/escalating during the plume; plans
       generating behind them; everything cleared ~60s after sim ends.

Screenshot: the plan-provenance ledger --
    SELECT model, status, count(*), round(avg(latency_ms)) avg_ms
    FROM isolation_plans GROUP BY model, status ORDER BY count DESC;

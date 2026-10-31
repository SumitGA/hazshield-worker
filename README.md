# hazshield-worker

These are the alarm workers of HazShield AI, the hazard monitoring system I'm building for
mineral processing plants. The overview of the whole project, with the architecture and
links to the other repos, is in [HazshieldAI](https://github.com/SumitGA/HazshieldAI).

The ingest gateway turns dangerous readings into violations on a Redis Stream. The workers
read that stream, fold thousands of violations into a handful of incidents (episodes), and
write an isolation plan for each serious one, using a local LLM through Ollama. They run on
the compute VM, next to Ollama.

## From violations to plans

Reading the stream. The workers share the violations stream as a group (a Redis consumer
group), so each violation goes to one worker. A worker only marks a violation as done after
its result is saved. If a worker crashes halfway, another worker takes over its unfinished
violations after a minute. That means a violation can occasionally be handled twice, so
every step is written so that doing it twice gives the same result. A violation that keeps
failing is moved aside to a dead-letter queue (`hazshield:violations:dlq`), so one bad
message can't hold up the real alarms behind it. `dlq.py` lists, retries or clears it.

Episodes. A sensor's violations become one episode with a simple lifecycle: a warning opens
it, a critical escalates it (or opens it as critical), and a minute without new violations
clears it. The database itself only allows one open episode per sensor, so two workers can't open
duplicates, and a restarted worker has nothing to rebuild.

Hazard context. When an episode turns critical, the worker describes the situation from
the plant's layout: what the sensor measures, the zone it's in, the zones connected to it
(passages, conveyors, ducts), and the assets and isolation points nearby. A plan that only
knew about the zone with the gas in it could send people the wrong way. The description is
built the same way every time, so the same situation always produces the same text, and a
fingerprint of that text (a SHA-256 hash) identifies the hazard. The live reading is left
out on purpose, otherwise every new reading would look like a new hazard.

Plans. Plan requests wait in the database, and each one is picked up by exactly one
worker. For each request the worker tries, in order:

1. the cache: a ready plan with the same hazard hash is copied, in 0 ms
2. the LLM: Ollama, one generation at a time, asked for a JSON plan with a summary,
   ordered steps (action, target, reason), an evacuation route and a ventilation note
3. a rulebook: a deterministic plan built from the same context (de-energise the zone,
   seal the duct links), used whenever a generation fails. After three failures in a row a
   circuit breaker skips the LLM for 60 seconds and goes straight to the rulebook. Rulebook
   plans are clearly marked as fallbacks, with the reason.

Every plan is stored with the model that produced it, how long it took and its status. On
the live system most plans come from the cache: one flapping sensor costs one generation,
not fifty. Plans stuck in "generating" by a crashed worker are put back to "pending" after
a while.

## Choosing a model

`bench.py` runs the same hazard context through several Ollama models a few times each and
scores whether the output is valid JSON, how long it takes, how many steps it has and
whether it names the actual hazard (an early phi3 plan called a gas leak "overheating").
I use it to pick the model before a demo:

```bash
HAZ_DATABASE_URL=... ./venv/bin/python3 bench.py --models phi3:mini,llama3:8b --trials 3
```

The worker currently runs `llama3:8b`, set with `HAZ_MODEL` in the systemd unit.

## Files

```
worker.py      stream consumer, crash recovery, DLQ, main loop
episodes.py    episode state machine and the sweep that clears quiet episodes
context.py     hazard context from the plant graph, and its hash
generator.py   plan generation: cache, Ollama, rulebook, circuit breaker
telemetry.py   Prometheus metrics and OpenTelemetry traces
dlq.py         inspect, requeue or drain the dead-letter queue
bench.py       compare models on the same hazard
deploy-worker.sh, hazshield-worker@.service_example
```

## Configuration

| Variable | Default | |
|---|---|---|
| `HAZ_DATABASE_URL` | local dev database | PostgreSQL connection string |
| `HAZ_REDIS_URL` | `redis://127.0.0.1:6379/0` | Redis connection string |
| `HAZ_OLLAMA_URL` | `http://127.0.0.1:11434` | Ollama |
| `HAZ_MODEL` | `phi3:mini` (the unit sets `llama3:8b`) | model used for plans |
| `HAZ_GEN_TIMEOUT_S` | `120` (the unit sets `300`) | how long one generation may take |
| `HAZ_CONSUMER` | host name | consumer name in the group (the unit uses host and instance) |
| `HAZ_BATCH` | `100` | messages read per call |
| `HAZ_CLAIM_IDLE_MS` | `60000` | when another worker's pending messages can be taken over |
| `HAZ_CLEAR_AFTER_S` | `60` | quiet time before an episode clears |
| `HAZ_BREAKER_FAILS`, `HAZ_BREAKER_COOLDOWN_S` | `3`, `60` | circuit breaker for the LLM |
| `HAZ_METRICS_PORT` | `9108` | Prometheus metrics |
| `HAZ_OTLP_ENDPOINT` | unset (no traces) | OpenTelemetry collector, e.g. `http://10.10.0.12:4318` |
| `HAZ_CRASH_AFTER` | `0` (off) | chaos testing: exit without acknowledging after n messages |

The service file is a template (`hazshield-worker@.service`), so I can run several workers
as `hazshield-worker@1`, `@2` and so on. The example has `CHANGE_ME` placeholders; the real
connection strings go in a drop-in (`systemctl edit hazshield-worker@1`).

## Deploying

```bash
./deploy-worker.sh
```

It copies the Python files to `/opt/hazshield-workers` on the compute VM, installs the
requirements, restarts `hazshield-worker@1`, and then compares the checksum of the running
`worker.py` with the one in the repo, so I know the deploy actually landed.

Before a demo, check that Ollama has the model and the worker is up:

```bash
ollama list
ollama pull llama3:8b
systemctl is-active ollama hazshield-worker@1
```

## The end-to-end demo

This is how I check that the whole pipeline thinks, not just that it doesn't lose data.

Take a baseline on the edge VM:

```bash
X=$(redis-cli -h 10.10.0.12 XLEN hazshield:violations)
psql "$PG" -c "SELECT count(*) FROM alarm_events; SELECT count(*) FROM isolation_plans;"
```

Run a three-minute gas plume with the simulator from hazshield-ingest:

```bash
./sim.py --pg "$PG" --rate 2000 --duration 180 --scenario plume
```

What I expect afterwards:

1. The stream grew by exactly the warnings plus criticals in the simulator's final line.
2. About 34 new episodes (one per methane sensor in the plume), most of them escalated,
   from more than 4,000 violations.
3. Fewer plans than episodes: sensors in the same zone describe the same hazard, so most
   plans are cache hits and only a few are real generations.
4. Nothing in the dead-letter queue.
5. In `journalctl`, episodes opening and escalating during the plume, plans being generated
   behind them, and everything cleared about a minute after the simulation ends.

And the query I like to finish with, which shows where every plan came from:

```sql
SELECT model, status, count(*), round(avg(latency_ms)) avg_ms
FROM isolation_plans GROUP BY model, status ORDER BY count DESC;
```

#!/usr/bin/env python3
"""DLQ inspector/requeuer.  ./dlq.py list | ./dlq.py requeue <id> | ./dlq.py drain"""
import os, sys
import redis

r = redis.from_url(os.environ.get("HAZ_REDIS_URL", "redis://127.0.0.1:6379/0"),
                   decode_responses=True)
DLQ, MAIN = "hazshield:violations:dlq", "hazshield:violations"

cmd = sys.argv[1] if len(sys.argv) > 1 else "list"
if cmd == "list":
    for eid, fields in r.xrange(DLQ, count=50):
        print(f"{eid}  {fields.get('error','?')}\n    payload: {fields.get('v','')[:120]}")
    print(f"total: {r.xlen(DLQ)}")
elif cmd == "requeue":            # after fixing whatever made it poison
    eid = sys.argv[2]
    entries = r.xrange(DLQ, min=eid, max=eid)
    if not entries: sys.exit("no such entry")
    r.xadd(MAIN, {"v": entries[0][1]["v"]})
    r.xdel(DLQ, eid)
    print(f"requeued {eid} to {MAIN}")
elif cmd == "drain":
    n = r.xlen(DLQ); r.delete(DLQ); print(f"dropped {n} entries")

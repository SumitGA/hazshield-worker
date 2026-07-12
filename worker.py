#!/usr/bin/env python3
"""HazShield alarm worker — Phase 3, Session 1: the consumer.

Reads violations from the Redis Stream the Rust gateway writes, with
the full reliability contract:

  * CONSUMER GROUPS, not pub/sub. Pub/sub is a megaphone: miss the
    moment, miss the message. A consumer group is a work queue with
    memory: every entry is DELIVERED to a named consumer, sits in the
    Pending Entries List (PEL) until ACKed, and can be reclaimed if its
    consumer dies. The gateway already guaranteed alarms reach the
    stream; this guarantees they leave it exactly as reliably.

  * AT-LEAST-ONCE, embraced. A worker can crash after processing but
    before acking; on restart that entry is delivered again. So every
    handler downstream must be IDEMPOTENT — Session 2 keys episodes on
    (sensor_id, ts) so a replayed violation is a no-op, and the same
    discipline continues into planning (prompt_hash dedupe, Session 4).

  * CRASH RECOVERY via XAUTOCLAIM: on every loop iteration we first
    sweep for entries another consumer claimed but never acked (idle
    past a threshold), steal them, and process them. A worker that
    dies mid-batch leaks nothing; its orphans are adopted.

  * GRACEFUL SHUTDOWN: SIGTERM stops new reads, finishes + acks the
    in-flight batch, then exits. Same contract as the gateway.

Chaos hook: HAZ_CRASH_AFTER=n makes the worker die WITHOUT acking after
processing n entries — the tool for demonstrating recovery, in the same
kill-test spirit as Phase 2.
"""
import asyncio
import json
import logging
import os
import signal
import socket
import sys
import time

import asyncpg
import redis.asyncio as aredis

from context import ContextBuilder
from generator import Generator
from episodes import Episodes

STREAM = "hazshield:violations"
GROUP = "hazshield:planners"
# Dead-letter stream: poison entries (unparseable, or repeatedly
# crashing the handler) are quarantined here and ACKed away from the
# main flow. Without this, ONE malformed entry = infinite redelivery:
# handler raises -> batch never acks -> XAUTOCLAIM re-adopts -> raises
# again, forever. The DLQ converts a wedge into a queue you inspect
# at leisure (dlq.py). Alarms are precious; so is the consumer's
# ability to move past one broken alarm to reach the next real one.
DLQ = "hazshield:violations:dlq"

log = logging.getLogger("worker")


def jlog_setup():
    h = logging.StreamHandler()

    class J(logging.Formatter):
        def format(self, r):
            d = {"ts": self.formatTime(r, "%Y-%m-%dT%H:%M:%SZ"),
                 "level": r.levelname, "msg": r.getMessage()}
            if r.__dict__.get("extra_fields"):
                d.update(r.__dict__["extra_fields"])
            return json.dumps(d)

    h.setFormatter(J())
    logging.basicConfig(level=logging.INFO, handlers=[h])


def f(**kw):  # structured-field helper: log.info("msg", extra=f(a=1))
    return {"extra_fields": kw}


class Worker:
    def __init__(self):
        self.redis_url = os.environ.get("HAZ_REDIS_URL", "redis://127.0.0.1:6379/0")
        self.pg_dsn = os.environ.get("HAZ_DATABASE_URL",
                                     "postgresql://hazshield:devpw@127.0.0.1/hazshield")
        # Consumer name must be STABLE across restarts of the same unit:
        # a restarted worker resumes its own pending entries first.
        self.consumer = os.environ.get("HAZ_CONSUMER", socket.gethostname())
        self.batch = int(os.environ.get("HAZ_BATCH", "100"))
        # How long an entry may sit unacked in a dead consumer's PEL
        # before others may steal it. Low for tests, ~60s in prod.
        self.claim_idle_ms = int(os.environ.get("HAZ_CLAIM_IDLE_MS", "60000"))
        self.crash_after = int(os.environ.get("HAZ_CRASH_AFTER", "0"))
        self.stop = asyncio.Event()
        self.processed = 0
        self.quarantined = 0
        self.by_severity = {"warn": 0, "critical": 0}

    async def run(self):
        pool = await asyncpg.create_pool(self.pg_dsn, min_size=1, max_size=4)
        ctx = ContextBuilder(pool, log, f)
        self.episodes = Episodes(pool, log, f, ctx)
        sweeper = asyncio.create_task(self.sweep_loop())
        generator = asyncio.create_task(Generator(pool, ctx, log, f).loop(self.stop))
        r = aredis.from_url(self.redis_url, decode_responses=True,
                            socket_timeout=15, socket_connect_timeout=5)
        # Idempotent group creation. '0' = the group owns the ENTIRE
        # existing backlog (all 683 rehearsal violations included);
        # '$' would mean "only new entries". For an alarm system the
        # backlog IS the job, so: 0.
        try:
            await r.xgroup_create(STREAM, GROUP, id="0", mkstream=True)
            log.info("consumer group created", extra=f(group=GROUP, from_id="0"))
        except aredis.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise
        log.info("worker up", extra=f(consumer=self.consumer, batch=self.batch))

        while not self.stop.is_set():
          # Transient Redis trouble must not kill the worker: log, pause,
          # let the client reconnect on the next command. systemd would
          # restart us anyway, but a worker that survives a blip keeps
          # its batch context and its dignity.
          try:
            # 1) adopt orphans: entries delivered to a consumer that
            #    died before acking, idle past the threshold.
            try:
                _, claimed, _ = await r.xautoclaim(
                    STREAM, GROUP, self.consumer,
                    min_idle_time=self.claim_idle_ms, start_id="0",
                    count=self.batch)
            except aredis.ResponseError:
                claimed = []
            if claimed:
                log.info("adopted orphaned entries", extra=f(count=len(claimed)))
                await self.process(r, claimed)
                continue

            # 2) normal tail-read: '>' = entries never delivered to
            #    anyone in this group. Blocks up to 5s, then loops
            #    (so shutdown latency is bounded).
            resp = await r.xreadgroup(GROUP, self.consumer,
                                      {STREAM: ">"},
                                      count=self.batch, block=5000)
            if resp:
                await self.process(r, resp[0][1])
          except (aredis.ConnectionError, aredis.TimeoutError) as e:
            log.warning("redis hiccup; retrying", extra=f(error=str(e)))
            await asyncio.sleep(2)

        sweeper.cancel()
        generator.cancel()
        await pool.close()
        await r.aclose()
        log.info("drained and stopped", extra=f(processed=self.processed,
                                                **self.by_severity))

    async def process(self, r, entries):
        ids = []
        for entry_id, fields in entries:
            try:
                v = json.loads(fields.get("v", ""))
                # Episode transition COMMITS (fsync'd, SET LOCAL sync
                # commit) before this entry is acked. Crash after
                # commit, before ack => redelivery => idempotent upsert.
                # Durable first, then ack. Never the reverse.
                await self.episodes.apply(v)
                self.by_severity[v["severity"]] = self.by_severity.get(v["severity"], 0) + 1
            except (json.JSONDecodeError, KeyError, TypeError, ValueError) as e:
                # Poison: malformed shape. Quarantine + ack; do NOT let
                # one bad entry wedge the whole partition. Infra errors
                # (DB down) still raise and abort the batch un-acked —
                # those SHOULD be retried; poison should not.
                await r.xadd(DLQ, {"original_id": entry_id,
                                   "error": f"{type(e).__name__}: {str(e)[:150]}",
                                   "v": fields.get("v", "")},
                             maxlen=10_000, approximate=True)
                self.quarantined += 1
                log.warning("poison entry quarantined to DLQ", extra=f(
                    entry=entry_id, error=f"{type(e).__name__}: {str(e)[:80]}"))
            self.processed += 1
            ids.append(entry_id)

            if self.crash_after and self.processed >= self.crash_after:
                # Chaos hook: die WITHOUT acking. The batch stays in
                # our PEL for another consumer to adopt.
                log.error("CHAOS: crashing without ack",
                          extra=f(processed=self.processed, unacked=len(ids)))
                os._exit(1)

        # ACK the batch: only now do these entries leave the PEL.
        # Crash anywhere above => redelivery => idempotency matters.
        await r.xack(STREAM, GROUP, *ids)
        if self.processed % 500 == 0 or len(entries) < self.batch:
            log.info("progress", extra=f(processed=self.processed,
                                         **self.by_severity))


    async def sweep_loop(self):
        # Clearing is time-driven, not event-driven: run alongside
        # consumption. Multiple workers sweeping is harmless — each
        # quiet episode matches exactly one UPDATE.
        while not self.stop.is_set():
            try:
                await self.episodes.sweep()
            except Exception as e:
                log.warning("sweep failed; retrying", extra=f(error=str(e)))
            await asyncio.sleep(min(self.episodes.clear_after / 3, 10))


def main():
    jlog_setup()
    w = Worker()
    loop = asyncio.new_event_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, w.stop.set)
    loop.run_until_complete(w.run())


if __name__ == "__main__":
    main()

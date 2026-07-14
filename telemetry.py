"""Observability for the worker cluster — Phase 5.

Two planes, one module:

1. METRICS (Prometheus). Counters and histograms exposed on a tiny
   HTTP endpoint Prometheus scrapes. These answer "how much, how fast,
   how often" — throughput, generation latency distribution, DLQ rate,
   breaker state. Cheap, always-on, aggregate.

2. TRACES (OpenTelemetry). A span tree following ONE violation from
   the moment the worker reads it, through the episode upsert, the
   context build, and the LLM generation. These answer "what happened
   to THIS one" — the causal chain, with timing on every hop. Exported
   to the collector over OTLP; sampled, because tracing every violation
   in a plume would drown the collector and tell you nothing new.

Both degrade to no-ops if their libraries or endpoints are absent, so
the worker still runs on a box without a collector. Observability that
takes down the thing it observes is worse than none.
"""
import os
import time
from contextlib import contextmanager

# ---- metrics -------------------------------------------------------
try:
    from prometheus_client import Counter, Histogram, Gauge, start_http_server
    _PROM = True
except ImportError:
    _PROM = False

if _PROM:
    VIOLATIONS = Counter("hazshield_violations_processed_total",
                         "Violations consumed from the stream", ["severity"])
    QUARANTINED = Counter("hazshield_violations_quarantined_total",
                          "Poison violations sent to the DLQ")
    EPISODES = Counter("hazshield_episodes_total",
                       "Episode transitions", ["kind"])   # opened/escalated/cleared
    PLANS = Counter("hazshield_plans_total",
                    "Plan outcomes", ["source"])          # llm/cache/rulebook
    GEN_LATENCY = Histogram("hazshield_generation_seconds",
                            "LLM generation wall time",
                            buckets=(1, 5, 10, 20, 30, 45, 60, 120, 300))
    BREAKER = Gauge("hazshield_breaker_open",
                    "1 when the generator circuit breaker is open")
    STREAM_LAG = Gauge("hazshield_consumer_pending",
                       "Entries delivered but not yet acked (PEL depth)")


def start_metrics_server():
    port = int(os.environ.get("HAZ_METRICS_PORT", "9108"))
    if _PROM:
        start_http_server(port)
        return port
    return None


def m_violation(sev):
    if _PROM: VIOLATIONS.labels(severity=sev).inc()

def m_quarantine():
    if _PROM: QUARANTINED.inc()

def m_episode(kind):
    if _PROM: EPISODES.labels(kind=kind).inc()

def m_plan(source):
    if _PROM: PLANS.labels(source=source).inc()

def m_generation(seconds):
    if _PROM: GEN_LATENCY.observe(seconds)

def m_breaker(is_open):
    if _PROM: BREAKER.set(1 if is_open else 0)

def m_pending(n):
    if _PROM: STREAM_LAG.set(n)


# ---- traces --------------------------------------------------------
try:
    from opentelemetry import trace, context as otel_context
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    from opentelemetry.trace import Status, StatusCode
    _OTEL = True
except ImportError:
    _OTEL = False

_tracer = None


def init_tracing():
    """Wire up OTLP export if a collector endpoint is configured."""
    global _tracer
    if not _OTEL:
        return None
    endpoint = os.environ.get("HAZ_OTLP_ENDPOINT")
    if not endpoint:
        return None
    resource = Resource.create({
        "service.name": "hazshield-worker",
        "service.instance.id": os.environ.get("HAZ_CONSUMER", "worker"),
    })
    provider = TracerProvider(resource=resource)
    provider.add_span_processor(BatchSpanProcessor(
        OTLPSpanExporter(endpoint=f"{endpoint}/v1/traces")))
    trace.set_tracer_provider(provider)
    _tracer = trace.get_tracer("hazshield")
    return _tracer


@contextmanager
def span(name, **attrs):
    """Start a span if tracing is live, else a transparent no-op."""
    if _tracer is None:
        yield None
        return
    with _tracer.start_as_current_span(name) as s:
        for k, v in attrs.items():
            s.set_attribute(k, v)
        try:
            yield s
        except Exception as e:
            if _OTEL:
                s.set_status(Status(StatusCode.ERROR, str(e)))
                s.record_exception(e)
            raise

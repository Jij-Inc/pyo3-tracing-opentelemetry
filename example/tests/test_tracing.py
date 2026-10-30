"""Tests for pyo3-tracing-opentelemetry integration using snapshot testing."""

from concurrent.futures import ThreadPoolExecutor

import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace import ReadableSpan
from opentelemetry.trace import NonRecordingSpan, SpanContext, TraceFlags, TraceState

import example_module

# Attributes that vary between runs and should be excluded from snapshots
UNSTABLE_ATTRIBUTES = {"busy_ns", "idle_ns", "thread.id"}


def normalize_span(span: ReadableSpan, span_id_map: dict[int, str]) -> dict:
    """Normalize a span for snapshot comparison.

    Replaces dynamic values (trace_id, span_id, timestamps) with stable placeholders.
    Removes timing-related attributes that vary between runs.
    """
    # Get span_id (context is always present for ReadableSpan)
    context = span.context
    assert context is not None
    span_id = context.span_id

    # Map span_id to a stable name
    if span_id not in span_id_map:
        span_id_map[span_id] = f"span_{len(span_id_map)}"

    parent_id = None
    parent = span.parent
    if parent is not None:
        parent_span_id = parent.span_id
        if parent_span_id not in span_id_map:
            span_id_map[parent_span_id] = f"span_{len(span_id_map)}"
        parent_id = span_id_map[parent_span_id]

    # Filter out unstable attributes
    attributes = {}
    if span.attributes:
        attributes = {
            k: v for k, v in span.attributes.items() if k not in UNSTABLE_ATTRIBUTES
        }

    return {
        "name": span.name,
        "id": span_id_map[span_id],
        "parent_id": parent_id,
        "attributes": attributes,
    }


def normalize_spans(spans: list[ReadableSpan]) -> list[dict]:
    """Normalize a list of spans, sorted by name for deterministic output."""
    span_id_map: dict[int, str] = {}
    normalized = [normalize_span(span, span_id_map) for span in spans]
    return sorted(normalized, key=lambda s: s["name"])


def test_traced_function(span_exporter, tracer, snapshot):
    """Test that traced_function produces spans that are forwarded to Python."""
    with tracer.start_as_current_span("python-parent"):
        result = example_module.traced_function()

    assert result == 42
    assert normalize_spans(span_exporter.spans) == snapshot


def test_nested_spans(span_exporter, tracer, snapshot):
    """Test that nested spans are properly forwarded."""
    with tracer.start_as_current_span("python-parent"):
        example_module.nested_spans()

    assert normalize_spans(span_exporter.spans) == snapshot


def test_trace_context_propagation(span_exporter, tracer):
    """Test that trace context is properly propagated from Python to Rust."""
    with tracer.start_as_current_span("python-parent") as parent_span:
        example_module.traced_function()

    parent_trace_id = parent_span.get_span_context().trace_id

    assert {span.name for span in span_exporter.spans} == {
        "python-parent",
        "traced_function_inner",
        "do_work",
    }

    # All spans should have the same trace_id
    for span in span_exporter.spans:
        assert span.context.trace_id == parent_trace_id, (
            f"Span '{span.name}' has different trace_id"
        )

    spans = {span.name: span for span in span_exporter.spans}
    assert (
        spans["traced_function_inner"].parent.span_id
        == parent_span.get_span_context().span_id
    )
    assert (
        spans["do_work"].parent.span_id
        == spans["traced_function_inner"].context.span_id
    )


@pytest.mark.parametrize("sampled", [False, True])
def test_remote_context_and_sampling(span_exporter, sampled):
    """Preserve W3C IDs, tracestate and sampling across detached Rust work."""
    parent = SpanContext(
        trace_id=0x4BF92F3577B34DA6A3CE929D0E0E4736,
        span_id=0x00F067AA0BA902B7,
        is_remote=True,
        trace_flags=TraceFlags(TraceFlags.SAMPLED if sampled else TraceFlags.DEFAULT),
        trace_state=TraceState([("vendor", "opaque-value")]),
    )
    with trace.use_span(NonRecordingSpan(parent)):
        assert example_module.traced_function() == 42
        assert trace.get_current_span().get_span_context() == parent

    if sampled:
        spans = {span.name: span for span in span_exporter.spans}
        assert set(spans) == {"traced_function_inner", "do_work"}
        assert spans["traced_function_inner"].parent.span_id == parent.span_id
        for span in spans.values():
            assert span.context.trace_id == parent.trace_id
            assert span.context.trace_state == parent.trace_state
            assert span.context.trace_flags.sampled
    else:
        assert span_exporter.spans == []

    # The Rust ContextGuard must not leave the remote parent attached.
    span_exporter.clear()
    assert example_module.traced_function() == 42
    root = next(s for s in span_exporter.spans if s.name == "traced_function_inner")
    assert root.parent is None
    assert root.context.trace_id != parent.trace_id


def test_context_isolation_between_threads(span_exporter, tracer):
    """Each Python caller keeps its own parent while Rust releases Python."""

    def run(index):
        with tracer.start_as_current_span(f"python-{index}") as parent:
            assert example_module.traced_function() == 42
            assert trace.get_current_span() is parent
            return parent.get_span_context()

    with ThreadPoolExecutor(max_workers=4) as executor:
        parents = list(executor.map(run, range(12)))

    assert len({parent.trace_id for parent in parents}) == len(parents)
    rust_spans = [s for s in span_exporter.spans if s.name == "traced_function_inner"]
    assert len(rust_spans) == len(parents)
    assert {(s.context.trace_id, s.parent.span_id) for s in rust_spans} == {
        (parent.trace_id, parent.span_id) for parent in parents
    }


def test_exported_span_metadata(span_exporter):
    """Exercise Python SDK object construction, including 128-bit IDs and events."""
    assert example_module.traced_function() == 42
    spans = {span.name: span for span in span_exporter.spans}
    assert set(spans) == {"traced_function_inner", "do_work"}
    for span in spans.values():
        assert span.context.is_valid
        assert span.start_time > 0
        assert span.end_time >= span.start_time
        assert span.resource.attributes["service.name"] == "example-module"
        assert span.instrumentation_scope.name == "example-module"
        assert span.kind == trace.SpanKind.INTERNAL
        assert span.status.status_code == trace.StatusCode.UNSET
        assert span.events
        assert all(span.start_time <= e.timestamp <= span.end_time for e in span.events)
    assert spans["traced_function_inner"].events[-1].attributes["result"] == 42


def test_traced_with_attributes(span_exporter, tracer, snapshot):
    """Test that span attributes are properly set."""
    with tracer.start_as_current_span("python-parent"):
        example_module.traced_with_attributes("test-request", 3)

    assert normalize_spans(span_exporter.spans) == snapshot


def test_rust_spans_follow_added_span_processor(span_exporter):
    """Rust spans must follow span processors added to the global `TracerProvider`
    *after* the tracing bridge has already been initialized.

    This is the core guarantee of dynamic span-processor resolution: the
    destination isn't frozen at the first call into the bridge — new
    processors registered later start receiving spans immediately. Before
    v0.2.0 the Rust side captured the span processors tuple once at init
    time and stored it in a `OnceLock`, so processors added later never
    received spans. This test would have failed against that behavior.
    """
    from opentelemetry import trace
    from opentelemetry.sdk.trace import TracerProvider as SdkTracerProvider
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor

    # Force init (if not already) and make sure the baseline exporter works.
    span_exporter.clear()
    example_module.traced_function()
    assert len(span_exporter.spans) > 0, (
        "Baseline sanity check failed: the session exporter collected no spans"
    )

    # Now register an additional processor on the same provider. The session
    # fixture installs an SDK TracerProvider; assert that so pyright can
    # narrow from the abstract `TracerProvider` (no `add_span_processor`) to
    # the SDK type. Build the extra exporter from the same class the fixture
    # gave us so we don't depend on importing `conftest` as a module.
    extra = type(span_exporter)()
    provider = trace.get_tracer_provider()
    assert isinstance(provider, SdkTracerProvider)

    # Snapshot the provider's span-processor tuple so we can restore it in
    # `finally`. The SDK has no public remove_span_processor API, so we touch
    # the same private attribute the bridge itself walks.
    active_processor = provider._active_span_processor
    original_processors = active_processor._span_processors
    provider.add_span_processor(SimpleSpanProcessor(extra))

    try:
        span_exporter.clear()
        example_module.traced_function()
        assert len(extra.spans) > 0, (
            "Newly added span processor did not receive Rust spans. "
            "This indicates the bridge is still using a cached snapshot of "
            "span processors from initialization time."
        )
        # The originally-registered processor continues to receive spans too.
        assert len(span_exporter.spans) > 0
    finally:
        # Restore the provider's processor tuple so the extra processor does
        # not leak into subsequent tests, and shut the exporter down.
        active_processor._span_processors = original_processors
        extra.shutdown()

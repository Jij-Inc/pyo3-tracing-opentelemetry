"""Tests for pyo3-tracing-opentelemetry integration using snapshot testing."""

from opentelemetry.sdk.trace import ReadableSpan

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

    # All spans should have the same trace_id
    for span in span_exporter.spans:
        assert span.context.trace_id == parent_trace_id, (
            f"Span '{span.name}' has different trace_id"
        )


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
    v0.1.3 the Rust side captured the span processors tuple once at init
    time and stored it in a `OnceLock`, so processors added later never
    received spans. This test would have failed against that behavior.
    """
    from opentelemetry import trace
    from opentelemetry.sdk.trace import TracerProvider as SdkTracerProvider
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor

    from conftest import TestSpanExporter

    # Force init (if not already) and make sure the baseline exporter works.
    span_exporter.clear()
    example_module.traced_function()
    assert len(span_exporter.spans) > 0, (
        "Baseline sanity check failed: the session exporter collected no spans"
    )

    # Now register an additional processor on the same provider. The session
    # fixture installs an SDK TracerProvider; assert that so pyright can
    # narrow from the abstract `TracerProvider` (no `add_span_processor`) to
    # the SDK type.
    extra = TestSpanExporter()
    provider = trace.get_tracer_provider()
    assert isinstance(provider, SdkTracerProvider)
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
        # The SDK has no public remove_span_processor API; shutting the
        # exporter down prevents the leaked processor from doing work in
        # subsequent tests.
        extra.shutdown()

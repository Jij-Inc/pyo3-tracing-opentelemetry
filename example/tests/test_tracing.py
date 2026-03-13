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
    # Map span_id to a stable name based on span name
    if span.context.span_id not in span_id_map:
        span_id_map[span.context.span_id] = f"span_{len(span_id_map)}"

    parent_id = None
    if span.parent is not None:
        if span.parent.span_id not in span_id_map:
            span_id_map[span.parent.span_id] = f"span_{len(span_id_map)}"
        parent_id = span_id_map[span.parent.span_id]

    # Filter out unstable attributes
    attributes = {}
    if span.attributes:
        attributes = {
            k: v for k, v in span.attributes.items() if k not in UNSTABLE_ATTRIBUTES
        }

    return {
        "name": span.name,
        "id": span_id_map[span.context.span_id],
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

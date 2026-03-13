"""Tests for pyo3-tracing-opentelemetry integration."""

import example_module


def test_traced_function(span_exporter, tracer):
    """Test that traced_function produces spans that are forwarded to Python."""
    with tracer.start_as_current_span("python-parent"):
        result = example_module.traced_function()

    assert result == 42

    # Check that we got spans from Rust
    span_names = [span.name for span in span_exporter.spans]
    assert "python-parent" in span_names
    # Rust spans should also be present
    assert any("traced_function_inner" in name for name in span_names)


def test_nested_spans(span_exporter, tracer):
    """Test that nested spans are properly forwarded."""
    with tracer.start_as_current_span("python-parent"):
        example_module.nested_spans()

    span_names = [span.name for span in span_exporter.spans]
    assert "python-parent" in span_names
    assert "outer_span" in span_names
    assert "inner_span" in span_names


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


def test_traced_with_attributes(span_exporter, tracer):
    """Test that span attributes are properly set."""
    with tracer.start_as_current_span("python-parent"):
        example_module.traced_with_attributes("test-request", 3)

    span_names = [span.name for span in span_exporter.spans]
    assert "python-parent" in span_names
    assert "process_request" in span_names

    # Find the process_request span and check attributes
    process_span = next(s for s in span_exporter.spans if s.name == "process_request")
    assert process_span.attributes is not None
    assert process_span.attributes.get("name") == "test-request"
    # Note: tracing fields are converted to strings by the OpenTelemetry layer
    assert process_span.attributes.get("count") == "3"

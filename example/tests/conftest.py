"""Pytest configuration for tracing tests."""

import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider, ReadableSpan
from opentelemetry.sdk.trace.export import SimpleSpanProcessor, SpanExporter, SpanExportResult
from opentelemetry.sdk.resources import Resource
from typing import Sequence


class TestSpanExporter(SpanExporter):
    """A simple span exporter that collects spans for testing."""

    def __init__(self):
        self.spans: list[ReadableSpan] = []

    def export(self, spans: Sequence[ReadableSpan]) -> SpanExportResult:
        self.spans.extend(spans)
        return SpanExportResult.SUCCESS

    def shutdown(self) -> None:
        pass

    def force_flush(self, timeout_millis: int = 30000) -> bool:
        return True

    def clear(self) -> None:
        self.spans.clear()


_test_exporter: TestSpanExporter | None = None
_test_provider: TracerProvider | None = None


@pytest.fixture(scope="session", autouse=True)
def setup_test_tracing():
    """Set up a test TracerProvider with a span exporter."""
    global _test_exporter, _test_provider

    previous_provider = trace.get_tracer_provider()

    _test_exporter = TestSpanExporter()
    resource = Resource.create({"service.name": "example-module-test"})
    _test_provider = TracerProvider(resource=resource)
    _test_provider.add_span_processor(SimpleSpanProcessor(_test_exporter))
    trace.set_tracer_provider(_test_provider)

    try:
        yield
    finally:
        if _test_provider is not None:
            _test_provider.shutdown()
        trace.set_tracer_provider(previous_provider)
        _test_exporter = None
        _test_provider = None


@pytest.fixture
def span_exporter() -> TestSpanExporter:
    """Get the test span exporter and clear any previous spans."""
    assert _test_exporter is not None
    _test_exporter.clear()
    return _test_exporter


@pytest.fixture
def tracer():
    """Get a tracer for creating test spans."""
    return trace.get_tracer("test-tracer")

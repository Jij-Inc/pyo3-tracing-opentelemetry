//! Trace context propagation between Python and Rust.

use std::collections::HashMap;

use opentelemetry::{Context, propagation::TextMapPropagator, trace::TraceContextExt};
use opentelemetry_sdk::propagation::TraceContextPropagator;
use pyo3::{prelude::*, types::PyDict};

/// Extract OpenTelemetry context from W3C trace headers.
///
/// The headers should contain at least `traceparent`, and optionally `tracestate`.
/// traceparent format: `{version}-{trace_id}-{parent_id}-{flags}`
/// Example: `00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01`
pub fn extract_context_from_headers(headers: &HashMap<String, String>) -> Option<Context> {
    let propagator = TraceContextPropagator::new();
    let context = propagator.extract(headers);

    // Check if the context has a valid span context
    if context.span().span_context().is_valid() {
        Some(context)
    } else {
        None
    }
}

/// Get trace context headers from Python's OpenTelemetry context.
///
/// This function calls Python's `opentelemetry.propagate.inject()` to get the
/// current trace context as W3C trace headers (traceparent and tracestate).
pub fn get_trace_headers_from_python(py: Python) -> Option<HashMap<String, String>> {
    let propagate = py.import("opentelemetry.propagate").ok()?;
    let inject = propagate.getattr("inject").ok()?;
    let carrier = PyDict::new(py);
    inject.call1((&carrier,)).ok()?;

    let mut headers = HashMap::new();

    // Extract traceparent (required)
    if let Some(value) = carrier
        .get_item("traceparent")
        .ok()
        .and_then(|v| v.and_then(|v| v.extract().ok()))
    {
        headers.insert("traceparent".to_string(), value);
    }

    // Extract tracestate (optional)
    if let Some(value) = carrier
        .get_item("tracestate")
        .ok()
        .and_then(|v| v.and_then(|v| v.extract().ok()))
    {
        headers.insert("tracestate".to_string(), value);
    }

    if headers.is_empty() {
        None
    } else {
        Some(headers)
    }
}

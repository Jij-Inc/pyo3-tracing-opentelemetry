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
    let sanitized_headers = sanitize_traceparent_flags(headers);
    let context = propagator.extract(&sanitized_headers);

    // Check if the context has a valid span context
    if context.span().span_context().is_valid() {
        Some(context)
    } else {
        None
    }
}

fn sanitize_traceparent_flags(headers: &HashMap<String, String>) -> HashMap<String, String> {
    let mut headers = headers.clone();
    let Some(traceparent) = headers.get("traceparent") else {
        return headers;
    };

    let parts = traceparent.split('-').collect::<Vec<_>>();
    if parts.len() != 4 || parts[0] != "00" || parts[3].len() != 2 {
        return headers;
    }

    let Ok(flags) = u8::from_str_radix(parts[3], 16) else {
        return headers;
    };

    let sampled = flags & 0x01;
    if sampled == flags {
        return headers;
    }

    headers.insert(
        "traceparent".to_string(),
        format!("{}-{}-{}-{sampled:02x}", parts[0], parts[1], parts[2],),
    );
    headers
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

#[cfg(test)]
mod tests {
    use super::*;

    fn headers(traceparent: &str) -> HashMap<String, String> {
        HashMap::from([("traceparent".to_string(), traceparent.to_string())])
    }

    #[test]
    fn extracts_context_from_traceparent_with_new_python_flags() {
        let context = extract_context_from_headers(&headers(
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-03",
        ))
        .expect("traceparent with extra flags should still propagate sampled context");

        let span_context = context.span().span_context().clone();
        assert!(span_context.is_valid());
        assert_eq!(
            span_context.trace_id().to_string(),
            "4bf92f3577b34da6a3ce929d0e0e4736"
        );
        assert_eq!(span_context.span_id().to_string(), "00f067aa0ba902b7");
        assert!(span_context.trace_flags().is_sampled());
    }

    #[test]
    fn preserves_unsampled_traceparent_with_extra_flags() {
        let context = extract_context_from_headers(&headers(
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-02",
        ))
        .expect("traceparent with extra unsampled flags should still propagate context");

        let span_context = context.span().span_context().clone();
        assert!(span_context.is_valid());
        assert!(!span_context.trace_flags().is_sampled());
    }
}

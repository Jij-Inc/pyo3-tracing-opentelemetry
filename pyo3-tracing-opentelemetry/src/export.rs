//! Span export functionality - converts Rust SpanData to Python ReadableSpan.

use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::SystemTime;

use opentelemetry::trace::SpanKind;
use opentelemetry_sdk::{
    Resource,
    error::OTelSdkResult,
    trace::{SpanData, SpanExporter as OTelSpanExporter},
};
use pyo3::{
    prelude::*,
    sync::PyOnceLock,
    types::{PyDict, PyList},
};

// ============================================================================
// Python Class Cache (to avoid repeated imports)
// ============================================================================

/// Cached Python classes for span conversion.
/// Using PyOnceLock to cache imports across calls.
pub(crate) struct PythonClasses {
    pub span_context_class: Py<PyAny>,
    pub span_kind_class: Py<PyAny>,
    pub trace_flags_class: Py<PyAny>,
    pub trace_state_class: Py<PyAny>,
    pub readable_span_class: Py<PyAny>,
    pub event_class: Py<PyAny>,
    pub resource_class: Py<PyAny>,
    pub instrumentation_scope_class: Py<PyAny>,
    pub status_class: Py<PyAny>,
    pub status_code_class: Py<PyAny>,
}

static PYTHON_CLASSES: PyOnceLock<PythonClasses> = PyOnceLock::new();

pub(crate) fn get_python_classes(py: Python<'_>) -> PyResult<&PythonClasses> {
    PYTHON_CLASSES.get_or_try_init(py, || {
        let trace_module = py.import("opentelemetry.trace")?;
        let sdk_trace_module = py.import("opentelemetry.sdk.trace")?;
        let resources_module = py.import("opentelemetry.sdk.resources")?;
        let instrumentation_module = py.import("opentelemetry.sdk.util.instrumentation")?;
        let status_module = py.import("opentelemetry.trace.status")?;

        Ok(PythonClasses {
            span_context_class: trace_module.getattr("SpanContext")?.unbind(),
            span_kind_class: trace_module.getattr("SpanKind")?.unbind(),
            trace_flags_class: trace_module.getattr("TraceFlags")?.unbind(),
            trace_state_class: trace_module.getattr("TraceState")?.unbind(),
            readable_span_class: sdk_trace_module.getattr("ReadableSpan")?.unbind(),
            event_class: sdk_trace_module.getattr("Event")?.unbind(),
            resource_class: resources_module.getattr("Resource")?.unbind(),
            instrumentation_scope_class: instrumentation_module
                .getattr("InstrumentationScope")?
                .unbind(),
            status_class: status_module.getattr("Status")?.unbind(),
            status_code_class: status_module.getattr("StatusCode")?.unbind(),
        })
    })
}

// ============================================================================
// PySpanExporter
// ============================================================================

/// Python span processor-based exporter that forwards spans to Python's OpenTelemetry
/// by directly constructing ReadableSpan objects.
///
/// The target span processors are resolved **dynamically** from Python's current
/// `TracerProvider` on every `export()` call, rather than captured once at init
/// time. This means the embedding application can call
/// `trace.set_tracer_provider(...)` or swap span processors at any point and
/// subsequent Rust spans will be routed to the new destination.
///
/// If Python has no SDK `TracerProvider` (or it has no span processors) at the
/// moment spans are exported, those spans are dropped silently.
pub(crate) struct PySpanExporter {
    /// Resource attributes to include in spans
    pub resource: Resource,
}

impl std::fmt::Debug for PySpanExporter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PySpanExporter").finish()
    }
}

/// One-shot latch so we only warn once per process about missing
/// `opentelemetry` Python modules. A broken environment should surface a
/// diagnostic, but we don't want to spam the log on every span export.
static IMPORT_WARNING_EMITTED: AtomicBool = AtomicBool::new(false);

fn warn_once_on_import_failure(module: &str, err: &pyo3::PyErr) {
    if !IMPORT_WARNING_EMITTED.swap(true, Ordering::Relaxed) {
        ::tracing::warn!(
            "pyo3-tracing-opentelemetry: failed to import `{module}` ({err}). \
             Rust spans will be dropped until the `opentelemetry` / \
             `opentelemetry-sdk` Python packages are importable. This warning \
             is emitted at most once per process."
        );
    }
}

/// Look up Python's current `TracerProvider` and return its active span
/// processors tuple, if any.
///
/// Returns `None` when:
/// - `opentelemetry.trace` / `opentelemetry.sdk.trace` cannot be imported
///   (in which case a one-shot warning is also emitted — a missing
///   `opentelemetry-sdk` is almost always an environment / packaging bug
///   rather than an intentional state),
/// - the current provider is not an SDK `TracerProvider` (e.g. the default
///   `ProxyTracerProvider`),
/// - the provider has no span processors configured.
///
/// The last two cases stay silent so tests and notebooks can set up
/// tracing lazily without log noise.
///
/// Uses the `_active_span_processor._span_processors` private path because
/// the OpenTelemetry Python SDK does not expose a public API for this.
fn get_current_span_processors(py: Python<'_>) -> Option<Py<PyAny>> {
    let trace = match py.import("opentelemetry.trace") {
        Ok(m) => m,
        Err(e) => {
            warn_once_on_import_failure("opentelemetry.trace", &e);
            return None;
        }
    };
    let sdk_trace = match py.import("opentelemetry.sdk.trace") {
        Ok(m) => m,
        Err(e) => {
            warn_once_on_import_failure("opentelemetry.sdk.trace", &e);
            return None;
        }
    };
    let tracer_provider_class = sdk_trace.getattr("TracerProvider").ok()?;

    let provider = trace.call_method0("get_tracer_provider").ok()?;

    if !provider.is_instance(&tracer_provider_class).ok()? {
        return None;
    }

    let active_processor = provider.getattr("_active_span_processor").ok()?;
    let span_processors = active_processor.getattr("_span_processors").ok()?;

    let len: usize = span_processors.len().ok()?;
    if len == 0 {
        return None;
    }

    Some(span_processors.unbind())
}

impl OTelSpanExporter for PySpanExporter {
    fn export(&self, batch: Vec<SpanData>) -> impl Future<Output = OTelSdkResult> + Send {
        let mut errors: Vec<String> = Vec::new();

        Python::attach(|py| {
            // Resolve the current destination span processors dynamically.
            // If Python has no provider right now, drop the batch silently:
            // this lets tests and notebooks install/swap/teardown providers
            // freely without keeping state alive on the Rust side.
            let Some(span_processors) = get_current_span_processors(py) else {
                return;
            };

            for span_data in batch {
                // Convert SpanData directly to Python ReadableSpan
                match span_data_to_readable_span(py, &span_data, &self.resource) {
                    Ok(readable_span) => {
                        // Iterate over all span processors and call on_end on each
                        match span_processors.bind(py).try_iter() {
                            Ok(processors) => {
                                for item in processors {
                                    match item {
                                        Ok(processor) => {
                                            // Call processor.on_end(readable_span)
                                            if let Err(err) =
                                                processor.call_method1("on_end", (&readable_span,))
                                            {
                                                let msg = format!(
                                                    "PySpanExporter: processor.on_end raised exception: {}",
                                                    err
                                                );
                                                ::tracing::warn!("{}", msg);
                                                errors.push(msg);
                                            }
                                        }
                                        Err(err) => {
                                            let msg = format!(
                                                "PySpanExporter: failed to iterate span_processors: {}",
                                                err
                                            );
                                            ::tracing::warn!("{}", msg);
                                            errors.push(msg);
                                        }
                                    }
                                }
                            }
                            Err(err) => {
                                let msg = format!(
                                    "PySpanExporter: failed to get iterator over span_processors: {}",
                                    err
                                );
                                ::tracing::warn!("{}", msg);
                                errors.push(msg);
                            }
                        }
                    }
                    Err(err) => {
                        let msg = format!(
                            "PySpanExporter: failed to convert SpanData to ReadableSpan: {}",
                            err
                        );
                        ::tracing::warn!("{}", msg);
                        errors.push(msg);
                    }
                }
            }
        });

        let result = if errors.is_empty() {
            Ok(())
        } else {
            let msg = errors.join("; ");
            Err(opentelemetry_sdk::error::OTelSdkError::InternalFailure(msg))
        };

        std::future::ready(result)
    }
}

// ============================================================================
// Span Conversion Utilities
// ============================================================================

/// Convert SystemTime to nanoseconds since Unix epoch
fn system_time_to_nanos(time: SystemTime) -> u64 {
    time.duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

/// Convert OpenTelemetry Value to Python object
fn value_to_pyobject(py: Python<'_>, value: &opentelemetry::Value) -> PyResult<Py<PyAny>> {
    use opentelemetry::Value;
    use pyo3::conversion::IntoPyObjectExt;
    match value {
        Value::Bool(b) => Ok((*b).into_py_any(py)?),
        Value::I64(i) => Ok((*i).into_py_any(py)?),
        Value::F64(f) => Ok((*f).into_py_any(py)?),
        Value::String(s) => Ok(s.as_str().into_py_any(py)?),
        Value::Array(arr) => {
            use opentelemetry::Array;
            match arr {
                Array::Bool(v) => {
                    let vec: Vec<bool> = v.to_vec();
                    Ok(vec.into_py_any(py)?)
                }
                Array::I64(v) => {
                    let vec: Vec<i64> = v.to_vec();
                    Ok(vec.into_py_any(py)?)
                }
                Array::F64(v) => {
                    let vec: Vec<f64> = v.to_vec();
                    Ok(vec.into_py_any(py)?)
                }
                Array::String(v) => {
                    let strings: Vec<&str> = v.iter().map(|s| s.as_ref()).collect();
                    Ok(strings.into_py_any(py)?)
                }
                // Handle unknown array types by stringifying
                _ => Ok(format!("{:?}", arr).into_py_any(py)?),
            }
        }
        // Handle unknown value types by stringifying
        _ => Ok(format!("{:?}", value).into_py_any(py)?),
    }
}

/// Convert TraceId to u128
fn trace_id_to_u128(trace_id: opentelemetry::trace::TraceId) -> u128 {
    u128::from_be_bytes(trace_id.to_bytes())
}

/// Convert SpanId to u64
fn span_id_to_u64(span_id: opentelemetry::trace::SpanId) -> u64 {
    u64::from_be_bytes(span_id.to_bytes())
}

/// Convert Rust TraceState to Python TraceState
fn trace_state_to_python<'py>(
    _py: Python<'py>,
    trace_state: &opentelemetry::trace::TraceState,
    trace_state_class: &Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyAny>> {
    // TraceState is a list of key-value pairs; convert to Python
    // Python's TraceState can be created from a list of (key, value) tuples
    let header = trace_state.header();
    let pairs: Vec<(String, String)> = header
        .split(',')
        .filter_map(|pair| {
            let mut parts = pair.splitn(2, '=');
            match (parts.next(), parts.next()) {
                (Some(k), Some(v)) if !k.is_empty() => Some((k.to_string(), v.to_string())),
                _ => None,
            }
        })
        .collect();

    if pairs.is_empty() {
        // Empty trace state
        trace_state_class.call0()
    } else {
        // Create from pairs
        trace_state_class.call1((pairs,))
    }
}

/// Convert SpanData directly to Python ReadableSpan
pub(crate) fn span_data_to_readable_span(
    py: Python<'_>,
    span: &SpanData,
    rust_resource: &Resource,
) -> PyResult<Py<PyAny>> {
    // Get cached Python classes
    let classes = get_python_classes(py)?;

    let span_context_class = classes.span_context_class.bind(py);
    let span_kind_class = classes.span_kind_class.bind(py);
    let trace_flags_class = classes.trace_flags_class.bind(py);
    let trace_state_class = classes.trace_state_class.bind(py);
    let readable_span_class = classes.readable_span_class.bind(py);
    let event_class = classes.event_class.bind(py);
    let resource_class = classes.resource_class.bind(py);
    let instrumentation_scope_class = classes.instrumentation_scope_class.bind(py);
    let status_class = classes.status_class.bind(py);
    let status_code_class = classes.status_code_class.bind(py);

    // Create TraceFlags and TraceState (preserve original trace_state)
    let trace_flags = trace_flags_class.call1((span.span_context.trace_flags().to_u8(),))?;
    let trace_state =
        trace_state_to_python(py, span.span_context.trace_state(), trace_state_class)?;

    // Get trace_id and span_id as integers
    let trace_id = trace_id_to_u128(span.span_context.trace_id());
    let span_id = span_id_to_u64(span.span_context.span_id());
    let parent_span_id = span_id_to_u64(span.parent_span_id);

    // Create SpanContext
    let context = span_context_class.call1((
        trace_id,
        span_id,
        false, // is_remote
        &trace_flags,
        &trace_state,
    ))?;

    // Create parent SpanContext if present
    let parent: Option<Bound<'_, PyAny>> = if parent_span_id != 0 {
        Some(span_context_class.call1((
            trace_id,
            parent_span_id,
            false,
            &trace_flags,
            &trace_state,
        ))?)
    } else {
        None
    };

    // Create Resource from Rust Resource
    let resource_attrs = PyDict::new(py);
    for kv in rust_resource.iter() {
        resource_attrs.set_item(kv.0.as_str(), value_to_pyobject(py, kv.1)?)?;
    }
    let resource = resource_class.call_method1("create", (resource_attrs,))?;

    // Create InstrumentationScope
    let scope_name = span.instrumentation_scope.name();
    let scope_version = span.instrumentation_scope.version();
    let scope = instrumentation_scope_class.call1((scope_name, scope_version))?;

    // Create Status
    let (status_code, status_description): (Bound<'_, PyAny>, Option<&str>) = match &span.status {
        opentelemetry::trace::Status::Unset => (status_code_class.getattr("UNSET")?, None),
        opentelemetry::trace::Status::Ok => (status_code_class.getattr("OK")?, None),
        opentelemetry::trace::Status::Error { description } => (
            status_code_class.getattr("ERROR")?,
            Some(description.as_ref()),
        ),
    };
    let status = status_class.call1((&status_code, status_description))?;

    // Create SpanKind
    let kind = match span.span_kind {
        SpanKind::Internal => span_kind_class.getattr("INTERNAL")?,
        SpanKind::Server => span_kind_class.getattr("SERVER")?,
        SpanKind::Client => span_kind_class.getattr("CLIENT")?,
        SpanKind::Producer => span_kind_class.getattr("PRODUCER")?,
        SpanKind::Consumer => span_kind_class.getattr("CONSUMER")?,
    };

    // Create attributes dict
    let attributes = PyDict::new(py);
    for kv in &span.attributes {
        attributes.set_item(kv.key.as_str(), value_to_pyobject(py, &kv.value)?)?;
    }

    // Create events list
    let events = PyList::empty(py);
    for event in span.events.iter() {
        let event_attrs = PyDict::new(py);
        for kv in event.attributes.iter() {
            event_attrs.set_item(kv.key.as_str(), value_to_pyobject(py, &kv.value)?)?;
        }
        let py_event = event_class.call1((
            event.name.as_ref(),
            event_attrs,
            system_time_to_nanos(event.timestamp),
        ))?;
        events.append(py_event)?;
    }

    // Create ReadableSpan with keyword arguments
    let kwargs = PyDict::new(py);
    kwargs.set_item("name", span.name.as_ref())?;
    kwargs.set_item("context", context)?;
    kwargs.set_item("parent", parent)?;
    kwargs.set_item("resource", resource)?;
    kwargs.set_item("attributes", attributes)?;
    kwargs.set_item("events", events)?;
    kwargs.set_item("links", PyList::empty(py))?;
    kwargs.set_item("kind", kind)?;
    kwargs.set_item("status", status)?;
    kwargs.set_item("start_time", system_time_to_nanos(span.start_time))?;
    kwargs.set_item("end_time", system_time_to_nanos(span.end_time))?;
    kwargs.set_item("instrumentation_scope", scope)?;

    let readable_span = readable_span_class.call((), Some(&kwargs))?;
    Ok(readable_span.unbind())
}

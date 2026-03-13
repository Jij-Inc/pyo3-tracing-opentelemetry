//! PyO3 OpenTelemetry Tracing Exporter
//!
//! This crate provides the infrastructure to forward Rust tracing spans
//! to Python's OpenTelemetry exporters by directly constructing ReadableSpan.
//!
//! # Overview
//!
//! When building Python extensions with PyO3 that use Rust's `tracing` crate,
//! you often want the spans to be exported through Python's OpenTelemetry SDK.
//! This crate bridges that gap by:
//!
//! 1. Converting Rust `SpanData` to Python `ReadableSpan` objects
//! 2. Forwarding spans to Python's configured `SpanProcessor`s
//! 3. Propagating trace context between Python and Rust
//!
//! # Usage
//!
//! ```rust,ignore
//! use pyo3::prelude::*;
//! use pyo3_tracing_opentelemetry::attach_parent_context_from_python;
//!
//! #[pyfunction]
//! fn my_traced_function(py: Python) -> PyResult<()> {
//!     // This ensures tracing is initialized and attaches Python's trace context
//!     let _guard = attach_parent_context_from_python(py);
//!
//!     // Your traced code here - spans will be forwarded to Python's exporters
//!     tracing::info_span!("my_operation").in_scope(|| {
//!         // ...
//!     });
//!
//!     Ok(())
//! }
//! ```

// Re-export ContextGuard since it's part of our public API return types
pub use opentelemetry::ContextGuard;

use std::{collections::HashMap, sync::OnceLock, time::SystemTime};

use anyhow::Result;
use futures_util::future::BoxFuture;
use opentelemetry::{
    Context, global,
    propagation::TextMapPropagator,
    trace::{SpanKind, TraceContextExt, TracerProvider as _},
};
use opentelemetry_sdk::{
    Resource,
    error::OTelSdkResult,
    propagation::TraceContextPropagator,
    trace::{SdkTracerProvider, SimpleSpanProcessor, SpanData, SpanExporter as OTelSpanExporter},
};
use pyo3::{
    prelude::*,
    sync::PyOnceLock,
    types::{PyDict, PyList},
};
use tracing_opentelemetry::OpenTelemetryLayer;
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

// ============================================================================
// Python Class Cache (to avoid repeated imports)
// ============================================================================

/// Cached Python classes for span conversion.
/// Using PyOnceLock to cache imports across calls.
struct PythonClasses {
    span_context_class: Py<PyAny>,
    span_kind_class: Py<PyAny>,
    trace_flags_class: Py<PyAny>,
    trace_state_class: Py<PyAny>,
    readable_span_class: Py<PyAny>,
    event_class: Py<PyAny>,
    resource_class: Py<PyAny>,
    instrumentation_scope_class: Py<PyAny>,
    status_class: Py<PyAny>,
    status_code_class: Py<PyAny>,
}

static PYTHON_CLASSES: PyOnceLock<PythonClasses> = PyOnceLock::new();

fn get_python_classes(py: Python<'_>) -> PyResult<&PythonClasses> {
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
// Tracing Infrastructure
// ============================================================================

/// Python span processor-based exporter that forwards spans to Python's OpenTelemetry
/// by directly constructing ReadableSpan objects.
struct PySpanExporter {
    /// Python list of span processors (from TracerProvider.span_processors)
    span_processors: Py<PyAny>,
    /// Resource attributes to include in spans
    resource: Resource,
}

impl std::fmt::Debug for PySpanExporter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PySpanExporter").finish()
    }
}

impl OTelSpanExporter for PySpanExporter {
    fn export(&mut self, batch: Vec<SpanData>) -> BoxFuture<'static, OTelSdkResult> {
        let mut errors: Vec<String> = Vec::new();

        Python::attach(|py| {
            for span_data in batch {
                // Convert SpanData directly to Python ReadableSpan
                match span_data_to_readable_span(py, &span_data, &self.resource) {
                    Ok(readable_span) => {
                        // Iterate over all span processors and call on_end on each
                        match self.span_processors.bind(py).try_iter() {
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

        if errors.is_empty() {
            Box::pin(std::future::ready(Ok(())))
        } else {
            let msg = errors.join("; ");
            Box::pin(std::future::ready(Err(
                opentelemetry_sdk::error::OTelSdkError::InternalFailure(msg),
            )))
        }
    }
}

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
fn span_data_to_readable_span(
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

static TRACING_INITIALIZED: OnceLock<()> = OnceLock::new();

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

/// Get the span processors from Python's TracerProvider if available.
///
/// Note: This uses internal attributes (`_active_span_processor._span_processors`)
/// because the OpenTelemetry Python SDK does not expose a public API to access
/// registered span processors. This is the same approach used by other integrations.
fn get_span_processor_from_python(py: Python) -> Option<Py<PyAny>> {
    let trace = py.import("opentelemetry.trace").ok()?;
    let sdk_trace = py.import("opentelemetry.sdk.trace").ok()?;
    let tracer_provider_class = sdk_trace.getattr("TracerProvider").ok()?;

    let provider = trace.call_method0("get_tracer_provider").ok()?;

    // Check if it's an SDK TracerProvider
    if !provider.is_instance(&tracer_provider_class).ok()? {
        return None;
    }

    // Access _active_span_processor (SynchronousMultiSpanProcessor or ConcurrentMultiSpanProcessor)
    // then get _span_processors tuple from it
    let active_processor = provider.getattr("_active_span_processor").ok()?;
    let span_processors = active_processor.getattr("_span_processors").ok()?;

    // Check if there are any span processors configured
    let len: usize = span_processors.len().ok()?;
    if len == 0 {
        return None;
    }

    // Return the span_processors tuple - we'll iterate over it in export
    Some(span_processors.unbind())
}

/// Configuration for initializing tracing.
#[derive(Debug, Clone)]
pub struct TracingConfig {
    /// Service name to use in the resource (for OpenTelemetry backends).
    pub service_name: String,
    /// Tracer name (must be a static string for OpenTelemetry requirements).
    pub tracer_name: &'static str,
}

impl Default for TracingConfig {
    fn default() -> Self {
        Self {
            service_name: "pyo3-app".to_string(),
            tracer_name: "pyo3-app",
        }
    }
}

/// Internal function to initialize tracing.
fn init_tracing_internal(config: &TracingConfig, span_processors: Option<Py<PyAny>>) -> Result<()> {
    TRACING_INITIALIZED.get_or_init(|| {
        // Create Resource for the TracerProvider
        let resource = Resource::builder()
            .with_service_name(config.service_name.clone())
            .build();

        // Create TracerProvider with optional exporter
        let provider_builder = SdkTracerProvider::builder().with_resource(resource.clone());

        let provider = if let Some(processors) = span_processors {
            // Use PySpanExporter to forward spans to Python's span processors
            let exporter = PySpanExporter {
                span_processors: processors,
                resource,
            };
            provider_builder
                .with_span_processor(SimpleSpanProcessor::new(Box::new(exporter)))
                .build()
        } else {
            // No exporter - spans are created but not exported
            provider_builder.build()
        };

        // Set as global provider
        global::set_tracer_provider(provider.clone());

        // Create OpenTelemetry layer for tracing
        let otel_layer = OpenTelemetryLayer::new(provider.tracer(config.tracer_name));

        // Initialize tracing subscriber with OpenTelemetry layer.
        // Use try_init() to avoid panic if already initialized (e.g., by another library).
        // If initialization fails, log a warning so embedding applications know they need
        // to integrate the OpenTelemetry layer into their own subscriber setup.
        if let Err(e) = tracing_subscriber::registry()
            .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
            .with(otel_layer)
            .try_init()
        {
            eprintln!(
                "pyo3-tracing-otel-exporter: failed to initialize tracing subscriber: {e}. \
                 If you embed this into an application with its own tracing, \
                 please add the OpenTelemetry layer to your existing subscriber."
            );
        }
    });
    Ok(())
}

/// Ensure that tracing is initialized (lazy initialization).
///
/// This function checks if Python's TracerProvider is an SDK TracerProvider
/// with a span processor, and initializes Rust-side tracing accordingly.
/// This is called automatically when `attach_parent_context_from_python` is invoked.
///
/// Note: Tracing is only initialized when a span processor is available.
/// This allows users to configure Python tracing after importing the library
/// but before calling traced functions.
pub fn ensure_tracing_initialized(py: Python) {
    ensure_tracing_initialized_with_config(py, &TracingConfig::default())
}

/// Ensure that tracing is initialized with custom configuration.
///
/// This function checks if Python's TracerProvider is an SDK TracerProvider
/// with a span processor, and initializes Rust-side tracing accordingly.
///
/// Note: Tracing is only initialized when a span processor is available.
/// This allows users to configure Python tracing after importing the library
/// but before calling traced functions.
pub fn ensure_tracing_initialized_with_config(py: Python, config: &TracingConfig) {
    // Only initialize once
    if TRACING_INITIALIZED.get().is_some() {
        return;
    }

    // Try to get the span processor from Python's TracerProvider.
    // Only initialize tracing when a processor is actually available,
    // so that tracing can be enabled later once Python tracing is configured.
    let processor = get_span_processor_from_python(py);
    if processor.is_some() {
        let _ = init_tracing_internal(config, processor);
    }
}

/// Attach parent context from Python's OpenTelemetry if available.
///
/// Returns a guard that will detach the context when dropped.
/// This function also ensures that tracing is initialized before attaching context.
///
/// # Example
///
/// ```rust,ignore
/// use pyo3::prelude::*;
/// use pyo3_tracing_opentelemetry::attach_parent_context_from_python;
///
/// #[pyfunction]
/// fn my_traced_function(py: Python) -> PyResult<()> {
///     let _guard = attach_parent_context_from_python(py);
///
///     // Your traced code here
///     tracing::info_span!("operation").in_scope(|| {
///         // ...
///     });
///
///     Ok(())
/// }
/// ```
pub fn attach_parent_context_from_python(py: Python) -> Option<opentelemetry::ContextGuard> {
    attach_parent_context_from_python_with_config(py, &TracingConfig::default())
}

/// Attach parent context from Python's OpenTelemetry with custom configuration.
///
/// Returns a guard that will detach the context when dropped.
/// This function also ensures that tracing is initialized before attaching context.
pub fn attach_parent_context_from_python_with_config(
    py: Python,
    config: &TracingConfig,
) -> Option<opentelemetry::ContextGuard> {
    // Ensure tracing is initialized before trying to attach context
    ensure_tracing_initialized_with_config(py, config);

    get_trace_headers_from_python(py)
        .and_then(|headers| extract_context_from_headers(&headers))
        .map(|ctx| ctx.attach())
}

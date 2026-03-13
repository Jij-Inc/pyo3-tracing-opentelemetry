//! Tracing initialization.

use std::sync::OnceLock;

use opentelemetry::{global, trace::TracerProvider as _};
use opentelemetry_sdk::{
    Resource,
    trace::{SdkTracerProvider, SimpleSpanProcessor},
};
use pyo3::{Py, prelude::*};
use tracing_opentelemetry::OpenTelemetryLayer;
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

use crate::export::PySpanExporter;

/// Stores the configuration used for initialization.
/// - `None`: No span processor was available in Python, OTel export is disabled.
/// - `Some(config)`: Initialized with the given configuration.
static TRACING_CONFIG: OnceLock<Option<TracingBridge>> = OnceLock::new();

/// Bridge between Python OpenTelemetry and Rust tracing.
///
/// This struct holds the configuration needed to initialize the tracing infrastructure
/// and provides methods for context propagation between Python and Rust.
#[derive(Debug, Clone)]
pub struct TracingBridge {
    /// Service name to use in the resource (for OpenTelemetry backends).
    pub service_name: &'static str,
    /// Tracer name (instrumentation scope name).
    pub tracer_name: &'static str,
}

impl TracingBridge {
    /// Create a new TracingBridge with the given name for both service and tracer.
    pub const fn new(name: &'static str) -> Self {
        Self {
            service_name: name,
            tracer_name: name,
        }
    }

    /// Initialize tracing with this configuration.
    ///
    /// Returns `Some(&config)` if OTel export was set up successfully,
    /// `None` if Python doesn't have a span processor configured.
    ///
    /// If tracing was already initialized with a different configuration,
    /// a warning is logged and the original configuration is returned.
    ///
    /// Note: Initialization happens only once per process.
    pub fn initialize(&self, py: Python) -> Option<&'static TracingBridge> {
        let result = initialize_tracing(py, self);

        // Warn if already initialized with different config
        if let Some(stored) = result {
            if stored.service_name != self.service_name || stored.tracer_name != self.tracer_name {
                tracing::warn!(
                    "pyo3-tracing-opentelemetry: tracing already initialized with \
                     service_name={:?}, tracer_name={:?}. \
                     Ignoring new config with service_name={:?}, tracer_name={:?}.",
                    stored.service_name,
                    stored.tracer_name,
                    self.service_name,
                    self.tracer_name
                );
            }
        }

        result
    }

    /// Attach parent context from Python's OpenTelemetry if available.
    ///
    /// Returns a guard that will detach the context when dropped.
    /// This function also initializes tracing if not already done.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use pyo3::prelude::*;
    /// use pyo3_tracing_opentelemetry::TracingBridge;
    ///
    /// const TRACING: TracingBridge = TracingBridge::new("my-service");
    ///
    /// #[pyfunction]
    /// fn my_traced_function(py: Python) -> PyResult<()> {
    ///     let _guard = TRACING.attach_parent_context(py);
    ///
    ///     // Your traced code here
    ///     tracing::info_span!("operation").in_scope(|| {
    ///         // ...
    ///     });
    ///
    ///     Ok(())
    /// }
    /// ```
    pub fn attach_parent_context(&self, py: Python) -> Option<opentelemetry::ContextGuard> {
        use crate::context::{extract_context_from_headers, get_trace_headers_from_python};

        // Initialize tracing (no-op if already done)
        self.initialize(py);

        get_trace_headers_from_python(py)
            .and_then(|headers| extract_context_from_headers(&headers))
            .map(|ctx| ctx.attach())
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

/// Initialize tracing with Python's OpenTelemetry configuration.
///
/// Returns `Some(&config)` if OTel export was set up successfully,
/// `None` if Python doesn't have a span processor configured (OTel export disabled).
///
/// This is a normal case - when Python-side OTel is not configured,
/// tracing export is simply disabled without any error.
///
/// Note: Initialization happens only once per process. Subsequent calls
/// return the cached result without re-checking Python's configuration.
pub(crate) fn initialize_tracing(py: Python, config: &TracingBridge) -> Option<&'static TracingBridge> {
    TRACING_CONFIG
        .get_or_init(|| {
            // Get span processors from Python (only during initialization)
            let span_processors = get_span_processor_from_python(py)?;

            // Create Resource for the TracerProvider
            let resource = Resource::builder()
                .with_service_name(config.service_name.to_string())
                .build();

            // Use PySpanExporter to forward spans to Python's span processors
            let exporter = PySpanExporter {
                span_processors,
                resource: resource.clone(),
            };

            let provider = SdkTracerProvider::builder()
                .with_resource(resource)
                .with_span_processor(SimpleSpanProcessor::new(Box::new(exporter)))
                .build();

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
                    "pyo3-tracing-opentelemetry: failed to initialize tracing subscriber: {e}. \
                     If you embed this into an application with its own tracing, \
                     please add the OpenTelemetry layer to your existing subscriber."
                );
            }

            Some(config.clone())
        })
        .as_ref()
}

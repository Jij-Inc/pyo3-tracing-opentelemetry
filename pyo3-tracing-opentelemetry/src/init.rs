//! Tracing initialization.

use std::sync::OnceLock;

use anyhow::Result;
use opentelemetry::{global, trace::TracerProvider as _};
use opentelemetry_sdk::{
    Resource,
    trace::{SdkTracerProvider, SimpleSpanProcessor},
};
use pyo3::{Py, prelude::*};
use tracing_opentelemetry::OpenTelemetryLayer;
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

use crate::export::PySpanExporter;

static TRACING_INITIALIZED: OnceLock<()> = OnceLock::new();

/// Configuration for initializing tracing.
#[derive(Debug, Clone)]
pub struct TracingConfig {
    /// Service name to use in the resource (for OpenTelemetry backends).
    pub service_name: &'static str,
    /// Tracer name (must be a static string for OpenTelemetry requirements).
    pub tracer_name: &'static str,
}

impl TracingConfig {
    /// Create a new TracingConfig with the given service name and tracer name.
    pub const fn new(service_name: &'static str, tracer_name: &'static str) -> Self {
        Self {
            service_name,
            tracer_name,
        }
    }

    /// Ensure that tracing is initialized with this configuration.
    ///
    /// This function checks if Python's TracerProvider is an SDK TracerProvider
    /// with a span processor, and initializes Rust-side tracing accordingly.
    ///
    /// Note: Tracing is only initialized when a span processor is available.
    /// This allows users to configure Python tracing after importing the library
    /// but before calling traced functions.
    pub fn ensure_initialized(&self, py: Python) {
        ensure_tracing_initialized_with_config(py, self)
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
    /// use pyo3_tracing_opentelemetry::TracingConfig;
    ///
    /// const TRACER: TracingConfig = TracingConfig::new("my-service", "my-service");
    ///
    /// #[pyfunction]
    /// fn my_traced_function(py: Python) -> PyResult<()> {
    ///     let _guard = TRACER.attach_parent_context(py);
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

        // Ensure tracing is initialized before trying to attach context
        self.ensure_initialized(py);

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

/// Internal function to initialize tracing.
fn init_tracing_internal(config: &TracingConfig, span_processors: Option<Py<PyAny>>) -> Result<()> {
    TRACING_INITIALIZED.get_or_init(|| {
        // Create Resource for the TracerProvider
        let resource = Resource::builder()
            .with_service_name(config.service_name.to_string())
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
                "pyo3-tracing-opentelemetry: failed to initialize tracing subscriber: {e}. \
                 If you embed this into an application with its own tracing, \
                 please add the OpenTelemetry layer to your existing subscriber."
            );
        }
    });
    Ok(())
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

//! Tracing initialization.

use std::sync::OnceLock;

use opentelemetry::trace::TracerProvider as _;
use opentelemetry_sdk::{
    Resource,
    trace::{SdkTracerProvider, SimpleSpanProcessor},
};
use pyo3::prelude::*;
use tracing_opentelemetry::OpenTelemetryLayer;
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

use crate::export::PySpanExporter;

/// Result of tracing initialization.
#[derive(Debug, Clone)]
pub enum TracingInitResult {
    /// OTel export is active with the given configuration.
    Active(TracingBridge),
    /// Python doesn't have a `TracerProvider` with span processors configured.
    ///
    /// As of v0.1.3 this variant is no longer produced: the bridge installs
    /// its Rust subscriber unconditionally and resolves the destination
    /// Python `TracerProvider` dynamically on each span export. If Python has
    /// no provider when a span is exported, the span is dropped silently. The
    /// variant is kept for backward compatibility so downstream `match` arms
    /// continue to compile.
    PythonOtelNotConfigured,
    /// Tracing subscriber failed to initialize (already initialized by another library).
    SubscriberAlreadyInitialized,
}

impl TracingInitResult {
    /// Returns `true` if OTel export is active.
    pub fn is_active(&self) -> bool {
        matches!(self, TracingInitResult::Active(_))
    }

    /// Returns the active configuration if OTel export is active.
    pub fn config(&self) -> Option<&TracingBridge> {
        match self {
            TracingInitResult::Active(config) => Some(config),
            _ => None,
        }
    }
}

/// Stores the initialization result.
static TRACING_INIT_RESULT: OnceLock<TracingInitResult> = OnceLock::new();

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
    /// Returns the initialization result indicating whether OTel export is active
    /// and why it might not be.
    ///
    /// If tracing was already initialized with a different configuration,
    /// a warning is logged and the original result is returned.
    ///
    /// Note: Initialization happens only once per process.
    pub fn initialize(&self, py: Python) -> &'static TracingInitResult {
        let result = initialize_tracing(py, self);

        // Warn if already initialized with different config
        if let Some(stored) = result.config()
            && (stored.service_name != self.service_name || stored.tracer_name != self.tracer_name)
        {
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

/// Initialize tracing with Python's OpenTelemetry configuration.
///
/// Returns `&'static TracingInitResult` indicating the outcome:
/// - `Active(config)`: OTel export is active with the given configuration.
/// - `SubscriberAlreadyInitialized`: Tracing subscriber was already initialized by another library.
///
/// # Important
///
/// Initialization happens only once per process, and the result is cached.
///
/// Unlike earlier versions, this function **does not** check whether Python's
/// `TracerProvider` is already configured: it installs the Rust subscriber
/// unconditionally. The destination Python span processors are resolved
/// dynamically on every span export (see [`crate::export::PySpanExporter`]),
/// so Python's `TracerProvider` can be configured, swapped, or torn down at
/// any point during the process lifetime and subsequent Rust spans will
/// follow. If no provider is configured when a span is exported, that span is
/// dropped silently.
pub(crate) fn initialize_tracing(
    _py: Python,
    config: &TracingBridge,
) -> &'static TracingInitResult {
    TRACING_INIT_RESULT.get_or_init(|| {
        // Create Resource for the TracerProvider
        let resource = Resource::builder()
            .with_service_name(config.service_name.to_string())
            .build();

        // Use PySpanExporter to forward spans to Python's span processors.
        // It resolves `trace.get_tracer_provider()` dynamically on each call
        // to `export()`, so the target is not locked in at init time.
        let exporter = PySpanExporter {
            resource: resource.clone(),
        };

        let provider = SdkTracerProvider::builder()
            .with_resource(resource)
            .with_span_processor(SimpleSpanProcessor::new(Box::new(exporter)))
            .build();

        // Create the OpenTelemetry layer. The tracer clones internal Arcs
        // from the provider, so the pipeline stays alive as long as the
        // subscriber (and thus the layer) does.
        //
        // We intentionally do **not** call
        // `opentelemetry::global::set_tracer_provider(...)` here. The bridge
        // routes Rust `tracing` events to Python through the layer we install
        // below and does not read from OpenTelemetry's Rust-side global. If
        // the host application has installed its own global tracer provider
        // (either directly or via another library), overwriting it from here
        // would clobber its telemetry setup for uses like
        // `opentelemetry::global::tracer(...)`.
        let otel_layer = OpenTelemetryLayer::new(provider.tracer(config.tracer_name));

        // Initialize tracing subscriber with OpenTelemetry layer.
        // Use try_init() to avoid panic if already initialized (e.g., by another library).
        if tracing_subscriber::registry()
            .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
            .with(otel_layer)
            .try_init()
            .is_err()
        {
            // Subscriber already initialized by another library.
            // OTel export won't work unless the embedding app adds the layer manually.
            return TracingInitResult::SubscriberAlreadyInitialized;
        }

        TracingInitResult::Active(config.clone())
    })
}

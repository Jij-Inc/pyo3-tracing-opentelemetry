# pyo3-tracing-opentelemetry

Bridge Rust `tracing` and Python OpenTelemetry for seamless distributed tracing across PyO3 FFI boundaries.

## Overview

When building Python extensions with PyO3 that use Rust's `tracing` crate, you often want the spans to be exported through Python's OpenTelemetry SDK. This crate bridges that gap by:

1. Converting Rust `SpanData` to Python `ReadableSpan` objects
2. Forwarding spans to Python's configured `SpanProcessor`s
3. Propagating trace context between Python and Rust (W3C Trace Context format)

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
pyo3-tracing-opentelemetry = "0.1"
```

## Usage

See the [example crate](./example) for a complete working example.

### Rust side

```rust
use pyo3::prelude::*;
use pyo3_tracing_opentelemetry::TracingBridge;

// Create a tracing bridge as a module-level constant
const TRACING: TracingBridge = TracingBridge::new("my-service");

#[pyfunction]
fn my_traced_function(py: Python) -> PyResult<()> {
    // This ensures tracing is initialized and attaches Python's trace context
    let _guard = TRACING.attach_parent_context(py);

    // Your traced code here - spans will be forwarded to Python's exporters
    tracing::info_span!("my_operation").in_scope(|| {
        tracing::info!("doing work");
    });

    Ok(())
}
```

For advanced configuration (different service name and tracer name):

```rust
use pyo3::prelude::*;
use pyo3_tracing_opentelemetry::TracingBridge;

// Use struct literal for different service_name and tracer_name
const TRACING: TracingBridge = TracingBridge {
    service_name: "my-service",
    tracer_name: "my-tracer",
};

#[pyfunction]
fn my_traced_function(py: Python) -> PyResult<()> {
    let _guard = TRACING.attach_parent_context(py);
    // ...
    Ok(())
}
```

### Python side

```python
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor, ConsoleSpanExporter

# Set up Python OpenTelemetry
provider = TracerProvider()
provider.add_span_processor(SimpleSpanProcessor(ConsoleSpanExporter()))
trace.set_tracer_provider(provider)

# Create a parent span in Python
tracer = trace.get_tracer("my-python-app")
with tracer.start_as_current_span("python-parent"):
    # Call your Rust function - Rust spans will appear as children
    my_rust_module.my_traced_function()
```

## How it works

1. When `TracingBridge::attach_parent_context(py)` is called, the crate:
   - Checks if Python has an SDK `TracerProvider` with span processors configured
   - Initializes a Rust `tracing-subscriber` with an OpenTelemetry layer (only once per process)
   - Extracts the current trace context from Python using W3C Trace Context propagation
   - Attaches that context so Rust spans become children of the Python span

2. When Rust spans complete, the `PySpanExporter`:
   - Converts `SpanData` to Python `ReadableSpan` objects
   - Calls `on_end()` on each of Python's configured span processors
   - This allows spans to flow through to any Python exporter (Jaeger, OTLP, Console, etc.)

**Note**: Tracing is initialized once per process. If multiple `TracingBridge` instances with different configurations call `attach_parent_context`, the first one wins and subsequent configurations are ignored (with a warning logged).

## Requirements

- Rust 2024 edition
- PyO3 0.27+
- Python with `opentelemetry-sdk` installed

## License

MIT OR Apache-2.0

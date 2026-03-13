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
//! use pyo3_tracing_opentelemetry::TracingBridge;
//!
//! const TRACING: TracingBridge = TracingBridge::new("my-service");
//!
//! #[pyfunction]
//! fn my_traced_function(py: Python) -> PyResult<()> {
//!     // This ensures tracing is initialized and attaches Python's trace context
//!     let _guard = TRACING.attach_parent_context(py);
//!
//!     // Your traced code here - spans will be forwarded to Python's exporters
//!     tracing::info_span!("my_operation").in_scope(|| {
//!         // ...
//!     });
//!
//!     Ok(())
//! }
//! ```

mod context;
mod export;
mod init;

// Re-export ContextGuard since it's part of our public API return types
pub use opentelemetry::ContextGuard;

// Public API from context module
pub use context::{extract_context_from_headers, get_trace_headers_from_python};

// Public API from init module
pub use init::{TracingBridge, TracingInitResult};

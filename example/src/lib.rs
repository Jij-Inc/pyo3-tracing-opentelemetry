//! Example PyO3 module demonstrating pyo3-tracing-opentelemetry usage.

use pyo3::prelude::*;
use pyo3_tracing_opentelemetry::TracingConfig;

const TRACER: TracingConfig = TracingConfig::new("example-module", "example-module");

/// A simple traced function that does some work.
#[pyfunction]
#[cfg_attr(feature = "stub_gen", pyo3_stub_gen::derive::gen_stub_pyfunction)]
fn traced_function(py: Python<'_>) -> PyResult<i32> {
    let _guard = TRACER.attach_parent_context(py);

    Ok(traced_function_inner())
}

#[tracing::instrument]
fn traced_function_inner() -> i32 {
    tracing::info!("Starting traced_function");

    let result = do_work();

    tracing::info!(result = result, "Finished traced_function");

    result
}

#[tracing::instrument]
fn do_work() -> i32 {
    tracing::info!("Doing some work");
    42
}

/// A traced function that calls nested spans.
#[pyfunction]
#[cfg_attr(feature = "stub_gen", pyo3_stub_gen::derive::gen_stub_pyfunction)]
fn nested_spans(py: Python<'_>) -> PyResult<()> {
    let _guard = TRACER.attach_parent_context(py);

    tracing::info_span!("outer_span").in_scope(|| {
        tracing::info!("In outer span");

        tracing::info_span!("inner_span").in_scope(|| {
            tracing::info!("In inner span");
        });
    });

    Ok(())
}

/// A traced function that emits events with attributes.
#[pyfunction]
#[cfg_attr(feature = "stub_gen", pyo3_stub_gen::derive::gen_stub_pyfunction)]
fn traced_with_attributes(py: Python<'_>, name: String, count: i32) -> PyResult<()> {
    let _guard = TRACER.attach_parent_context(py);

    tracing::info_span!("process_request", %name, %count).in_scope(|| {
        tracing::info!(name = %name, count = count, "Processing request");

        for i in 0..count {
            tracing::debug!(iteration = i, "Processing iteration");
        }

        tracing::info!("Request processed successfully");
    });

    Ok(())
}

#[pymodule]
fn example_module(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(traced_function, m)?)?;
    m.add_function(wrap_pyfunction!(nested_spans, m)?)?;
    m.add_function(wrap_pyfunction!(traced_with_attributes, m)?)?;
    Ok(())
}

#[cfg(feature = "stub_gen")]
pyo3_stub_gen::define_stub_info_gatherer!(stub_info);

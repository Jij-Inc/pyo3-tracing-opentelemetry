use pyo3_stub_gen::Result;

fn main() -> Result<()> {
    let stub = example_module::stub_info()?;
    stub.generate()?;
    Ok(())
}

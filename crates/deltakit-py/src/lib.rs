use anyhow::Result;
use deltakit_core as core;
use pyo3::prelude::*;
use shard_planner as sp;

#[pyclass]
#[derive(Clone)]
struct PyShardFile { #[pyo3(get)] path: String, #[pyo3(get)] bytes: i64, #[pyo3(get)] rows: u64 }

#[pyclass]
#[derive(Clone)]
struct PyShard { #[pyo3(get)] id: u32, #[pyo3(get)] bytes: i64, #[pyo3(get)] rows: u64, #[pyo3(get)] files: Vec<PyShardFile> }

#[pyfunction]
fn shard_manifest(py: Python<'_>, uri: String, version: i64, shards: u32, balance: Option<String>, by: Option<Vec<String>>, sticky_by: Option<Vec<String>>, row_group_aware: Option<bool>) -> PyResult<Vec<PyShard>> {
    let mode = match balance.as_deref() { Some("rows") => sp::BalanceMode::Rows, _ => sp::BalanceMode::Bytes };
    let opts = sp::ShardOptions { by: by.unwrap_or_default(), sticky_by: sticky_by.unwrap_or_default(), max_files_per_shard: None, balance: mode, row_group_aware: row_group_aware.unwrap_or(false) };
    py.allow_threads(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let res: Result<Vec<sp::Shard>> = rt.block_on(async move {
            let h = core::load_table(&uri).await?;
            sp::plan_shards(&h, version, shards, opts).await
        });
        match res {
            Ok(v) => Ok(v.into_iter().map(|s| PyShard { id: s.id, bytes: s.bytes, rows: s.rows, files: s.files.into_iter().map(|f| PyShardFile { path: f.path, bytes: f.bytes, rows: f.approx_rows }).collect() }).collect()),
            Err(e) => Err(pyo3::exceptions::PyRuntimeError::new_err(e.to_string())),
        }
    })
}

#[pymodule]
fn deltakit_py(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(shard_manifest, m)?)?;
    Ok(())
}



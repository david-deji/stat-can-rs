use crate::StatCanClient;
use pyo3::prelude::*;
use pyo3_polars::PyDataFrame;

#[pyclass]
pub struct PyStatCanClient {
    client: StatCanClient,
    rt: tokio::runtime::Runtime,
}

#[pymethods]
impl PyStatCanClient {
    #[new]
    fn new() -> PyResult<Self> {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let client = match StatCanClient::new() {
            Ok(c) => c,
            Err(e) => return Err(pyo3::exceptions::PyRuntimeError::new_err(e.to_string())),
        };
        Ok(PyStatCanClient { client, rt })
    }

    fn fetch_full_table(&self, pid: &str) -> PyResult<PyDataFrame> {
        self.rt.block_on(async {
            match self.client.fetch_full_table(pid).await {
                Ok(wrapper) => Ok(PyDataFrame(wrapper.into_polars())),
                Err(e) => Err(pyo3::exceptions::PyRuntimeError::new_err(e.to_string())),
            }
        })
    }

    fn get_cube_metadata(&self, pid: &str) -> PyResult<String> {
        self.rt.block_on(async {
            match self.client.get_cube_metadata(pid).await {
                Ok(meta) => serde_json::to_string(&meta)
                    .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string())),
                Err(e) => Err(pyo3::exceptions::PyRuntimeError::new_err(e.to_string())),
            }
        })
    }

    fn get_all_cubes_list_lite(&self) -> PyResult<String> {
        self.rt.block_on(async {
            match self.client.get_all_cubes_list_lite().await {
                Ok(cubes) => serde_json::to_string(&cubes)
                    .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string())),
                Err(e) => Err(pyo3::exceptions::PyRuntimeError::new_err(e.to_string())),
            }
        })
    }

    fn get_data_from_cube_pid(&self, pid: &str, coords: Vec<String>) -> PyResult<String> {
        self.rt.block_on(async {
            match self.client.get_data_from_coords(pid, coords, 1).await {
                Ok(data) => serde_json::to_string(&data)
                    .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string())),
                Err(e) => Err(pyo3::exceptions::PyRuntimeError::new_err(e.to_string())),
            }
        })
    }
}

#[pymodule]
fn statcan_rs(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<PyStatCanClient>()?;
    Ok(())
}

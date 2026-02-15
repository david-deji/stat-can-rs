use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::time::Instant;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StatCanErrorResponse {
    pub status: Option<String>,
    pub object: Option<String>,
    pub message: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct VectorDataResponse {
    pub status: String,
    pub object: Option<VectorDataObject>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct VectorDataObject {
    pub vector_id: i64,
    pub coordinate: String,
}

fn main() {
    let success_obj = json!([{
        "status": "SUCCESS",
        "object": {
            "vectorId": 12345,
            "coordinate": "1.1.1.0.0.0.0.0.0.0",
            "vectorDataPoint": []
        }
    }]);

    // Error case usually comes as an object
    let error_obj = json!({
        "status": "FAILED",
        "message": "Some error occurred"
    });

    let iterations = 100_000;

    println!("Benchmarking {} iterations...", iterations);

    // --- Scenario A: Success (Array) ---
    // Note: Array skips the is_object() block entirely in both cases, so diff should be minimal or zero.
    // The improvement comes when body IS an object (which happens on error, or potentially single item success?)

    // Let's test "Error Case" first as that hits the optimized path.
    println!("\n--- Scenario: Error Object ---");

    let start = Instant::now();
    for _ in 0..iterations {
        let body = error_obj.clone();
        let _ = run_original(body);
    }
    let duration_original = start.elapsed();

    let start = Instant::now();
    for _ in 0..iterations {
        let body = error_obj.clone();
        let _ = run_optimized(body);
    }
    let duration_optimized = start.elapsed();

    println!("Original:  {:.2?}", duration_original);
    println!("Optimized: {:.2?}", duration_optimized);

    let diff = duration_original.as_secs_f64() - duration_optimized.as_secs_f64();
    if diff > 0.0 {
        println!(
            "Improvement: {:.2}%",
            diff / duration_original.as_secs_f64() * 100.0
        );
    } else {
        println!(
            "Regression: {:.2}%",
            -diff / duration_original.as_secs_f64() * 100.0
        );
    }
}

// Mimic existing implementation
fn run_original(body: Value) -> Result<Vec<VectorDataResponse>, String> {
    if body.is_object() {
        // Clone 1 + Deserialize 1
        if let Ok(err_resp) = serde_json::from_value::<StatCanErrorResponse>(body.clone()) {
            let mut is_error = false;
            let mut status_msg = "FAILED".to_string();

            if let Some(s) = &err_resp.status {
                if s != "SUCCESS" {
                    is_error = true;
                    status_msg = s.clone();
                }
            } else if let Some(msg) = &err_resp.message {
                is_error = true;
                status_msg = msg.clone();
            }

            if is_error {
                return Err(status_msg);
            }
        }
    }

    // Clone 2 + Deserialize 2
    // We clone because in real code we use 'body' in map_err
    serde_json::from_value(body.clone()).map_err(|_| format!("Failed JSON body: {}", body))
}

fn run_optimized(body: Value) -> Result<Vec<VectorDataResponse>, String> {
    if body.is_object() {
        // Direct Inspection (No Clone, No Deserialize)
        let mut is_error = false;
        let mut status_msg = "FAILED".to_string();

        if let Some(s) = body.get("status").and_then(|v| v.as_str()) {
            if s != "SUCCESS" {
                is_error = true;
                status_msg = s.to_string();
            }
        } else if let Some(msg) = body.get("message").and_then(|v| v.as_str()) {
            // Original logic: else if let Some(msg) = &err_resp.message
            // Note: Original code checks message ONLY if status is NOT present (because of else if)
            // Wait, original:
            // if let Some(s) = &err_resp.status { if s!=SUCCESS { is_error=true } }
            // else if let Some(msg) = &err_resp.message { is_error=true }

            // This means:
            // 1. If status is present:
            //    - If "SUCCESS": is_error = false. Message is IGNORED.
            //    - If != "SUCCESS": is_error = true.
            // 2. If status is MISSING:
            //    - If message is present: is_error = true.
            //    - If message is missing: is_error = false.

            is_error = true;
            status_msg = msg.to_string();
        }

        if is_error {
            return Err(status_msg);
        }
    }

    // Clone 2 + Deserialize 2
    serde_json::from_value(body.clone()).map_err(|_| format!("Failed JSON body: {}", body))
}

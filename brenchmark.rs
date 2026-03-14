use std::collections::HashMap;
use std::time::Instant;

fn main() {
    let num_cols = 1000;
    let num_filters = 100;

    let columns: Vec<String> = (0..num_cols).map(|i| format!("Column_{}", i)).collect();
    let filter_pairs: Vec<(String, String)> = (0..num_filters)
        .map(|i| (format!("COLUMN_{}", i * 10), "value".to_string()))
        .collect();

    // Slow approach
    let start_slow = Instant::now();
    for _ in 0..100 {
        for (col_name, _col_val) in &filter_pairs {
            let col_lower = col_name.to_lowercase();
            let actual_col = columns
                .iter()
                .find(|c| c.to_lowercase() == col_lower)
                .map(|c| c.to_string());
            assert!(actual_col.is_some());
        }
    }
    let duration_slow = start_slow.elapsed();

    // Fast approach
    let start_fast = Instant::now();
    for _ in 0..100 {
        let col_map: HashMap<String, String> = columns
            .iter()
            .map(|c| (c.to_lowercase(), c.to_string()))
            .collect();
        for (col_name, _col_val) in &filter_pairs {
            let col_lower = col_name.to_lowercase();
            let actual_col = col_map.get(&col_lower).cloned();
            assert!(actual_col.is_some());
        }
    }
    let duration_fast = start_fast.elapsed();

    println!("Slow approach: {:?}", duration_slow);
    println!("Fast approach: {:?}", duration_fast);
}

use criterion::{criterion_group, criterion_main, Criterion};
use serde_json::{json, Value};
use std::hint::black_box;

fn format_csv_original(records: &[Value]) -> Result<String, String> {
    let mut wtr = csv::Writer::from_writer(Vec::new());
    let headers = if let Some(first) = records.first() {
        first
            .as_object()
            .map(|obj| obj.keys().cloned().collect::<Vec<String>>())
            .unwrap_or_default()
    } else {
        Vec::new()
    };

    if !headers.is_empty() {
        wtr.write_record(&headers).map_err(|e| e.to_string())?;

        for row in records {
            if let Some(obj) = row.as_object() {
                let vals: Vec<String> = headers
                    .iter()
                    .map(|k| {
                        obj.get(k)
                            .map(|v| {
                                if let Some(s) = v.as_str() {
                                    s.to_string()
                                } else {
                                    v.to_string()
                                }
                            })
                            .unwrap_or_default()
                    })
                    .collect();
                wtr.write_record(&vals).map_err(|e| e.to_string())?;
            }
        }
    }
    let buf = wtr.into_inner().map_err(|e| e.to_string())?;
    String::from_utf8(buf).map_err(|e| e.to_string())
}

fn format_csv_optimized(records: &[Value]) -> Result<String, String> {
    let mut wtr = csv::Writer::from_writer(Vec::new());

    let headers: Vec<&str> = if let Some(first) = records.first() {
        first
            .as_object()
            .map(|obj| obj.keys().map(|k| k.as_str()).collect())
            .unwrap_or_default()
    } else {
        Vec::new()
    };

    if !headers.is_empty() {
        wtr.write_record(&headers).map_err(|e| e.to_string())?;

        for row in records {
            if let Some(obj) = row.as_object() {
                for key in &headers {
                    if let Some(val) = obj.get(*key) {
                        if let Some(s) = val.as_str() {
                            wtr.write_field(s).map_err(|e| e.to_string())?;
                        } else {
                            wtr.write_field(val.to_string())
                                .map_err(|e| e.to_string())?;
                        }
                    } else {
                        wtr.write_field("").map_err(|e| e.to_string())?;
                    }
                }
                wtr.write_record(None::<&[u8]>).map_err(|e| e.to_string())?;
            }
        }
    }

    let buf = wtr.into_inner().map_err(|e| e.to_string())?;
    String::from_utf8(buf).map_err(|e| e.to_string())
}

fn criterion_benchmark(c: &mut Criterion) {
    let mut records = Vec::new();
    for i in 0..1000 {
        records.push(json!({
            "id": i,
            "name": format!("Name {}", i),
            "value": i * 10,
            "active": i % 2 == 0,
            "description": "Some longer string that we don't want to clone over and over again",
        }));
    }

    c.bench_function("csv_format_original", |b| {
        b.iter(|| format_csv_original(black_box(&records)))
    });

    c.bench_function("csv_format_optimized", |b| {
        b.iter(|| format_csv_optimized(black_box(&records)))
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);

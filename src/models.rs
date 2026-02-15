use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CubeListResponse {
    pub object: Option<Vec<Cube>>,
    pub status: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Cube {
    pub cube_title_en: String,
    pub cube_pid: Option<String>,
    #[serde(deserialize_with = "deserialize_int_or_string")]
    pub product_id: String,
}

fn deserialize_int_or_string<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum IntOrString {
        Int(i64),
        String(String),
    }

    match IntOrString::deserialize(deserializer)? {
        IntOrString::Int(v) => Ok(v.to_string()),
        IntOrString::String(v) => Ok(v),
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CubeMetadataResponse {
    pub object: Option<CubeMetadata>,
    pub status: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CubeMetadata {
    pub cube_title_en: String,
    pub product_id: String,
    pub dimension: Vec<Dimension>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Dimension {
    pub dimension_name_en: String,
    pub dimension_position_id: i32,
    pub member: Vec<Member>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Member {
    pub member_name_en: String,
    pub member_id: i32,
    pub classification_code: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DataResponse {
    pub object: Option<Vec<DataPoint>>,
    pub status: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DataPoint {
    pub vector_id: i64,
    pub coordinate: String,
    pub ref_date: String,
    pub value: Option<f64>,
    pub decimals: Option<i32>,
    pub scalar_factor_code: Option<i32>,
    pub symbol_code: Option<i32>,
    pub status_code: Option<i32>,
    pub security_level_code: Option<i32>,
    pub release_time: String,
    pub frequency_code: Option<i32>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FullTableResponse {
    pub object: Option<String>, // The URL
    pub status: String,
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
    pub vector_data_point: Vec<VectorPoint>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct VectorPoint {
    pub ref_per: String,
    pub value: Option<f64>,
    pub decimals: Option<i32>,
    pub scalar_factor_code: Option<i32>,
    pub symbol_code: Option<i32>,
    pub status_code: Option<i32>,
    pub security_level_code: Option<i32>,
    pub release_time: String,
    pub frequency_code: Option<i32>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StatCanErrorResponse {
    pub status: Option<String>,
    pub object: Option<String>,
    pub message: Option<String>,
}

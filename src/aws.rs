#![allow(dead_code, clippy::enum_variant_names)]

/// Definitions for AWS RDS Data Service API
/// Manually copy-pasted from the aws-sdk-rdsdata create, stripped of needless fields
/// and adapted for usage with serde.
use serde::{Deserialize, Serialize};
use std::vec::Vec;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecuteStatementInputDef {
    pub sql: Option<String>,
    pub database: Option<String>,
    pub schema: Option<String>,
    pub parameters: Option<Vec<SqlParameterDef>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SqlParameterDef {
    pub name: String,
    pub value: FieldDef,
    pub type_hint: Option<TypeHintDef>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum FieldDef {
    ArrayValue(ArrayValueDef),
    BlobValue(String),
    BooleanValue(bool),
    DoubleValue(f64),
    IsNull(bool),
    LongValue(i64),
    StringValue(String),
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ArrayValueDef {
    ArrayValues(Vec<ArrayValueDef>),
    BooleanValues(Vec<bool>),
    DoubleValues(Vec<f64>),
    LongValues(Vec<i64>),
    StringValues(Vec<String>),
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum TypeHintDef {
    #[serde(rename = "DATE")]
    Date,
    #[serde(rename = "DECIMAL")]
    Decimal,
    #[serde(rename = "JSON")]
    Json,
    #[serde(rename = "TIME")]
    Time,
    #[serde(rename = "TIMESTAMP")]
    Timestamp,
    #[serde(rename = "UUID")]
    Uuid,
}

#[derive(Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecuteStatementOutputDef {
    pub records: Option<Vec<Vec<FieldDef>>>,
    pub column_metadata: Option<Vec<ColumnMetadataDef>>,
    pub number_of_records_updated: i64,
    pub generated_fields: Option<Vec<FieldDef>>,
    pub formatted_records: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ColumnMetadataDef {
    pub name: Option<String>,
    pub r#type: i32,
    pub type_name: Option<String>,
    pub label: Option<String>,
    pub schema_name: Option<String>,
    pub table_name: Option<String>,
    pub is_auto_increment: bool,
    pub is_signed: bool,
    pub is_currency: bool,
    pub is_case_sensitive: bool,
    pub nullable: i32,
    pub precision: i32,
    pub scale: i32,
    pub array_base_column_type: i32,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BatchExecuteStatementInputDef {
    pub sql: Option<String>,
    pub database: Option<String>,
    pub schema: Option<String>,
    pub parameter_sets: Option<Vec<Vec<SqlParameterDef>>>,
}

#[derive(Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BatchExecuteStatementOutputDef {
    pub update_results: Option<Vec<UpdateResultDef>>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateResultDef {
    pub generated_fields: Option<Vec<FieldDef>>,
}

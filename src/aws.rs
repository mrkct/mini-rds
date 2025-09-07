#![allow(dead_code, clippy::enum_variant_names)]

use base64::Engine;
use chrono::{DateTime, Utc};
use log::{error, info};
use serde::{Deserialize, Serialize};
use sqlx::{
    Column, Row, TypeInfo,
    mysql::{MySqlColumn, MySqlRow},
};
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

pub fn try_row_to_aws_fields(row: MySqlRow) -> Result<Vec<FieldDef>, sqlx::Error> {
    let columns = row.columns();
    let mut values = Vec::new();

    for column in columns {
        let field = column_into_fielddef(&row, column).inspect_err(|e| {
            error!(
                "Error converting column '{}' to FieldDef: {e}",
                column.name()
            );
        })?;

        values.push(field);
    }

    Ok(values)
}

fn column_into_fielddef(row: &MySqlRow, column: &MySqlColumn) -> Result<FieldDef, sqlx::Error> {
    let column_name = column.name();
    let type_name = column.type_info().name();

    let field = match type_name {
        "VARCHAR" | "CHAR" | "TEXT" | "LONGTEXT" | "MEDIUMTEXT" | "TINYTEXT" => {
            match row.try_get::<Option<String>, _>(column_name)? {
                Some(value) => FieldDef::StringValue(value),
                None => FieldDef::IsNull(true),
            }
        }
        "BOOLEAN" | "BOOL" => match row.try_get::<Option<bool>, _>(column_name)? {
            Some(value) => FieldDef::BooleanValue(value),
            None => FieldDef::IsNull(true),
        },
        "TINYINT" | "SMALLINT" | "MEDIUMINT" | "INT" | "BIGINT" => {
            match row.try_get::<Option<i64>, _>(column_name)? {
                Some(value) => FieldDef::LongValue(value),
                None => FieldDef::IsNull(true),
            }
        }
        "FLOAT" | "DOUBLE" | "DECIMAL" | "NUMERIC" => {
            match row.try_get::<Option<f64>, _>(column_name)? {
                Some(value) => FieldDef::DoubleValue(value),
                None => FieldDef::IsNull(true),
            }
        }
        "DATE" | "DATETIME" | "TIMESTAMP" | "TIME" | "YEAR" => {
            // Convert temporal types to string representation
            match row.try_get::<Option<DateTime<Utc>>, _>(column_name)? {
                Some(value) => FieldDef::StringValue(value.to_string()),
                None => FieldDef::IsNull(true),
            }
        }
        "VARBINARY" | "BINARY" | "BLOB" | "LONGBLOB" | "MEDIUMBLOB" | "TINYBLOB" => {
            match row.try_get::<Option<Vec<u8>>, _>(column_name)? {
                Some(value) => {
                    // Encode to base64 string
                    let value = base64::engine::general_purpose::STANDARD.encode(&value);
                    FieldDef::BlobValue(value)
                }
                None => FieldDef::IsNull(true),
            }
        }
        _ => {
            info!("Unknown field type for column '{column_name}': {type_name}");
            // Try to get as string for unknown types
            match row.try_get::<Option<String>, _>(column_name)? {
                Some(value) => FieldDef::StringValue(value),
                None => FieldDef::IsNull(true),
            }
        }
    };
    Ok(field)
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

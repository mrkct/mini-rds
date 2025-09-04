use chrono::{DateTime, Utc};
use log::{error, info};
use serde::{Deserialize, Serialize};
use sqlx::{mysql::{MySqlRow, MySqlColumn}, Column, Row, TypeInfo, types};
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
    pub name: Option<String>,
    pub value: Option<FieldDef>,
    pub type_hint: Option<TypeHintDef>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum FieldDef {
    ArrayValue(ArrayValueDef),
    BlobValue(BlobDef),
    BooleanValue(bool),
    DoubleValue(f64),
    IsNull(bool),
    LongValue(i64),
    StringValue(String),
}

pub struct VecFieldDef(pub Vec<FieldDef>);

impl TryFrom<MySqlRow> for VecFieldDef {
    type Error = sqlx::Error;

    fn try_from(row: MySqlRow) -> Result<Self, Self::Error> {
        let columns = row.columns();
        let mut values = Vec::new();
        
        for column in columns {
            let field = column_into_fielddef(&row, column)
                .inspect_err(|e| {
                    error!("Error converting column '{}' to FieldDef: {}", column.name(), e);
                })?;
            
            values.push(field);
        }
        
        Ok(VecFieldDef(values))
    }
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
        },
        "BOOLEAN" | "BOOL" => {
            match row.try_get::<Option<bool>, _>(column_name)? {
                Some(value) => FieldDef::BooleanValue(value),
                None => FieldDef::IsNull(true),
            }
        },
        "TINYINT" | "SMALLINT" | "MEDIUMINT" | "INT" | "BIGINT" => {
            match row.try_get::<Option<i64>, _>(column_name)? {
                Some(value) => FieldDef::LongValue(value),
                None => FieldDef::IsNull(true),
            }
        },
        "FLOAT" | "DOUBLE" | "DECIMAL" | "NUMERIC" => {
            match row.try_get::<Option<f64>, _>(column_name)? {
                Some(value) => FieldDef::DoubleValue(value),
                None => FieldDef::IsNull(true),
            }
        },
        "DATE" | "DATETIME" | "TIMESTAMP" | "TIME" | "YEAR" => {
            // Convert temporal types to string representation
            match row.try_get::<Option<DateTime<Utc>>, _>(column_name)? {
                Some(value) => FieldDef::StringValue(value.to_string()),
                None => FieldDef::IsNull(true),
            }
        },
        "VARBINARY" | "BINARY" | "BLOB" | "LONGBLOB" | "MEDIUMBLOB" | "TINYBLOB" => {
            match row.try_get::<Option<Vec<u8>>, _>(column_name)? {
                Some(value) => FieldDef::BlobValue(BlobDef { inner: value }),
                None => FieldDef::IsNull(true),
            }
        },
        _ => {
            info!("Unknown field type for column '{}': {}", column_name, type_name);
            // Try to get as string for unknown types
            match row.try_get::<Option<String>, _>(column_name)? {
                Some(value) => FieldDef::StringValue(value),
                None => FieldDef::IsNull(true),
            }
        },
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

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BlobDef {
    inner: Vec<u8>,
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

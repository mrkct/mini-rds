use std::collections::HashMap;

use anyhow::anyhow;
use axum::http::StatusCode;
use base64::Engine as _;
use chrono::{DateTime, Utc};
use log::{error, info};
use sqlx::{Column, Row, TypeInfo};
use sqlx::{
    Either, Executor, MySql, MySqlPool,
    mysql::{MySqlArguments, MySqlColumn, MySqlRow},
    query::Query,
};

use crate::aws::{FieldDef, SqlParameterDef};

const MAX_SQL_LEN: usize = 65536;

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

/// Rewrite named parameters (e.g., :id) to positional placeholders ('?') while preserving
/// all other SQL characters and whitespace exactly. Returns the rewritten SQL and the ordered
/// list of parameter names.
fn rewrite_named_params_preserving_sql(sql: &str) -> (String, Vec<String>) {
    #[derive(Copy, Clone, Debug, PartialEq, Eq)]
    enum State {
        Normal,
        Quoted(char), // quote delimiter: '\'', '"', or '`'
        LineComment,  // -- ... or # ...
        BlockComment, // /* ... */
    }

    let mut out = String::with_capacity(sql.len());
    let mut args = Vec::<String>::new();
    let mut state = State::Normal;
    let mut chars = sql.chars().peekable();

    while let Some(ch) = chars.next() {
        match state {
            State::Normal => match ch {
                '\'' | '"' | '`' => {
                    out.push(ch);
                    state = State::Quoted(ch);
                }
                '-' => {
                    if matches!(chars.peek(), Some('-')) {
                        out.push('-');
                        out.push('-');
                        chars.next();
                        state = State::LineComment;
                    } else {
                        out.push('-');
                    }
                }
                '#' => {
                    out.push('#');
                    state = State::LineComment;
                }
                '/' => {
                    if matches!(chars.peek(), Some('*')) {
                        out.push('/');
                        out.push('*');
                        chars.next();
                        state = State::BlockComment;
                    } else {
                        out.push('/');
                    }
                }
                ':' => {
                    // Only treat as a named parameter if next char starts a valid identifier
                    let mut clone = chars.clone();
                    if let Some(nc) = clone.peek().copied() {
                        if nc == '_' || nc.is_ascii_alphabetic() {
                            // consume identifier
                            let mut name = String::new();
                            while let Some(&c) = clone.peek() {
                                if c == '_' || c.is_ascii_alphanumeric() {
                                    name.push(c);
                                    clone.next();
                                } else {
                                    break;
                                }
                            }
                            if !name.is_empty() {
                                // commit consumption
                                for _ in 0..name.len() {
                                    chars.next();
                                }
                                out.push('?');
                                args.push(name);
                                continue;
                            }
                        }
                    }
                    // Not a named param, just output ':'
                    out.push(':');
                }
                _ => out.push(ch),
            },
            State::Quoted(delim) => {
                out.push(ch);
                match ch {
                    // backslash escapes inside quoted strings
                    '\\' if delim != '`' => {
                        if let Some(next) = chars.next() {
                            out.push(next);
                        }
                    }
                    // doubled delimiter ('' or "" or ``) as escape: copy both and stay quoted
                    _ if ch == delim => {
                        if matches!(chars.peek(), Some(&c) if c == delim) {
                            out.push(delim);
                            chars.next();
                        } else {
                            state = State::Normal;
                        }
                    }
                    _ => {}
                }
            }
            State::LineComment => {
                out.push(ch);
                if ch == '\n' {
                    state = State::Normal;
                }
            }
            State::BlockComment => {
                out.push(ch);
                if ch == '*' && matches!(chars.peek(), Some('/')) {
                    out.push('/');
                    chars.next();
                    state = State::Normal;
                }
            }
        }
    }

    (out, args)
}

fn bind_parameters<'q>(
    mut query: Query<'q, MySql, MySqlArguments>,
    args_to_be_bound: &[&str],
    params: &'q [SqlParameterDef],
) -> Result<Query<'q, MySql, MySqlArguments>, anyhow::Error> {
    let params = {
        let mut map = HashMap::new();
        for param in params {
            if map.insert(param.name.clone(), param).is_some() {
                return Err(anyhow!("Duplicate parameter: {}", param.name));
            }
        }
        map
    };

    for argname in args_to_be_bound {
        let Some(arg) = params.get(*argname) else {
            return Err(anyhow!("Missing parameter: {argname}"));
        };

        query = match &arg.value {
            FieldDef::ArrayValue(_) => {
                return Err(anyhow!("Array parameters are not supported"));
            }
            FieldDef::BlobValue(b64) => {
                let data = base64::engine::general_purpose::STANDARD
                    .decode(b64)
                    .map_err(|e| {
                        anyhow!(
                            "Failed to decode base64 blob for parameter '{}': {e}",
                            arg.name
                        )
                    })?;
                query.bind(data)
            }
            FieldDef::BooleanValue(x) => query.bind(*x),
            FieldDef::DoubleValue(x) => query.bind(*x),
            FieldDef::IsNull(_) => query.bind(None::<String>),
            FieldDef::LongValue(x) => query.bind(*x),
            FieldDef::StringValue(x) => query.bind(x.as_str()),
        }
    }

    Ok(query)
}

pub async fn run_query(
    pool: &MySqlPool,
    database: Option<String>,
    schema: Option<String>,
    sql: &str,
    params: Vec<Vec<SqlParameterDef>>,
) -> Result<Either<Vec<Vec<FieldDef>>, u64>, (StatusCode, anyhow::Error)> {
    let _ = params;
    if sql.len() > MAX_SQL_LEN {
        return Err((
            StatusCode::BAD_REQUEST,
            anyhow::anyhow!("SQL statement exceeds maximum length"),
        ));
    }

    // Use the same connection for all queries, because otherwise the "USE database"
    // command might not apply to the subsequent queries
    let mut conn = pool
        .acquire()
        .await
        .inspect_err(|e| error!("Failed to acquire a database connection: {e:?}"))
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.into()))?;

    if let Some(database) = &database {
        conn.execute(sqlx::raw_sql(&format!("USE {database}")))
            .await
            .inspect_err(|e| error!("Failed to select database '{database}': {e:?}"))
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.into()))?;
    }

    if schema.is_some() {
        return Err((
            StatusCode::NOT_IMPLEMENTED,
            anyhow!("Schema selection is not supported"),
        ));
    }

    let (prepared_sql, args_to_be_bound) = rewrite_named_params_preserving_sql(sql);
    info!("Running '{prepared_sql}' with {} parameters", params.len());

    let value = if sql.trim_start().to_ascii_uppercase().starts_with("SELECT") {
        let mut collected_records = vec![];

        for row_params in params {
            let query = sqlx::query(&prepared_sql);
            let arg_refs: Vec<&str> = args_to_be_bound.iter().map(|s| s.as_str()).collect();
            let query = bind_parameters(query, &arg_refs, &row_params)
                .map_err(|e| (StatusCode::BAD_REQUEST, e))?;

            let records = query
                .fetch_all(&mut *conn)
                .await
                .inspect_err(|e| error!("Failed to execute query: {e:?}"))
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.into()))?;

            collected_records.extend(
                records
                    .into_iter()
                    .filter_map(|row| try_row_to_aws_fields(row).ok()),
            );
        }

        Either::Left(collected_records)
    } else {
        let mut affected_rows = 0;
        for row_params in params {
            let query = sqlx::query(&prepared_sql);
            let arg_refs: Vec<&str> = args_to_be_bound.iter().map(|s| s.as_str()).collect();
            let query = bind_parameters(query, &arg_refs, &row_params)
                .map_err(|e| (StatusCode::BAD_REQUEST, e))?;

            affected_rows += query
                .execute(&mut *conn)
                .await
                .inspect_err(|e| error!("Failed to execute query: {e:?}"))
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.into()))?
                .rows_affected();
        }

        Either::Right(affected_rows)
    };

    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rewrite_named_params_simple() {
        let sql = "SELECT * FROM t WHERE id = :id AND name = :name";
        let (rewritten, args) = rewrite_named_params_preserving_sql(sql);
        assert_eq!(rewritten, "SELECT * FROM t WHERE id = ? AND name = ?");
        assert_eq!(args, vec!["id", "name"]);
    }

    #[test]
    fn test_rewrite_named_params_colon_in_string() {
        let sql = r#"SELECT ':notparam' AS s, col FROM t WHERE a = :a"#;
        let (rewritten, args) = rewrite_named_params_preserving_sql(sql);
        assert_eq!(
            rewritten,
            r#"SELECT ':notparam' AS s, col FROM t WHERE a = ?"#
        );
        assert_eq!(args, vec!["a"]);
    }

    #[test]
    fn test_rewrite_named_params_comments() {
        let sql = "-- :skip one\nSELECT :x /* :skip two */ , :y # :skip three\nFROM t";
        let (rewritten, args) = rewrite_named_params_preserving_sql(sql);
        assert_eq!(
            rewritten,
            "-- :skip one\nSELECT ? /* :skip two */ , ? # :skip three\nFROM t"
        );
        assert_eq!(args, vec!["x", "y"]);
    }

    #[test]
    fn test_rewrite_named_params_mysql_literals_preserved() {
        let sql = "INSERT INTO t (a,b,c,d) VALUES (x'1234', b'1010', _utf8mb4'hé', :p)";
        let (rewritten, args) = rewrite_named_params_preserving_sql(sql);
        assert!(rewritten.contains("x'1234', b'1010', _utf8mb4'hé'"));
        assert!(rewritten.ends_with(", ?)"));
        assert_eq!(args, vec!["p"]);
    }

    #[test]
    fn test_rewrite_named_params_non_identifier_after_colon() {
        let sql = "SELECT ':' AS c, :1 AS not_param";
        let (rewritten, args) = rewrite_named_params_preserving_sql(sql);
        assert_eq!(rewritten, "SELECT ':' AS c, :1 AS not_param");
        assert!(args.is_empty());
    }
}

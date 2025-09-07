use std::collections::HashMap;

use anyhow::anyhow;
use axum::http::StatusCode;
use base64::Engine as _;
use log::{error, warn};
use sqlx::{Either, Executor, MySql, MySqlPool, mysql::MySqlArguments, query::Query};

use crate::aws::{FieldDef, SqlParameterDef, try_row_to_aws_fields};

const MAX_SQL_LEN: usize = 65536;

fn tokenize_query(sql: &str) -> Vec<&str> {
    fn parse_quoted_string<'a>(
        sql: &'a str,
        chars: &mut std::iter::Peekable<std::str::CharIndices>,
        start_idx: usize,
        quote_char: char,
    ) -> &'a str {
        let mut end_idx = start_idx + 1;

        while let Some((idx, ch)) = chars.next() {
            if ch == quote_char {
                end_idx = idx + ch.len_utf8();
                break;
            } else if ch == '\\' {
                // Handle backslash escaping - consume the next character
                end_idx = idx + ch.len_utf8();
                if let Some((idx, next_ch)) = chars.next() {
                    end_idx = idx + next_ch.len_utf8();
                }
            } else {
                end_idx = idx + ch.len_utf8();
            }
        }

        &sql[start_idx..end_idx]
    }

    const SPECIAL_CHARS: &str = "=><!+-*/%(),;";

    let sql = sql.trim();

    let mut tokens = Vec::new();
    let mut chars = sql.char_indices().peekable();

    while let Some((start_idx, ch)) = chars.next() {
        match ch {
            ch if ch.is_whitespace() => continue,
            ch if SPECIAL_CHARS.contains(ch) => {
                tokens.push(&sql[start_idx..start_idx + ch.len_utf8()]);
            }

            // Quoted strings (single quotes, double quotes, backticks)
            '\'' | '"' | '`' => {
                let quoted_string = parse_quoted_string(sql, &mut chars, start_idx, ch);
                tokens.push(quoted_string);
            }

            // Regular tokens (identifiers, keywords, numbers)
            _ => {
                let token_start = start_idx;
                let mut end_idx = start_idx + ch.len_utf8();

                // Continue until we hit whitespace or a special character
                while let Some(&(idx, next_ch)) = chars.peek() {
                    match next_ch {
                        ch if ch.is_whitespace() => break,
                        '\'' | '"' | '`' => break,
                        ch if SPECIAL_CHARS.contains(ch) => break,
                        _ => {
                            chars.next();
                            end_idx = idx + next_ch.len_utf8();
                        }
                    }
                }

                let token = &sql[token_start..end_idx];
                if !token.is_empty() {
                    tokens.push(token);
                }
            }
        }
    }

    tokens
}

fn make_prepared_statement(tokens: Vec<&str>) -> (String, Vec<&str>) {
    let mut prepared_query = String::new();
    let mut args = vec![];

    for token in tokens {
        let mut chars = token.chars();
        if let Some(':') = chars.next() {
            // Named parameter
            prepared_query.push('?');
            args.push(&token[1..]);
        } else {
            prepared_query.push_str(token);
        }
        prepared_query.push(' ');
    }

    (prepared_query.trim_end().to_string(), args)
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
    let mut tx = pool
        .begin()
        .await
        .inspect_err(|e| error!("Failed to acquire a database connection: {e:?}"))
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.into()))?;

    if let Some(database) = &database {
        tx.execute(sqlx::raw_sql(&format!("USE {database}")))
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

    let query_tokens = tokenize_query(sql);
    let (prepared_sql, args_to_be_bound) = make_prepared_statement(query_tokens.clone());

    let value = if let Some(first) = query_tokens.first()
        && first.eq_ignore_ascii_case("SELECT")
    {
        let mut collected_records = vec![];

        for row_params in params {
            let query = sqlx::query(&prepared_sql);
            let query = bind_parameters(query, &args_to_be_bound, &row_params)
                .map_err(|e| (StatusCode::BAD_REQUEST, e))?;

            let records = query
                .fetch_all(&mut *tx)
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
            let query = bind_parameters(query, &args_to_be_bound, &row_params)
                .map_err(|e| (StatusCode::BAD_REQUEST, e))?;

            affected_rows += query
                .execute(&mut *tx)
                .await
                .inspect_err(|e| error!("Failed to execute query: {e:?}"))
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.into()))?
                .rows_affected();
        }

        Either::Right(affected_rows)
    };

    tx.commit()
        .await
        .inspect_err(|e| warn!("Failed to commit transaction: {e:?}"))
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.into()))?;

    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tokenize_query() {
        let sql = r#"SELECT * FROM table WHERE name = 'Marco' AND age > 30"#;
        let tokens = tokenize_query(sql);
        assert_eq!(
            tokens,
            vec![
                "SELECT", "*", "FROM", "table", "WHERE", "name", "=", "'Marco'", "AND", "age", ">",
                "30"
            ]
        );
    }

    #[test]
    fn test_tokenize_query_escaped_quote() {
        let sql = r#"SELECT * FROM table WHERE description = 'It\'s a test'"#;
        let tokens = tokenize_query(sql);
        assert_eq!(
            tokens,
            vec![
                "SELECT",
                "*",
                "FROM",
                "table",
                "WHERE",
                "description",
                "=",
                r#"'It\'s a test'"#
            ]
        );
    }

    #[test]
    fn test_tokenize_query_different_quotes() {
        let sql = r#"SELECT * FROM table WHERE note = "He said, 'Hello'""#;
        let tokens = tokenize_query(sql);
        assert_eq!(
            tokens,
            vec![
                "SELECT",
                "*",
                "FROM",
                "table",
                "WHERE",
                "note",
                "=",
                r#""He said, 'Hello'""#
            ]
        );
    }

    #[test]
    fn test_tokenize_query_whitespace() {
        let sql = "  SELECT   *  FROM   table  ";
        let tokens = tokenize_query(sql);
        assert_eq!(tokens, vec!["SELECT", "*", "FROM", "table"]);
    }

    #[test]
    fn test_tokenize_query_backticks() {
        let sql = r#"SELECT * FROM `my table` WHERE `column name` = 'value'"#;
        let tokens = tokenize_query(sql);
        assert_eq!(
            tokens,
            vec![
                "SELECT",
                "*",
                "FROM",
                "`my table`",
                "WHERE",
                "`column name`",
                "=",
                "'value'"
            ]
        );
    }

    #[test]
    fn test_tokenize_query_escaped_backticks() {
        let sql = r#"SELECT * FROM `table\`name` WHERE id = 1"#;
        let tokens = tokenize_query(sql);
        assert_eq!(
            tokens,
            vec![
                "SELECT",
                "*",
                "FROM",
                "`table\\`name`",
                "WHERE",
                "id",
                "=",
                "1"
            ]
        );
    }

    #[test]
    fn test_make_prepared_statement_no_params() {
        let tokens = vec!["SELECT", "*", "FROM", "table"];
        let (prepared_sql, args) = make_prepared_statement(tokens);
        assert_eq!(prepared_sql, "SELECT * FROM table");
        assert_eq!(args, Vec::<&str>::new());
    }

    #[test]
    fn test_make_prepared_statement_single_param() {
        let tokens = vec!["SELECT", "*", "FROM", "table", "WHERE", "id", "=", ":id"];
        let (prepared_sql, args) = make_prepared_statement(tokens);
        assert_eq!(prepared_sql, "SELECT * FROM table WHERE id = ?");
        assert_eq!(args, vec!["id"]);
    }

    #[test]
    fn test_make_prepared_statement_multiple_params() {
        let tokens = vec![
            "SELECT", "*", "FROM", "table", "WHERE", "name", "=", ":name", "AND", "age", ">",
            ":age",
        ];
        let (prepared_sql, args) = make_prepared_statement(tokens);
        assert_eq!(
            prepared_sql,
            "SELECT * FROM table WHERE name = ? AND age > ?"
        );
        assert_eq!(args, vec!["name", "age"]);
    }

    #[test]
    fn test_make_prepared_statement_repeated_param() {
        let tokens = vec![
            "SELECT",
            "*",
            "FROM",
            "table",
            "WHERE",
            "status",
            "=",
            ":status",
            "OR",
            "backup_status",
            "=",
            ":status",
        ];
        let (prepared_sql, args) = make_prepared_statement(tokens);
        assert_eq!(
            prepared_sql,
            "SELECT * FROM table WHERE status = ? OR backup_status = ?"
        );
        assert_eq!(args, vec!["status", "status"]);
    }

    #[test]
    fn test_make_prepared_statement_mixed_content() {
        let tokens = vec![
            "INSERT", "INTO", "users", "(", "name", ",", "email", ")", "VALUES", "(", ":name", ",",
            ":email", ")",
        ];
        let (prepared_sql, args) = make_prepared_statement(tokens);
        assert_eq!(
            prepared_sql,
            "INSERT INTO users ( name , email ) VALUES ( ? , ? )"
        );
        assert_eq!(args, vec!["name", "email"]);
    }

    #[test]
    fn test_make_prepared_statement_colon_in_string() {
        // Test that colons inside quoted strings are not treated as parameters
        let tokens = vec![
            "SELECT",
            "*",
            "FROM",
            "table",
            "WHERE",
            "note",
            "=",
            "'time:12:00'",
            "AND",
            "id",
            "=",
            ":id",
        ];
        let (prepared_sql, args) = make_prepared_statement(tokens);
        assert_eq!(
            prepared_sql,
            "SELECT * FROM table WHERE note = 'time:12:00' AND id = ?"
        );
        assert_eq!(args, vec!["id"]);
    }

    #[test]
    fn test_make_prepared_statement_empty_param_name() {
        let tokens = vec!["SELECT", "*", "FROM", "table", "WHERE", "id", "=", ":"];
        let (prepared_sql, args) = make_prepared_statement(tokens);
        assert_eq!(prepared_sql, "SELECT * FROM table WHERE id = ?");
        assert_eq!(args, vec![""]);
    }
}

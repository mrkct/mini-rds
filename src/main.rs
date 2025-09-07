use anyhow::Result;
use axum::{
    Router,
    extract::{Json, State},
    http::StatusCode,
    routing::post,
};
use log::{error, info};
use sqlx::{Either, MySqlPool};
use std::net::SocketAddr;

use crate::aws::{
    BatchExecuteStatementInputDef, BatchExecuteStatementOutputDef, ExecuteStatementInputDef,
    ExecuteStatementOutputDef,
};

mod aws;
mod query;
use query::run_query;

macro_rules! get_or_400 {
    ($input:expr, $field:ident) => {
        match &$input.$field {
            Some(value) => value,
            None => {
                return Err((
                    StatusCode::BAD_REQUEST,
                    format!("Missing required field: {}", stringify!($field)),
                ))
            }
        }
    };
}

async fn execute_statement(
    State(pool): State<MySqlPool>,
    Json(input): Json<ExecuteStatementInputDef>,
) -> Result<Json<ExecuteStatementOutputDef>, (StatusCode, String)> {
    let sql = get_or_400!(input, sql);
    let params = input.parameters.unwrap_or(vec![]);

    let output = match run_query(&pool, input.database, input.schema, sql, vec![params]).await {
        Ok(Either::Left(records)) => ExecuteStatementOutputDef {
            records: Some(records),
            ..ExecuteStatementOutputDef::default()
        },
        Ok(Either::Right(affected_rows)) => ExecuteStatementOutputDef {
            number_of_records_updated: affected_rows as i64,
            ..ExecuteStatementOutputDef::default()
        },
        Err((status, err)) => {
            error!("Error executing statement: {err}");
            return Err((status, err.to_string()));
        }
    };

    Ok(Json(output))
}

async fn batch_execute_statement(
    State(pool): State<MySqlPool>,
    Json(input): Json<BatchExecuteStatementInputDef>,
) -> Result<Json<BatchExecuteStatementOutputDef>, (StatusCode, String)> {
    let sql = get_or_400!(input, sql);
    let params = input.parameter_sets.unwrap_or(vec![]);

    let output = match run_query(&pool, input.database, input.schema, sql, params).await {
        Ok(Either::Left(_records)) => BatchExecuteStatementOutputDef {
            ..BatchExecuteStatementOutputDef::default()
        },
        Ok(Either::Right(_affected_rows)) => BatchExecuteStatementOutputDef {
            ..BatchExecuteStatementOutputDef::default()
        },
        Err((status, err)) => return Err((status, err.to_string())),
    };

    Ok(Json(output))
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::builder()
        .filter_level(log::LevelFilter::Debug)
        .init();

    let url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "mysql://root:my-secret-password@localhost:3306".to_string());
    let pool = MySqlPool::connect(&url).await?;

    let app = Router::new()
        .route("/Execute", post(execute_statement))
        .route("/BatchExecute", post(batch_execute_statement))
        .with_state(pool);

    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    info!("Listening on {addr}");
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app)
        .await
        .expect("Failed to start server");
    Ok(())
}

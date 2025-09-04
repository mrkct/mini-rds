use anyhow::Result;
use axum::{
    Router,
    extract::{Json, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
};
use log::{debug, error, info, warn};
use sqlx::{Acquire, MySqlPool, Executor, Execute};
use std::{fmt::format, net::SocketAddr};


use crate::aws::{ExecuteStatementInputDef, ExecuteStatementOutputDef, VecFieldDef};

mod aws;

// A macro that either gets a value from the struct or returns a 400 error if it's None
macro_rules! get_or_400 {
    ($input:expr, $field:ident) => {
        match &$input.$field {
            Some(value) => value,
            None => return Err(StatusCode::BAD_REQUEST),
        }
    };
}

async fn execute_statement(
    State(pool): State<MySqlPool>,
    Json(input): Json<ExecuteStatementInputDef>,
) -> Result<Json<ExecuteStatementOutputDef>, StatusCode> {
    info!("Received ExecuteStatement request: {input:#?}");

    // Use the same connection for all queries, because otherwise the "USE database"
    // command might not apply to the subsequent queries
    let mut tx = pool.begin()
        .await
        .inspect_err(|e| error!("Failed to acquire a database connection: {e:?}"))
        .map_err(|_| {StatusCode::INTERNAL_SERVER_ERROR })?;

    if let Some(database) = &input.database {
        tx
            .execute(sqlx::raw_sql(&format!("USE {database}")))
            .await
            .inspect_err(|e| error!("Failed to select database '{}': {e:?}", database))
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    }

    if let Some(schema) = &input.schema {
        warn!("Received 'schema={schema}' but schema selection is not implemented");
        return Err(StatusCode::NOT_IMPLEMENTED);
    }

    let sql = get_or_400!(input, sql);
    let _params = input
        .parameters
        .as_ref()
        .map(|p| p.as_slice())
        .unwrap_or(&[]);

    let records = sqlx::query(sql)
        .fetch_all(&mut *tx)
        .await
        .inspect_err(|e| error!("Failed to execute query: {e:?}"))
        .map_err(|_e| StatusCode::INTERNAL_SERVER_ERROR)?;

    tx.commit().await
        .inspect_err(|e| warn!("Failed to commit transaction: {e:?}"))
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    
    let records: Vec<_> = records
        .into_iter()
        .filter_map(|row| VecFieldDef::try_from(row).map(|x| x.0).ok())
        .collect();

    debug!(
        "Query executed successfully, fetched {} records",
        records.len()
    );
    debug!("Records: {records:#?}");

    Ok(Json(ExecuteStatementOutputDef {
        records: Some(records),
        ..ExecuteStatementOutputDef::default()
    }))
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
        .with_state(pool);

    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    info!("Listening on {addr}");
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app)
        .await
        .expect("Failed to start server");
    Ok(())
}

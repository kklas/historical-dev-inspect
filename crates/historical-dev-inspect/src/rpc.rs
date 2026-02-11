use std::sync::Arc;

use axum::{
    body::Body,
    extract::State,
    http::{Response, StatusCode},
    response::IntoResponse,
    routing::post,
    Json, Router,
};
use fastcrypto::encoding::Base64;
use serde::{Deserialize, Serialize};
use sui_json_rpc_types::DevInspectResults;
use sui_protocol_config::{Chain, ProtocolConfig};
use sui_types::{
    base_types::SuiAddress,
    digests::TransactionDigest,
    error::SuiError,
    inner_temporary_store::PackageStoreWithFallback,
    storage::{BackingPackageStore, ChildObjectResolver, ObjectStore, ParentSync},
    sui_serde::BigInt,
    sui_system_state::{get_sui_system_state, SuiSystemStateTrait},
    transaction::TransactionKind,
};
use thiserror::Error;

use crate::{
    db::{HistoricalDb, HistoricalDbError},
    execution::{dev_inspect_transaction, EpochInfo},
    graphql::GraphqlClient,
    graphql_store::GraphqlView,
    store::{HistoricalPoint, HistoricalView, StoreError},
};

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub enum Position {
    Before,
    After,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "$kind", rename_all = "camelCase")]
pub enum ExecutionPoint {
    Checkpoint {
        #[serde(rename = "checkpointSequenceNumber")]
        checkpoint_sequence_number: u64,
        position: Position,
    },
    Transaction {
        #[serde(rename = "transactionDigest")]
        transaction_digest: TransactionDigest,
        position: Position,
    },
}

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct HistoricalDevInspectRequest {
    pub sender_address: SuiAddress,
    pub tx_bytes: Base64,
    pub execution_point: ExecutionPoint,
    pub gas_price: Option<BigInt<u64>>,
}

#[derive(Debug, Error)]
pub enum ApiError {
    #[error("invalid input: {0}")]
    BadRequest(String),
    #[error("bcs error: {0}")]
    BcsError(#[from] bcs::Error),
    #[error("sui error: {0}")]
    SuiError(#[from] SuiError),
    #[error("db error: {0}")]
    DbError(#[from] HistoricalDbError),
    #[error("store error: {0}")]
    StoreError(#[from] StoreError),
    #[error("execution error: {0}")]
    ExecutionError(#[from] anyhow::Error),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response<Body> {
        match self {
            ApiError::BadRequest(msg) => (StatusCode::BAD_REQUEST, msg).into_response(),
            ApiError::BcsError(e) => (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
            ApiError::DbError(e) => {
                (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
            }
            ApiError::SuiError(e) => {
                (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
            }
            ApiError::StoreError(e) => (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
            ApiError::ExecutionError(e) => {
                (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
            }
        }
    }
}

#[derive(Clone)]
pub struct AppState {
    pub db: Arc<HistoricalDb>,
}

pub fn create_router() -> Router<AppState> {
    Router::new().route(
        "/historical_devInspectTransactionBlock",
        post(historical_dev_inspect_handler),
    )
}

pub async fn historical_dev_inspect_handler(
    State(state): State<AppState>,
    Json(data): Json<HistoricalDevInspectRequest>,
) -> Response<Body> {
    handle_historical_dev_inspect(state.db.clone(), data)
        .map(IntoResponse::into_response)
        .unwrap_or_else(IntoResponse::into_response)
}

fn handle_historical_dev_inspect(
    db: Arc<HistoricalDb>,
    data: HistoricalDevInspectRequest,
) -> Result<Json<DevInspectResults>, ApiError> {
    let transaction_kind: TransactionKind = bcs::from_bytes(
        &data
            .tx_bytes
            .to_vec()
            .map_err(|e| ApiError::BadRequest(e.to_string()))?,
    )?;

    let historical_point = match data.execution_point {
        ExecutionPoint::Checkpoint {
            checkpoint_sequence_number,
            position,
        } => HistoricalPoint::new_for_checkpoint(
            checkpoint_sequence_number,
            matches!(position, Position::Before),
        ),
        ExecutionPoint::Transaction {
            transaction_digest,
            position,
        } => HistoricalPoint::new_for_transaction(
            &db,
            transaction_digest,
            matches!(position, Position::Before),
        )?,
    };

    let store = HistoricalView::new(db, historical_point)?;

    run_dev_inspect(store, &transaction_kind, data.sender_address)
}

// --- GraphQL-backed mode ---

#[derive(Clone)]
pub struct GraphqlAppState {
    pub client: Arc<GraphqlClient>,
}

pub fn create_graphql_router() -> Router<GraphqlAppState> {
    Router::new().route(
        "/historical_devInspectTransactionBlock",
        post(historical_dev_inspect_graphql_handler),
    )
}

pub async fn historical_dev_inspect_graphql_handler(
    State(state): State<GraphqlAppState>,
    Json(data): Json<HistoricalDevInspectRequest>,
) -> Response<Body> {
    handle_historical_dev_inspect_graphql(state.client.clone(), data)
        .map(IntoResponse::into_response)
        .unwrap_or_else(IntoResponse::into_response)
}

fn handle_historical_dev_inspect_graphql(
    client: Arc<GraphqlClient>,
    data: HistoricalDevInspectRequest,
) -> Result<Json<DevInspectResults>, ApiError> {
    let transaction_kind: TransactionKind = bcs::from_bytes(
        &data
            .tx_bytes
            .to_vec()
            .map_err(|e| ApiError::BadRequest(e.to_string()))?,
    )?;

    let checkpoint = match data.execution_point {
        ExecutionPoint::Checkpoint {
            checkpoint_sequence_number,
            position,
        } => match position {
            // State before checkpoint N = state at end of checkpoint N-1
            Position::Before => {
                if checkpoint_sequence_number == 0 {
                    return Err(ApiError::BadRequest(
                        "cannot query state before checkpoint 0".into(),
                    ));
                }
                checkpoint_sequence_number - 1
            }
            // State after checkpoint N = state at end of checkpoint N
            Position::After => checkpoint_sequence_number,
        },
        ExecutionPoint::Transaction { .. } => {
            return Err(ApiError::BadRequest(
                "transaction-level granularity is not supported in GraphQL mode; \
                 use checkpoint-level execution points instead"
                    .into(),
            ));
        }
    };

    let store = GraphqlView::new(client, checkpoint);

    run_dev_inspect(store, &transaction_kind, data.sender_address)
}

// --- Shared execution logic ---

pub fn run_dev_inspect<S>(
    store: S,
    transaction_kind: &TransactionKind,
    sender_address: SuiAddress,
) -> Result<Json<DevInspectResults>, ApiError>
where
    S: ObjectStore + ChildObjectResolver + BackingPackageStore + ParentSync,
{
    let system_state = get_sui_system_state(&store)?;

    let protocol_config =
        ProtocolConfig::get_for_version(system_state.protocol_version().into(), Chain::Mainnet);

    let (inner_store, _gas_status, effects, execution_results) = dev_inspect_transaction(
        &store,
        transaction_kind,
        sender_address,
        EpochInfo::from(&system_state),
        &protocol_config,
    )?;

    let silent = true;
    let executor = sui_execution::executor(&protocol_config, silent)
        .expect("Creating an executor should not fail here");

    let mut layout_resolver =
        executor.type_layout_resolver(Box::new(PackageStoreWithFallback::new(&inner_store, store)));

    DevInspectResults::new(
        effects,
        inner_store.events.clone(),
        execution_results,
        vec![], // raw_txn_data
        vec![], // raw_effects
        layout_resolver.as_mut(),
    )
    .map(Json)
    .map_err(ApiError::SuiError)
}

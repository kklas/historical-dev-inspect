use std::{collections::HashSet, sync::Arc};

use anyhow::Result;
use chrono::{DateTime, Utc};
use shared_crypto::intent::{AppId, Intent, IntentMessage, IntentScope, IntentVersion};
use sui_graphql_client::query_types::Epoch;
use sui_protocol_config::ProtocolConfig;
use sui_types::{
    base_types::SuiAddress,
    committee::EpochId,
    crypto::default_hash,
    digests::TransactionDigest,
    effects::TransactionEffects,
    error::ExecutionError,
    execution::ExecutionResult,
    gas::checked::SuiGasStatus,
    inner_temporary_store::InnerTemporaryStore,
    metrics::LimitsMetrics,
    object::Object,
    sui_system_state::{SuiSystemState, SuiSystemStateTrait},
    transaction::{
        GasData, InputObjectKind, ObjectReadResult, TransactionData, TransactionDataV1,
        TransactionExpiration, TransactionKind,
    },
};

use crate::store::HistoricalView;
use crate::transaction_input_loader::read_objects_for_signing;

const DEV_INSPECT_GAS_COIN_VALUE: u64 = 1_000_000_000_000_000;

pub struct EpochInfo {
    pub epoch_id: u64,
    pub start_timestamp_ms: u64,
    pub reference_gas_price: u64,
}

impl TryFrom<&Epoch> for EpochInfo {
    type Error = anyhow::Error;

    fn try_from(epoch: &Epoch) -> Result<Self, Self::Error> {
        Ok(EpochInfo {
            epoch_id: epoch.epoch_id,
            start_timestamp_ms: epoch
                .start_timestamp
                .0
                .parse::<DateTime<Utc>>()
                .expect("Failed to parse timestamp")
                .timestamp_millis() as u64,
            reference_gas_price: epoch
                .reference_gas_price
                .clone()
                .ok_or_else(|| anyhow::anyhow!("Reference gas price not available for epoch"))?
                .try_into()?,
        })
    }
}

impl From<&SuiSystemState> for EpochInfo {
    fn from(system_state: &SuiSystemState) -> Self {
        EpochInfo {
            epoch_id: system_state.epoch(),
            start_timestamp_ms: system_state.epoch_start_timestamp_ms(),
            reference_gas_price: system_state.reference_gas_price(),
        }
    }
}

pub fn dev_inspect_transaction(
    store: &HistoricalView,
    transaction_kind: &TransactionKind,
    sender: SuiAddress,
    epoch_info: EpochInfo,
    protocol_config: &ProtocolConfig,
) -> Result<(
    InnerTemporaryStore,
    SuiGasStatus,
    TransactionEffects,
    Result<Vec<ExecutionResult>, ExecutionError>,
)> {
    let (mut input_objects, receiving_objects) = read_objects_for_signing(
        store,
        &transaction_kind.input_objects()?,
        &transaction_kind.receiving_objects(),
        epoch_info.epoch_id,
    )?;

    // use a dummy gas object
    let reference_gas_price = epoch_info.reference_gas_price;
    let transaction_gas_price = epoch_info.reference_gas_price;
    let max_tx_gas = protocol_config.max_tx_gas();

    let dummy_gas_object =
        Object::new_gas_with_balance_and_owner_for_testing(DEV_INSPECT_GAS_COIN_VALUE, sender);
    let gas_object_ref = dummy_gas_object.compute_object_reference();

    let gas_data = GasData {
        payment: vec![gas_object_ref],
        owner: sender,
        price: reference_gas_price,
        budget: max_tx_gas,
    };
    let gas_status = SuiGasStatus::new(
        max_tx_gas,
        transaction_gas_price,
        reference_gas_price,
        protocol_config,
    )?;

    input_objects.push(ObjectReadResult::new(
        InputObjectKind::ImmOrOwnedMoveObject(gas_object_ref),
        dummy_gas_object.into(),
    ));

    // Use a throwaway metrics registry for local execution.
    let registry = prometheus::Registry::new();
    let metrics = Arc::new(LimitsMetrics::new(&registry));

    let transaction_data = TransactionData::V1(TransactionDataV1 {
        kind: transaction_kind.clone(),
        sender,
        gas_data: gas_data.clone(),
        expiration: TransactionExpiration::None,
    });

    let intent_msg = IntentMessage::new(
        Intent {
            version: IntentVersion::V0,
            scope: IntentScope::TransactionData,
            app_id: AppId::Sui,
        },
        transaction_data,
    );

    let transaction_digest = TransactionDigest::new(default_hash(&intent_msg.value));
    let transaction_signer = sender;

    let checked_input_objects = sui_transaction_checks::check_dev_inspect_input(
        protocol_config,
        transaction_kind,
        input_objects,
        receiving_objects,
    )?;

    let silent = true;
    let enable_profiler = None;
    let executor = sui_execution::executor(protocol_config, silent, enable_profiler)
        .expect("Creating an executor should not fail here");

    let enable_expensive_checks = false;
    let certificate_deny_set = HashSet::new();
    let epoch_id = EpochId::from(epoch_info.epoch_id);
    let epoch_timestamp_ms = epoch_info.start_timestamp_ms;
    let skip_all_checks = true;

    Ok(executor.dev_inspect_transaction(
        store,
        protocol_config,
        metrics,
        enable_expensive_checks,
        &certificate_deny_set,
        &epoch_id,
        epoch_timestamp_ms,
        checked_input_objects,
        gas_data,
        gas_status,
        transaction_kind.clone(),
        transaction_signer,
        transaction_digest,
        skip_all_checks,
    ))
}

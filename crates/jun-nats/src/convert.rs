//! jun-types record → proto::v1 message conversions.
//!
//! Mechanical mapping — keep field order in sync with proto/jun.proto.

use crate::proto;
use jun_types::{
    BalanceChangeRecord, CheckpointRecord, DependencyRecord, MoveCallRecord, ObjectChangeRecord,
    TransactionRecord,
};

pub fn checkpoint(r: &CheckpointRecord) -> proto::Checkpoint {
    proto::Checkpoint {
        sequence_number: r.sequence_number.clone(),
        epoch: r.epoch.clone(),
        timestamp_ms: r.timestamp_ms.clone(),
        digest: r.digest.clone(),
        previous_digest: r.previous_digest.clone(),
        content_digest: r.content_digest.clone(),
        total_network_transactions: r.total_network_transactions.clone(),
        rolling_computation_cost: r.rolling_computation_cost.clone(),
        rolling_storage_cost: r.rolling_storage_cost.clone(),
        rolling_storage_rebate: r.rolling_storage_rebate.clone(),
        rolling_non_refundable_storage_fee: r.rolling_non_refundable_storage_fee.clone(),
    }
}

pub fn transaction(r: &TransactionRecord) -> proto::Transaction {
    proto::Transaction {
        digest: r.digest.clone(),
        sender: r.sender.clone(),
        success: r.success,
        computation_cost: r.computation_cost.clone(),
        storage_cost: r.storage_cost.clone(),
        storage_rebate: r.storage_rebate.clone(),
        non_refundable_storage_fee: r.non_refundable_storage_fee.clone(),
        checkpoint_seq: r.checkpoint_seq.clone(),
        timestamp_ms: r.timestamp_ms.clone(),
        move_call_count: r.move_call_count,
        epoch: r.epoch.clone(),
        error_kind: r.error_kind.clone(),
        error_description: r.error_description.clone(),
        error_command_index: r.error_command_index,
        error_abort_code: r.error_abort_code.clone(),
        error_module: r.error_module.clone(),
        error_function: r.error_function.clone(),
        error_arg_idx: r.error_arg_idx,
        error_type_arg_idx: r.error_type_arg_idx,
        error_sub_kind: r.error_sub_kind.clone(),
        error_current_size: r.error_current_size.clone(),
        error_max_size: r.error_max_size.clone(),
        error_coin_type: r.error_coin_type.clone(),
        error_denied_address: r.error_denied_address.clone(),
        error_object_id: r.error_object_id.clone(),
        events_digest: r.events_digest.clone(),
        lamport_version: r.lamport_version.clone(),
        dependency_count: r.dependency_count,
        gas_owner: r.gas_owner.clone(),
        gas_price: r.gas_price.clone(),
        gas_budget: r.gas_budget.clone(),
        gas_object_id: r.gas_object_id.clone(),
        expiration_kind: r.expiration_kind.clone(),
        expiration_epoch: r.expiration_epoch.clone(),
        expiration_min_epoch: r.expiration_min_epoch.clone(),
        expiration_max_epoch: r.expiration_max_epoch.clone(),
        expiration_min_ts_seconds: r.expiration_min_ts_seconds.clone(),
        expiration_max_ts_seconds: r.expiration_max_ts_seconds.clone(),
        expiration_chain: r.expiration_chain.clone(),
        expiration_nonce: r.expiration_nonce,
    }
}

pub fn move_call(r: &MoveCallRecord) -> proto::MoveCall {
    proto::MoveCall {
        tx_digest: r.tx_digest.clone(),
        call_index: r.call_index,
        package: r.package.clone(),
        module: r.module.clone(),
        function: r.function.clone(),
        checkpoint_seq: r.checkpoint_seq.clone(),
        timestamp_ms: r.timestamp_ms.clone(),
    }
}

pub fn balance_change(r: &BalanceChangeRecord) -> proto::BalanceChange {
    proto::BalanceChange {
        tx_digest: r.tx_digest.clone(),
        checkpoint_seq: r.checkpoint_seq.clone(),
        address: r.address.clone(),
        coin_type: r.coin_type.clone(),
        amount: r.amount.clone(),
        timestamp_ms: r.timestamp_ms.clone(),
    }
}

pub fn object_change(r: &ObjectChangeRecord) -> proto::ObjectChange {
    proto::ObjectChange {
        tx_digest: r.tx_digest.clone(),
        object_id: r.object_id.clone(),
        change_type: r.change_type.clone(),
        object_type: r.object_type.clone(),
        input_version: r.input_version.clone(),
        input_digest: r.input_digest.clone(),
        input_owner: r.input_owner.clone(),
        input_owner_kind: r.input_owner_kind.clone(),
        output_version: r.output_version.clone(),
        output_digest: r.output_digest.clone(),
        output_owner: r.output_owner.clone(),
        output_owner_kind: r.output_owner_kind.clone(),
        is_gas_object: r.is_gas_object,
        checkpoint_seq: r.checkpoint_seq.clone(),
        timestamp_ms: r.timestamp_ms.clone(),
    }
}

pub fn dependency(r: &DependencyRecord) -> proto::Dependency {
    proto::Dependency {
        tx_digest: r.tx_digest.clone(),
        depends_on_digest: r.depends_on_digest.clone(),
        checkpoint_seq: r.checkpoint_seq.clone(),
        timestamp_ms: r.timestamp_ms.clone(),
    }
}

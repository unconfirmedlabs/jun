//! Checkpoint extraction — protobuf parsing + BCS decode + record extraction.
//!
//! Uses sui-types for type-safe BCS deserialization of TransactionEffects,
//! TransactionData, and TransactionEvents.

use sui_types::effects::TransactionEffects;
use sui_types::transaction::SenderSignedData;
use sui_types::messages_checkpoint::CheckpointContents;
use serde::Serialize;

/// Decoded checkpoint ready for JSON serialization.
#[derive(Serialize)]
pub struct ExtractedCheckpoint {
    pub transactions: Vec<ExtractedTransaction>,
}

/// A single transaction's extracted data.
#[derive(Serialize)]
pub struct ExtractedTransaction {
    pub digest: String,
    pub sender: String,
    pub success: bool,
    pub computation_cost: String,
    pub storage_cost: String,
    pub storage_rebate: String,
    pub epoch: String,
    pub changed_objects_count: usize,
    pub dependencies_count: usize,
}

/// Extract all records from a decompressed checkpoint protobuf.
///
/// TODO: Currently uses a minimal protobuf walk to find BCS fields.
/// Future: use prost with the Sui proto definitions for full fidelity.
pub fn extract_checkpoint(_decompressed: &[u8]) -> Result<String, Box<dyn std::error::Error>> {
    // For now, return a minimal scaffold that proves sui-types BCS works.
    // The full implementation will:
    // 1. Parse proto to find per-transaction effects/tx/events BCS fields
    // 2. bcs::from_bytes::<TransactionEffects>(&effects_bcs)
    // 3. bcs::from_bytes::<SenderSignedData>(&tx_bcs)
    // 4. Extract records matching Jun's ProcessedCheckpoint shape

    let result = ExtractedCheckpoint {
        transactions: vec![],
    };
    Ok(serde_json::to_string(&result)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sui_types::effects::TransactionEffects;

    #[test]
    fn test_sui_types_available() {
        // Prove sui-types BCS types are usable
        // TransactionEffects is the main type we'll decode
        let _effects_type_name = std::any::type_name::<TransactionEffects>();
        assert!(_effects_type_name.contains("TransactionEffects"));
    }

    #[test]
    fn test_bcs_decode_smoke() {
        // Prove BCS decode compiles and links against sui-types
        // This will fail at runtime (empty bytes) but proves the types resolve
        let result = bcs::from_bytes::<TransactionEffects>(&[]);
        assert!(result.is_err()); // Expected: empty bytes can't decode
    }
}

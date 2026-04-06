/**
 * NATS message types — shared between jun (publisher) and consumers (proxy, clients).
 *
 * Each type corresponds to a NATS subject: jun.{type_name}
 */

export interface CheckpointMessage {
  sequence_number: string;
  epoch: string;
  digest: string;
  timestamp: string; // ISO 8601
  tx_count: number;
}

export interface TransactionMessage {
  digest: string;
  sender: string;
  success: boolean;
  computation_cost: string;
  storage_cost: string;
  storage_rebate: string;
  move_call_count: number;
  checkpoint_seq: string;
  timestamp: string;
  epoch: string;
}

export interface MoveCallMessage {
  tx_digest: string;
  call_index: number;
  package: string;
  module: string;
  function: string;
  checkpoint_seq: string;
  timestamp: string;
}

export interface BalanceChangeMessage {
  tx_digest: string;
  checkpoint_seq: string;
  address: string;
  coin_type: string;
  amount: string;
  timestamp: string;
}

export interface ObjectChangeMessage {
  tx_digest: string;
  object_id: string;
  change_type: string;
  object_type: string | null;
  checkpoint_seq: string;
  timestamp: string;
}

export interface TransactionDependencyMessage {
  tx_digest: string;
  depends_on_digest: string;
  checkpoint_seq: string;
  timestamp: string;
}

export interface TransactionInputMessage {
  tx_digest: string;
  input_index: number;
  kind: string;
  object_id: string | null;
  checkpoint_seq: string;
  timestamp: string;
}

export interface CommandMessage {
  tx_digest: string;
  command_index: number;
  kind: string;
  package: string | null;
  module: string | null;
  function: string | null;
  checkpoint_seq: string;
  timestamp: string;
}

export interface SystemTransactionMessage {
  tx_digest: string;
  kind: string;
  data: string;
  checkpoint_seq: string;
  timestamp: string;
}

export interface UnchangedConsensusObjectMessage {
  tx_digest: string;
  object_id: string;
  kind: string;
  checkpoint_seq: string;
  timestamp: string;
}

export interface RawEventMessage {
  tx_digest: string;
  event_seq: number;
  package_id: string;
  module: string;
  event_type: string;
  sender: string;
  contents: string;
  checkpoint_seq: string;
  timestamp: string;
}

/** All NATS subject names */
export type NatsSubject =
  | "jun.checkpoints"
  | "jun.transactions"
  | "jun.move_calls"
  | "jun.balance_changes"
  | "jun.object_changes"
  | "jun.transaction_dependencies"
  | "jun.transaction_inputs"
  | "jun.commands"
  | "jun.system_transactions"
  | "jun.unchanged_consensus_objects"
  | "jun.raw_events";

/** Map subject → message type */
export interface NatsMessageMap {
  "jun.checkpoints": CheckpointMessage;
  "jun.transactions": TransactionMessage;
  "jun.move_calls": MoveCallMessage;
  "jun.balance_changes": BalanceChangeMessage;
  "jun.object_changes": ObjectChangeMessage;
  "jun.transaction_dependencies": TransactionDependencyMessage;
  "jun.transaction_inputs": TransactionInputMessage;
  "jun.commands": CommandMessage;
  "jun.system_transactions": SystemTransactionMessage;
  "jun.unchanged_consensus_objects": UnchangedConsensusObjectMessage;
  "jun.raw_events": RawEventMessage;
}

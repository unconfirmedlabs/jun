/**
 * NATS broadcast — publishes all record types to structured subjects.
 *
 * Subject hierarchy:
 *   {prefix}.checkpoints
 *   {prefix}.transactions
 *   {prefix}.move_calls
 *   {prefix}.balance_changes
 *   {prefix}.object_changes
 *   {prefix}.transaction_dependencies
 *   {prefix}.transaction_inputs
 *   {prefix}.commands
 *   {prefix}.system_transactions
 *   {prefix}.unchanged_consensus_objects
 *   {prefix}.raw_events
 *
 * Default prefix: "jun.sui" — decoded protocol events use "jun.{protocol}"
 */
import { connect, type NatsConnection, StringCodec } from "nats";
import type { Broadcast, ProcessedCheckpoint } from "../types.ts";
import type { Logger } from "../../logger.ts";
import { createLogger } from "../../logger.ts";

export interface NatsDestinationConfig {
  /** NATS server URL */
  url: string;
  /** Subject prefix (default: "jun") */
  prefix?: string;
}

export function createNatsBroadcast(config: NatsDestinationConfig): Broadcast {
  const log: Logger = createLogger().child({ component: "destination:nats" });
  const prefix = config.prefix ?? "jun.sui";
  const sc = StringCodec();
  let nc: NatsConnection | null = null;

  function pub(subject: string, data: Record<string, unknown>): void {
    if (!nc) return;
    try {
      nc.publish(`${prefix}.${subject}`, sc.encode(JSON.stringify(data)));
    } catch (error) {
      log.warn({ error, subject }, "NATS publish failed");
    }
  }

  return {
    name: "nats",

    async initialize(): Promise<void> {
      nc = await connect({
        servers: config.url,
        maxReconnectAttempts: -1,
        reconnectTimeWait: 2000,
      });
      log.info({ url: config.url, prefix }, "NATS connected");
    },

    push(processed: ProcessedCheckpoint): void {
      const cp = processed.checkpoint;

      pub("checkpoints", {
        sequence_number: cp.sequenceNumber.toString(),
        epoch: (cp.epoch ?? 0n).toString(),
        digest: cp.digest,
        timestamp: cp.timestamp.toISOString(),
        tx_count: cp.transactions.length,
      });

      for (const tx of processed.transactions) {
        pub("transactions", {
          digest: tx.digest,
          sender: tx.sender,
          success: tx.success,
          computation_cost: tx.computationCost,
          storage_cost: tx.storageCost,
          storage_rebate: tx.storageRebate,
          move_call_count: tx.moveCallCount,
          checkpoint_seq: tx.checkpointSeq.toString(),
          timestamp: tx.timestamp.toISOString(),
          epoch: (tx.epoch ?? 0n).toString(),
        });
      }

      for (const mc of processed.moveCalls) {
        pub("move_calls", {
          tx_digest: mc.txDigest,
          call_index: mc.callIndex,
          package: mc.package,
          module: mc.module,
          function: mc.function,
          checkpoint_seq: mc.checkpointSeq.toString(),
          timestamp: mc.timestamp.toISOString(),
        });
      }

      for (const bc of processed.balanceChanges) {
        pub("balance_changes", {
          tx_digest: bc.txDigest,
          checkpoint_seq: bc.checkpointSeq.toString(),
          address: bc.address,
          coin_type: bc.coinType,
          amount: bc.amount,
          timestamp: bc.timestamp.toISOString(),
        });
      }

      for (const oc of processed.objectChanges) {
        pub("object_changes", {
          tx_digest: oc.txDigest,
          object_id: oc.objectId,
          change_type: oc.changeType,
          object_type: oc.objectType,
          checkpoint_seq: oc.checkpointSeq.toString(),
          timestamp: oc.timestamp.toISOString(),
        });
      }

      for (const dep of processed.dependencies) {
        pub("transaction_dependencies", {
          tx_digest: dep.txDigest,
          depends_on_digest: dep.dependsOnDigest,
          checkpoint_seq: dep.checkpointSeq.toString(),
          timestamp: dep.timestamp.toISOString(),
        });
      }

      for (const inp of processed.inputs) {
        pub("transaction_inputs", {
          tx_digest: inp.txDigest,
          input_index: inp.inputIndex,
          kind: inp.kind,
          object_id: inp.objectId,
          checkpoint_seq: inp.checkpointSeq.toString(),
          timestamp: inp.timestamp.toISOString(),
        });
      }

      for (const cmd of processed.commands) {
        pub("commands", {
          tx_digest: cmd.txDigest,
          command_index: cmd.commandIndex,
          kind: cmd.kind,
          package: cmd.package,
          module: cmd.module,
          function: cmd.function,
          checkpoint_seq: cmd.checkpointSeq.toString(),
          timestamp: cmd.timestamp.toISOString(),
        });
      }

      for (const sys of processed.systemTransactions) {
        pub("system_transactions", {
          tx_digest: sys.txDigest,
          kind: sys.kind,
          data: sys.data,
          checkpoint_seq: sys.checkpointSeq.toString(),
          timestamp: sys.timestamp.toISOString(),
        });
      }

      for (const uco of processed.unchangedConsensusObjects) {
        pub("unchanged_consensus_objects", {
          tx_digest: uco.txDigest,
          object_id: uco.objectId,
          kind: uco.kind,
          checkpoint_seq: uco.checkpointSeq.toString(),
          timestamp: uco.timestamp.toISOString(),
        });
      }

      for (const ev of processed.rawEvents) {
        pub("raw_events", {
          tx_digest: ev.txDigest,
          event_seq: ev.eventSeq,
          package_id: ev.packageId,
          module: ev.module,
          event_type: ev.eventType,
          sender: ev.sender,
          contents: ev.contents,
          checkpoint_seq: ev.checkpointSeq.toString(),
          timestamp: ev.timestamp.toISOString(),
        });
      }
    },

    async shutdown(): Promise<void> {
      if (nc) {
        await nc.drain();
        nc = null;
      }
      log.info("NATS disconnected");
    },
  };
}

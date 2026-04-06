/**
 * Stdout broadcast — JSONL, one record per line with type prefix.
 */
import type { Broadcast, ProcessedCheckpoint } from "../types.ts";

export function createStdoutBroadcast(): Broadcast {
  return {
    name: "stdout",
    async initialize(): Promise<void> {},

    push(processed: ProcessedCheckpoint): void {
      const cp = processed.checkpoint;
      console.log(JSON.stringify({
        type: "checkpoint",
        sequence_number: cp.sequenceNumber.toString(),
        epoch: (cp.epoch ?? 0n).toString(),
        timestamp: cp.timestamp.toISOString(),
        tx_count: cp.transactions.length,
      }));

      for (const tx of processed.transactions) {
        console.log(JSON.stringify({ type: "transaction", ...tx, checkpointSeq: tx.checkpointSeq.toString(), epoch: (tx.epoch ?? 0n).toString(), timestamp: tx.timestamp.toISOString() }));
      }
      for (const mc of processed.moveCalls) {
        console.log(JSON.stringify({ type: "move_call", ...mc, checkpointSeq: mc.checkpointSeq.toString(), timestamp: mc.timestamp.toISOString() }));
      }
      for (const bc of processed.balanceChanges) {
        console.log(JSON.stringify({ type: "balance_change", ...bc, checkpointSeq: bc.checkpointSeq.toString(), timestamp: bc.timestamp.toISOString() }));
      }
      for (const oc of processed.objectChanges) {
        console.log(JSON.stringify({ type: "object_change", ...oc, checkpointSeq: oc.checkpointSeq.toString(), timestamp: oc.timestamp.toISOString() }));
      }
      for (const dep of processed.dependencies) {
        console.log(JSON.stringify({ type: "dependency", ...dep, checkpointSeq: dep.checkpointSeq.toString(), timestamp: dep.timestamp.toISOString() }));
      }
      for (const inp of processed.inputs) {
        console.log(JSON.stringify({ type: "input", ...inp, checkpointSeq: inp.checkpointSeq.toString(), timestamp: inp.timestamp.toISOString() }));
      }
      for (const cmd of processed.commands) {
        console.log(JSON.stringify({ type: "command", ...cmd, checkpointSeq: cmd.checkpointSeq.toString(), timestamp: cmd.timestamp.toISOString() }));
      }
      for (const sys of processed.systemTransactions) {
        console.log(JSON.stringify({ type: "system_transaction", ...sys, checkpointSeq: sys.checkpointSeq.toString(), timestamp: sys.timestamp.toISOString() }));
      }
      for (const uco of processed.unchangedConsensusObjects) {
        console.log(JSON.stringify({ type: "unchanged_consensus_object", ...uco, checkpointSeq: uco.checkpointSeq.toString(), timestamp: uco.timestamp.toISOString() }));
      }
      for (const ev of processed.rawEvents) {
        console.log(JSON.stringify({ type: "raw_event", ...ev, checkpointSeq: ev.checkpointSeq.toString(), timestamp: ev.timestamp.toISOString() }));
      }
    },

    async shutdown(): Promise<void> {},
  };
}

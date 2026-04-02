/**
 * Fast protobuf wire format parser for Sui checkpoint messages.
 *
 * Replaces protobufjs Checkpoint.decode() + Checkpoint.toObject() with a
 * single-pass parser that extracts only the fields needed by the BCS
 * decoders and balance computation, skipping everything else.
 *
 * Output matches the Checkpoint.toObject({ longs: String, enums: String, defaults: false })
 * structure so downstream code doesn't need changes.
 *
 * Protobuf wire format:
 *   Field tag = (field_number << 3) | wire_type
 *   Wire type 0 = varint, 2 = length-delimited, 1 = 64-bit, 5 = 32-bit
 *
 * All byte fields (bcs.value, signature) are returned as Uint8Array subarrays
 * of the original decompressed buffer (zero-copy).
 */

// ---------------------------------------------------------------------------
// Wire format primitives
// ---------------------------------------------------------------------------

/** Read a protobuf varint (up to 32 bits). Returns [value, newOffset]. */
function readVarint(buf: Uint8Array, offset: number): [number, number] {
  let value = 0;
  let shift = 0;
  for (;;) {
    const byte = buf[offset]!;
    offset++;
    value |= (byte & 0x7f) << shift;
    if ((byte & 0x80) === 0) return [value, offset];
    shift += 7;
  }
}

/** Read a protobuf varint as a string (for uint64 fields, matching longs: String). */
function readVarintString(buf: Uint8Array, offset: number): [string, number] {
  // Fast path: single byte (value 0-127, very common for small numbers)
  if ((buf[offset]! & 0x80) === 0) {
    return [String(buf[offset]!), offset + 1];
  }
  // General path: use BigInt for multi-byte
  let value = 0n;
  let shift = 0n;
  for (;;) {
    const byte = buf[offset]!;
    offset++;
    value |= BigInt(byte & 0x7f) << shift;
    if ((byte & 0x80) === 0) return [value.toString(), offset];
    shift += 7n;
  }
}

/** Read a length-delimited field and return [dataSlice, offsetAfterField]. */
function readLengthDelimited(buf: Uint8Array, offset: number): [Uint8Array, number] {
  const [length, newOffset] = readVarint(buf, offset);
  return [buf.subarray(newOffset, newOffset + length), newOffset + length];
}

/** Skip a protobuf field based on its wire type. Returns new offset. */
function skipField(buf: Uint8Array, offset: number, wireType: number): number {
  switch (wireType) {
    case 0: { // varint — skip bytes until MSB is 0
      while (buf[offset]! & 0x80) offset++;
      return offset + 1;
    }
    case 1: return offset + 8;  // 64-bit fixed
    case 2: { // length-delimited
      const [length, newOffset] = readVarint(buf, offset);
      return newOffset + length;
    }
    case 5: return offset + 4;  // 32-bit fixed
    default: return -1; // Unknown wire type
  }
}

/** Decode a protobuf UTF-8 string from a Uint8Array slice (ASCII fast path). */
function decodeString(buf: Uint8Array): string {
  let str = "";
  for (let i = 0; i < buf.length; i++) {
    str += String.fromCharCode(buf[i]!);
  }
  return str;
}

// ---------------------------------------------------------------------------
// Output types (matches Checkpoint.toObject structure)
// ---------------------------------------------------------------------------

export interface FastProtoCheckpoint {
  sequenceNumber: string;
  summary: { bcs: { value: Uint8Array }; } | null;
  signature: { epoch: string; signature: Uint8Array; bitmap: Uint8Array } | null;
  contents: { bcs: { value: Uint8Array } } | null;
  transactions: FastProtoTransaction[];
  objects: { objects: FastProtoObject[] } | null;
}

export interface FastProtoTransaction {
  digest: string;
  transaction: { bcs: { value: Uint8Array } } | null;
  effects: { bcs: { value: Uint8Array } } | null;
  events: { bcs: { value: Uint8Array } } | null;
  balanceChanges: { address: string; coinType: string; amount: string }[];
}

export interface FastProtoObject {
  bcs: { value: Uint8Array };
  objectId: string;
  version: string;
}

// ---------------------------------------------------------------------------
// Message parsers
// ---------------------------------------------------------------------------

/** Extract Bcs.value (field 2) from a Bcs message. */
function parseBcsValue(buf: Uint8Array): Uint8Array | null {
  let offset = 0;
  const end = buf.length;
  while (offset < end) {
    const [tag, tagEnd] = readVarint(buf, offset);
    offset = tagEnd;
    const fieldNumber = tag >>> 3;
    const wireType = tag & 7;

    if (wireType === 2) {
      const [data, fieldEnd] = readLengthDelimited(buf, offset);
      if (fieldNumber === 2) return data; // Bcs.value
      offset = fieldEnd;
    } else {
      offset = skipField(buf, offset, wireType);
    }
  }
  return null;
}

/** Parse a wrapper message that has bcs at field 1. Returns { bcs: { value } }. */
function parseBcsWrapper(buf: Uint8Array): { bcs: { value: Uint8Array } } | null {
  let offset = 0;
  const end = buf.length;
  while (offset < end) {
    const [tag, tagEnd] = readVarint(buf, offset);
    offset = tagEnd;
    const fieldNumber = tag >>> 3;
    const wireType = tag & 7;

    if (wireType === 2) {
      const [data, fieldEnd] = readLengthDelimited(buf, offset);
      if (fieldNumber === 1) {
        // This is the Bcs submessage — extract its value field
        const value = parseBcsValue(data);
        if (value) return { bcs: { value } };
      }
      offset = fieldEnd;
    } else {
      offset = skipField(buf, offset, wireType);
    }
  }
  return null;
}

/** Parse ValidatorAggregatedSignature (epoch=1, signature=2, bitmap=3). */
function parseSignature(buf: Uint8Array): FastProtoCheckpoint["signature"] {
  let offset = 0;
  const end = buf.length;
  let epoch = "0";
  let signature: Uint8Array | null = null;
  let bitmap: Uint8Array | null = null;

  while (offset < end) {
    const [tag, tagEnd] = readVarint(buf, offset);
    offset = tagEnd;
    const fieldNumber = tag >>> 3;
    const wireType = tag & 7;

    if (wireType === 0) {
      const [val, newOffset] = readVarintString(buf, offset);
      offset = newOffset;
      if (fieldNumber === 1) epoch = val;
    } else if (wireType === 2) {
      const [data, fieldEnd] = readLengthDelimited(buf, offset);
      offset = fieldEnd;
      if (fieldNumber === 2) signature = data;
      else if (fieldNumber === 3) bitmap = data;
    } else {
      offset = skipField(buf, offset, wireType);
    }
  }

  if (!signature || !bitmap) return null;
  return { epoch, signature, bitmap };
}

/** Parse BalanceChange (address=1, coin_type=2, amount=3). */
function parseBalanceChange(buf: Uint8Array): { address: string; coinType: string; amount: string } {
  let offset = 0;
  const end = buf.length;
  let address = "";
  let coinType = "";
  let amount = "0";

  while (offset < end) {
    const [tag, tagEnd] = readVarint(buf, offset);
    offset = tagEnd;
    const fieldNumber = tag >>> 3;
    const wireType = tag & 7;

    if (wireType === 2) {
      const [data, fieldEnd] = readLengthDelimited(buf, offset);
      offset = fieldEnd;
      if (fieldNumber === 1) address = decodeString(data);
      else if (fieldNumber === 2) coinType = decodeString(data);
      else if (fieldNumber === 3) amount = decodeString(data);
    } else {
      offset = skipField(buf, offset, wireType);
    }
  }

  return { address, coinType, amount };
}

/** Parse ExecutedTransaction. */
function parseTransaction(buf: Uint8Array): FastProtoTransaction {
  let offset = 0;
  const end = buf.length;
  let digest = "";
  let transaction: { bcs: { value: Uint8Array } } | null = null;
  let effects: { bcs: { value: Uint8Array } } | null = null;
  let events: { bcs: { value: Uint8Array } } | null = null;
  const balanceChanges: { address: string; coinType: string; amount: string }[] = [];

  while (offset < end) {
    const [tag, tagEnd] = readVarint(buf, offset);
    offset = tagEnd;
    const fieldNumber = tag >>> 3;
    const wireType = tag & 7;

    if (wireType === 2) {
      const [data, fieldEnd] = readLengthDelimited(buf, offset);
      offset = fieldEnd;

      switch (fieldNumber) {
        case 1: digest = decodeString(data); break;                  // digest
        case 2: transaction = parseBcsWrapper(data); break;           // transaction
        case 4: effects = parseBcsWrapper(data); break;               // effects
        case 5: events = parseBcsWrapper(data); break;                // events
        case 8: balanceChanges.push(parseBalanceChange(data)); break; // balance_changes
        // Skip: signatures(3), checkpoint(6), timestamp(7), objects(9)
      }
    } else if (wireType === 0) {
      // Skip varint fields (checkpoint=6)
      offset = skipField(buf, offset, 0);
    } else {
      offset = skipField(buf, offset, wireType);
    }
  }

  return { digest, transaction, effects, events, balanceChanges };
}

/** Parse Object (bcs=1, object_id=2, version=3). */
function parseObject(buf: Uint8Array): FastProtoObject | null {
  let offset = 0;
  const end = buf.length;
  let bcsValue: Uint8Array | null = null;
  let objectId = "";
  let version = "0";

  while (offset < end) {
    const [tag, tagEnd] = readVarint(buf, offset);
    offset = tagEnd;
    const fieldNumber = tag >>> 3;
    const wireType = tag & 7;

    if (wireType === 2) {
      const [data, fieldEnd] = readLengthDelimited(buf, offset);
      offset = fieldEnd;

      if (fieldNumber === 1) {
        // Bcs wrapper
        bcsValue = parseBcsValue(data);
      } else if (fieldNumber === 2) {
        objectId = decodeString(data);
      }
      // Skip: digest(4), owner(5), object_type(6), etc.
    } else if (wireType === 0) {
      const [val, newOffset] = readVarintString(buf, offset);
      offset = newOffset;
      if (fieldNumber === 3) version = val;
    } else {
      offset = skipField(buf, offset, wireType);
    }
  }

  if (!bcsValue) return null;
  return { bcs: { value: bcsValue }, objectId, version };
}

/** Parse ObjectSet (repeated Object at field 1). */
function parseObjectSet(buf: Uint8Array): FastProtoObject[] {
  let offset = 0;
  const end = buf.length;
  const objects: FastProtoObject[] = [];

  while (offset < end) {
    const [tag, tagEnd] = readVarint(buf, offset);
    offset = tagEnd;
    const fieldNumber = tag >>> 3;
    const wireType = tag & 7;

    if (wireType === 2) {
      const [data, fieldEnd] = readLengthDelimited(buf, offset);
      offset = fieldEnd;
      if (fieldNumber === 1) {
        const obj = parseObject(data);
        if (obj) objects.push(obj);
      }
    } else {
      offset = skipField(buf, offset, wireType);
    }
  }

  return objects;
}

// ---------------------------------------------------------------------------
// Main entry point
// ---------------------------------------------------------------------------

/**
 * Parse a decompressed checkpoint protobuf into the minimal structure needed
 * by decodeCheckpointFromProto() and computeBalanceChangesFromArchive().
 *
 * Drop-in replacement for:
 *   Checkpoint.decode(decompressed) → Checkpoint.toObject(decoded, { longs: String, ... })
 */
export function parseCheckpointProto(buf: Uint8Array): FastProtoCheckpoint {
  let offset = 0;
  const end = buf.length;

  let sequenceNumber = "0";
  let summary: FastProtoCheckpoint["summary"] = null;
  let signature: FastProtoCheckpoint["signature"] = null;
  let contents: FastProtoCheckpoint["contents"] = null;
  const transactions: FastProtoTransaction[] = [];
  let objects: FastProtoCheckpoint["objects"] = null;

  while (offset < end) {
    const [tag, tagEnd] = readVarint(buf, offset);
    offset = tagEnd;
    const fieldNumber = tag >>> 3;
    const wireType = tag & 7;

    if (wireType === 0) {
      const [val, newOffset] = readVarintString(buf, offset);
      offset = newOffset;
      if (fieldNumber === 1) sequenceNumber = val;
    } else if (wireType === 2) {
      const [data, fieldEnd] = readLengthDelimited(buf, offset);
      offset = fieldEnd;

      switch (fieldNumber) {
        case 3: summary = parseBcsWrapper(data); break;
        case 4: signature = parseSignature(data); break;
        case 5: contents = parseBcsWrapper(data); break;
        case 6: transactions.push(parseTransaction(data)); break;
        case 7: objects = { objects: parseObjectSet(data) }; break;
        // Skip: digest(2)
      }
    } else {
      offset = skipField(buf, offset, wireType);
    }
  }

  return { sequenceNumber, summary, signature, contents, transactions, objects };
}

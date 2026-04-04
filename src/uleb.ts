/**
 * ULEB128 variable-length integer encoding — shared across BCS parsing.
 */

/**
 * Read a ULEB128 integer from a buffer at the given offset.
 * Returns [value, bytesConsumed].
 */
export function readUleb128(buf: Uint8Array, offset: number): [number, number] {
  let value = 0;
  let shift = 0;
  let pos = offset;
  for (;;) {
    const byte = buf[pos]!;
    value |= (byte & 0x7f) << shift;
    pos++;
    if ((byte & 0x80) === 0) break;
    shift += 7;
  }
  return [value, pos - offset];
}

/**
 * Read a BCS string (ULEB128 length prefix + UTF-8 bytes).
 * Returns [string, newOffset].
 */
export function readBcsString(buf: Uint8Array, offset: number): [string, number] {
  const [len, lenBytes] = readUleb128(buf, offset);
  offset += lenBytes;
  const str = new TextDecoder().decode(buf.slice(offset, offset + len));
  return [str, offset + len];
}

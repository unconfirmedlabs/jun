// Unified checkpoint processor — single Zig FFI call from compressed bytes to
// fully decoded balance changes + transactions + events.
//
// Entry points:
//   process_checkpoint_compressed — compressed bytes → balance changes + transactions + events (main)
//   compute_balance_changes       — decompressed proto → balance changes only (legacy)
//   compute_balance_changes_compressed — compressed → balance changes only (legacy)
//
// Output format for process_checkpoint_compressed:
//   u32 num_balance_changes
//   u32 num_events
//   u32 num_transactions
//   u64 timestamp_ms
//   Balance changes:
//     u16 owner_len + owner + u16 coinType_len + coinType + i64 amount
//   Transactions:
//     u16 digest_len + digest(base58)
//     u16 sender_len + sender(hex)
//     u8 success
//     u64 computation_cost
//     u64 storage_cost
//     u64 storage_rebate
//     u16 num_events
//     u16 num_move_calls
//     Move calls:
//       u16 package_len + package(hex)
//       u16 module_len + module
//       u16 function_len + function
//   Events:
//     u16 package_len + package + u16 module_len + module + u16 sender_len + sender
//     + u16 event_type_len + event_type + u32 contents_len + contents

const std = @import("std");

// --- Constants ---

const HEX: [16]u8 = "0123456789abcdef".*;
const NORMALIZED_SUI = "0x0000000000000000000000000000000000000000000000000000000000000002::sui::SUI";

const MAX_TX: u32 = 4096;
const MAX_OBJECTS: u32 = 16384;
const MAX_COINS: u32 = 8192;
const MAX_CREATED: u32 = 4096;
const MAX_DELETED: u32 = 4096;
const MAX_AGG: u32 = 4096;
const SCRATCH_SIZE: u32 = 512 * 1024;
const BASE58_ALPHABET: [58]u8 = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz".*;

// --- Protobuf primitives ---

fn readVarint(buf: []const u8, pos: u32) ?struct { val: u64, end: u32 } {
    var v: u64 = 0;
    var s: u6 = 0;
    var p = pos;
    while (p < buf.len) {
        const b = buf[p];
        p += 1;
        v |= @as(u64, b & 0x7F) << s;
        if (b & 0x80 == 0) return .{ .val = v, .end = p };
        s +|= 7;
        if (s > 63) return null;
    }
    return null;
}

fn skipPbField(buf: []const u8, pos: u32, wt: u3) ?u32 {
    return switch (wt) {
        0 => blk: {
            var p = pos;
            while (p < buf.len) {
                if (buf[p] & 0x80 == 0) break :blk p + 1;
                p += 1;
            }
            break :blk null;
        },
        1 => if (pos + 8 <= buf.len) pos + 8 else null,
        2 => blk: {
            const r = readVarint(buf, pos) orelse break :blk null;
            const e = r.end + @as(u32, @intCast(r.val));
            break :blk if (e <= buf.len) e else null;
        },
        5 => if (pos + 4 <= buf.len) pos + 4 else null,
        else => null,
    };
}

fn readLen(buf: []const u8, pos: u32) ?struct { off: u32, len: u32, end: u32 } {
    const r = readVarint(buf, pos) orelse return null;
    const l: u32 = @intCast(r.val);
    const e = r.end + l;
    if (e > buf.len) return null;
    return .{ .off = r.end, .len = l, .end = e };
}

const BcsSlice = struct { off: u32, len: u32 };

fn bcsValue(buf: []const u8, off: u32, len: u32) ?BcsSlice {
    const end = off + len;
    var p = off;
    while (p < end) {
        const t = readVarint(buf, p) orelse return null;
        p = t.end;
        if (@as(u3, @intCast(t.val & 7)) == 2) {
            const ld = readLen(buf, p) orelse return null;
            if (@as(u32, @intCast(t.val >> 3)) == 2) return .{ .off = ld.off, .len = ld.len };
            p = ld.end;
        } else {
            p = skipPbField(buf, p, @intCast(t.val & 7)) orelse return null;
        }
    }
    return null;
}

fn bcsWrapper(buf: []const u8, off: u32, len: u32) ?BcsSlice {
    const end = off + len;
    var p = off;
    while (p < end) {
        const t = readVarint(buf, p) orelse return null;
        p = t.end;
        if (@as(u3, @intCast(t.val & 7)) == 2) {
            const ld = readLen(buf, p) orelse return null;
            if (@as(u32, @intCast(t.val >> 3)) == 1) return bcsValue(buf, ld.off, ld.len);
            p = ld.end;
        } else {
            p = skipPbField(buf, p, @intCast(t.val & 7)) orelse return null;
        }
    }
    return null;
}

// --- BCS primitives ---

fn readUleb(buf: []const u8, pos: u32) ?struct { val: u32, end: u32 } {
    var v: u32 = 0;
    var s: u5 = 0;
    var p = pos;
    while (p < buf.len) {
        const b = buf[p];
        p += 1;
        v |= @as(u32, b & 0x7F) << s;
        if (b & 0x80 == 0) return .{ .val = v, .end = p };
        s +|= 7;
    }
    return null;
}

fn skipBcsTypeTag(buf: []const u8, pos: u32) ?u32 {
    if (pos >= buf.len) return null;
    const tag = buf[pos];
    var p = pos + 1;
    return switch (tag) {
        0, 1, 2, 3, 4, 5, 8, 9, 10 => p,
        6 => skipBcsTypeTag(buf, p),
        7 => blk: { // struct
            p += 32; // address
            const ml = readUleb(buf, p) orelse break :blk null;
            p = ml.end + ml.val;
            const nl = readUleb(buf, p) orelse break :blk null;
            p = nl.end + nl.val;
            const pc = readUleb(buf, p) orelse break :blk null;
            p = pc.end;
            for (0..pc.val) |_| {
                p = skipBcsTypeTag(buf, p) orelse break :blk null;
            }
            break :blk p;
        },
        else => null,
    };
}

fn skipVecU8(buf: []const u8, pos: u32) ?u32 {
    const r = readUleb(buf, pos) orelse return null;
    return r.end + r.val;
}

fn skipOwner(buf: []const u8, pos: u32) ?u32 {
    if (pos >= buf.len) return null;
    return switch (buf[pos]) {
        0, 1 => pos + 1 + 32,
        2 => pos + 1 + 8,
        3 => pos + 1,
        4 => pos + 1 + 40,
        else => null,
    };
}

fn skipOptionU64(buf: []const u8, pos: u32) ?u32 {
    if (pos >= buf.len) return null;
    return switch (buf[pos]) {
        0 => pos + 1,
        1 => if (pos + 9 <= buf.len) pos + 9 else null,
        else => null,
    };
}

fn skipOptionString(buf: []const u8, pos: u32) ?u32 {
    if (pos >= buf.len) return null;
    return switch (buf[pos]) {
        0 => pos + 1,
        1 => blk: {
            const s = readBcsStr(buf, pos + 1) orelse break :blk null;
            break :blk s.end;
        },
        else => null,
    };
}

fn skipVecVecU8(buf: []const u8, pos: u32) ?u32 {
    const cnt = readUleb(buf, pos) orelse return null;
    var p = cnt.end;
    for (0..cnt.val) |_| p = skipVecU8(buf, p) orelse return null;
    return p;
}

fn skipVecAddresses(buf: []const u8, pos: u32) ?u32 {
    const cnt = readUleb(buf, pos) orelse return null;
    const bytes = cnt.val * 32;
    return if (cnt.end + bytes <= buf.len) cnt.end + bytes else null;
}

fn skipObjectDigestVec(buf: []const u8, pos: u32) ?u32 {
    return skipVecU8(buf, pos);
}

fn skipSuiObjectRef(buf: []const u8, pos: u32) ?u32 {
    var p = pos;
    if (p + 32 + 8 > buf.len) return null;
    p += 32; // object id
    p += 8; // version
    p = skipVecU8(buf, p) orelse return null; // digest
    return p;
}

fn skipSharedObjectRef(buf: []const u8, pos: u32) ?u32 {
    return if (pos + 32 + 8 + 1 <= buf.len) pos + 41 else null;
}

fn skipObjectArg(buf: []const u8, pos: u32) ?u32 {
    const tag = readUleb(buf, pos) orelse return null;
    const p = tag.end;
    return switch (tag.val) {
        0, 2 => skipSuiObjectRef(buf, p),
        1 => skipSharedObjectRef(buf, p),
        else => null,
    };
}

fn skipFundsWithdrawal(buf: []const u8, pos: u32) ?u32 {
    var p = pos;

    // Reservation enum
    const reservation = readUleb(buf, p) orelse return null;
    p = reservation.end;
    if (reservation.val != 0 or p + 8 > buf.len) return null;
    p += 8;

    // WithdrawalType enum
    const withdrawal_type = readUleb(buf, p) orelse return null;
    p = withdrawal_type.end;
    if (withdrawal_type.val != 0) return null;
    p = skipBcsTypeTag(buf, p) orelse return null;

    // WithdrawFrom enum (null variants only)
    const withdraw_from = readUleb(buf, p) orelse return null;
    if (withdraw_from.val > 1) return null;
    return withdraw_from.end;
}

fn skipCallArg(buf: []const u8, pos: u32) ?u32 {
    const tag = readUleb(buf, pos) orelse return null;
    const p = tag.end;
    return switch (tag.val) {
        0 => blk: {
            const pure = readBcsStr(buf, p) orelse break :blk null;
            break :blk pure.end;
        },
        1 => skipObjectArg(buf, p),
        2 => skipFundsWithdrawal(buf, p),
        else => null,
    };
}

fn skipVecCallArgs(buf: []const u8, pos: u32) ?u32 {
    const cnt = readUleb(buf, pos) orelse return null;
    var p = cnt.end;
    for (0..cnt.val) |_| p = skipCallArg(buf, p) orelse return null;
    return p;
}

fn skipArgument(buf: []const u8, pos: u32) ?u32 {
    const tag = readUleb(buf, pos) orelse return null;
    return switch (tag.val) {
        0 => tag.end,
        1, 2 => if (tag.end + 2 <= buf.len) tag.end + 2 else null,
        3 => if (tag.end + 4 <= buf.len) tag.end + 4 else null,
        else => null,
    };
}

fn skipVecArguments(buf: []const u8, pos: u32) ?u32 {
    const cnt = readUleb(buf, pos) orelse return null;
    var p = cnt.end;
    for (0..cnt.val) |_| p = skipArgument(buf, p) orelse return null;
    return p;
}

fn skipVecTypeTags(buf: []const u8, pos: u32) ?u32 {
    const cnt = readUleb(buf, pos) orelse return null;
    var p = cnt.end;
    for (0..cnt.val) |_| p = skipBcsTypeTag(buf, p) orelse return null;
    return p;
}

fn skipOptionTypeTag(buf: []const u8, pos: u32) ?u32 {
    const tag = readUleb(buf, pos) orelse return null;
    return switch (tag.val) {
        0 => tag.end,
        1 => skipBcsTypeTag(buf, tag.end),
        else => null,
    };
}

// --- Hex encoding ---

fn bytesToHex(raw: []const u8, out: []u8) void {
    out[0] = '0';
    out[1] = 'x';
    for (raw, 0..) |b, i| {
        out[2 + i * 2] = HEX[b >> 4];
        out[2 + i * 2 + 1] = HEX[b & 0x0F];
    }
}

fn hexDecode(hex: []const u8, out: *[32]u8) bool {
    var src = hex;
    if (src.len >= 2 and src[0] == '0' and (src[1] == 'x' or src[1] == 'X')) src = src[2..];
    if (src.len != 64) return false;
    for (0..32) |i| {
        const hi = hexVal(src[i * 2]) orelse return false;
        const lo = hexVal(src[i * 2 + 1]) orelse return false;
        out[i] = (@as(u8, hi) << 4) | @as(u8, lo);
    }
    return true;
}

fn hexVal(c: u8) ?u4 {
    if (c >= '0' and c <= '9') return @intCast(c - '0');
    if (c >= 'a' and c <= 'f') return @intCast(c - 'a' + 10);
    if (c >= 'A' and c <= 'F') return @intCast(c - 'A' + 10);
    return null;
}

fn base58Encode(raw: []const u8, out: []u8) ?u16 {
    if (raw.len == 0) return 0;

    var digits: [64]u8 = [_]u8{0} ** 64;
    var digits_len: usize = 1;

    for (raw) |byte| {
        var carry: u32 = byte;
        var i: usize = 0;
        while (i < digits_len) : (i += 1) {
            carry += @as(u32, digits[i]) * 256;
            digits[i] = @intCast(carry % 58);
            carry /= 58;
        }
        while (carry > 0) {
            if (digits_len >= digits.len) return null;
            digits[digits_len] = @intCast(carry % 58);
            digits_len += 1;
            carry /= 58;
        }
    }

    var leading_zeroes: usize = 0;
    while (leading_zeroes < raw.len and raw[leading_zeroes] == 0) : (leading_zeroes += 1) {}

    const total_len = leading_zeroes + digits_len;
    if (total_len > out.len) return null;

    for (0..leading_zeroes) |i| out[i] = '1';
    for (0..digits_len) |i| out[total_len - 1 - i] = BASE58_ALPHABET[digits[i]];
    return @intCast(total_len);
}

// --- BCS string reader ---

fn readBcsStr(buf: []const u8, pos: u32) ?struct { off: u32, len: u32, end: u32 } {
    const r = readUleb(buf, pos) orelse return null;
    return .{ .off = r.end, .len = r.val, .end = r.end + r.val };
}

// --- TypeTag formatter (writes to scratch) ---

const Scratch = struct {
    buf: []u8,
    pos: u32,

    fn write(self: *Scratch, data: []const u8) bool {
        if (self.pos + data.len > self.buf.len) return false;
        @memcpy(self.buf[self.pos..][0..data.len], data);
        self.pos += @intCast(data.len);
        return true;
    }

    fn writeHex32(self: *Scratch, raw: []const u8) bool {
        if (self.pos + 66 > self.buf.len) return false;
        bytesToHex(raw[0..32], self.buf[self.pos..][0..66]);
        self.pos += 66;
        return true;
    }
};

fn formatTypeTag(buf: []const u8, pos: u32, s: *Scratch) ?u32 {
    if (pos >= buf.len) return null;
    const tag = buf[pos];
    var p = pos + 1;
    const names = [_][]const u8{ "bool", "u8", "u64", "u128", "address", "signer", "vector", "struct", "u16", "u32", "u256" };

    switch (tag) {
        0, 1, 2, 3, 4, 5, 8, 9, 10 => {
            _ = s.write(names[tag]);
            return p;
        },
        6 => { // vector<T>
            _ = s.write("vector<");
            p = formatTypeTag(buf, p, s) orelse return null;
            _ = s.write(">");
            return p;
        },
        7 => { // struct
            if (p + 32 > buf.len) return null;
            _ = s.writeHex32(buf[p .. p + 32]);
            p += 32;
            _ = s.write("::");
            const mod = readBcsStr(buf, p) orelse return null;
            _ = s.write(buf[mod.off .. mod.off + mod.len]);
            p = mod.end;
            _ = s.write("::");
            const name = readBcsStr(buf, p) orelse return null;
            _ = s.write(buf[name.off .. name.off + name.len]);
            p = name.end;
            const pc = readUleb(buf, p) orelse return null;
            p = pc.end;
            if (pc.val > 0) {
                _ = s.write("<");
                for (0..pc.val) |i| {
                    if (i > 0) _ = s.write(", ");
                    p = formatTypeTag(buf, p, s) orelse return null;
                }
                _ = s.write(">");
            }
            return p;
        },
        else => return null,
    }
}

// --- Data types ---

const CoinSnap = struct {
    obj_id: [32]u8,
    version: u64,
    owner: [32]u8,
    has_owner: bool,
    ct_off: u32, // into scratch
    ct_len: u16,
    balance: u64,
};

const AggKey = struct {
    owner: [32]u8,
    ct_off: u32,
    ct_len: u16,
};

const AggEntry = struct {
    key: AggKey,
    delta: i128,
    occupied: bool,
};

// --- Aggregation (open-addressing hash table) ---

fn hashKey(owner: [32]u8, ct: []const u8) u32 {
    var h: u32 = 2166136261;
    for (owner) |b| {
        h ^= b;
        h *%= 16777619;
    }
    for (ct) |b| {
        h ^= b;
        h *%= 16777619;
    }
    return h;
}

fn addDelta(
    agg: []AggEntry,
    owner: [32]u8,
    ct_off: u32,
    ct_len: u16,
    delta: i128,
    scratch: []const u8,
) void {
    if (delta == 0) return;
    const ct = scratch[ct_off .. ct_off + ct_len];
    const h = hashKey(owner, ct);
    var idx = h % @as(u32, @intCast(agg.len));
    var probes: u32 = 0;
    while (probes < agg.len) : (probes += 1) {
        const e = &agg[idx];
        if (!e.occupied) {
            e.* = .{
                .key = .{ .owner = owner, .ct_off = ct_off, .ct_len = ct_len },
                .delta = delta,
                .occupied = true,
            };
            return;
        }
        if (std.mem.eql(u8, &e.key.owner, &owner) and
            std.mem.eql(u8, scratch[e.key.ct_off .. e.key.ct_off + e.key.ct_len], ct))
        {
            e.delta += delta;
            return;
        }
        idx = (idx + 1) % @as(u32, @intCast(agg.len));
    }
}

// --- Effects parser ---

fn parseChangedObjects(
    buf: []const u8,
    pos_in: u32,
    created: *[MAX_CREATED][32]u8,
    created_n: *u32,
    deleted: *[MAX_DELETED][32]u8,
    deleted_n: *u32,
    agg: []AggEntry,
    scratch: *Scratch,
) ?u32 {
    var pos = pos_in;
    const cnt = readUleb(buf, pos) orelse return null;
    pos = cnt.end;

    for (0..cnt.val) |_| {
        if (pos + 32 > buf.len) return null;
        const obj_id = buf[pos..][0..32];
        pos += 32;

        // inputState: ObjectIn
        if (pos >= buf.len) return null;
        const in_tag = buf[pos];
        pos += 1;
        if (in_tag == 1) { // Exist
            pos += 8; // version u64
            pos = skipVecU8(buf, pos) orelse return null; // digest
            pos = skipOwner(buf, pos) orelse return null;
        }

        // outputState: ObjectOut
        if (pos >= buf.len) return null;
        const out_tag = buf[pos];
        pos += 1;

        if (out_tag == 0) { // NotExist
            if (deleted_n.* < MAX_DELETED) {
                deleted[deleted_n.*] = obj_id.*;
                deleted_n.* += 1;
            }
        } else if (out_tag == 1) { // ObjectWrite
            pos = skipVecU8(buf, pos) orelse return null;
            pos = skipOwner(buf, pos) orelse return null;
            if (in_tag == 0 and created_n.* < MAX_CREATED) {
                created[created_n.*] = obj_id.*;
                created_n.* += 1;
            }
        } else if (out_tag == 2) { // PackageWrite
            pos += 8;
            pos = skipVecU8(buf, pos) orelse return null;
        } else if (out_tag == 3) { // AccumulatorWriteV1
            if (pos + 32 > buf.len) return null;
            const owner_raw = buf[pos..][0..32];
            pos += 32;

            // Parse Balance<CoinType> from TypeTag
            const ct_start = scratch.pos;
            const ct_result = parseBalanceCoinType(buf, pos, scratch);
            if (ct_result) |cr| {
                pos = cr.end;
                const ct_len: u16 = @intCast(scratch.pos - ct_start);

                // operation: 0=Merge, 1=Split
                if (pos >= buf.len) return null;
                const op = buf[pos];
                pos += 1;

                // value: 0=Integer(u64), 1=IntegerTuple, 2=EventDigest
                if (pos >= buf.len) return null;
                const vt = buf[pos];
                pos += 1;
                var amount: i128 = 0;
                if (vt == 0) {
                    if (pos + 8 > buf.len) return null;
                    amount = @intCast(std.mem.readInt(u64, buf[pos..][0..8], .little));
                    pos += 8;
                } else if (vt == 1) {
                    pos += 16;
                } else if (vt == 2) {
                    const ec = readUleb(buf, pos) orelse return null;
                    pos = ec.end;
                    for (0..ec.val) |_| {
                        pos += 8;
                        pos = skipVecU8(buf, pos) orelse return null;
                    }
                }

                if (op == 0) { // Merge
                    addDelta(agg, owner_raw.*, ct_start, ct_len, amount, scratch.buf);
                } else if (op == 1) { // Split
                    addDelta(agg, owner_raw.*, ct_start, ct_len, -amount, scratch.buf);
                }
            } else {
                // Not Balance<T>, skip TypeTag + operation + value
                pos = skipBcsTypeTag(buf, pos) orelse return null;
                pos += 1; // operation
                if (pos >= buf.len) return null;
                const vt = buf[pos];
                pos += 1;
                if (vt == 0) pos += 8 else if (vt == 1) pos += 16 else if (vt == 2) {
                    const ec = readUleb(buf, pos) orelse return null;
                    pos = ec.end;
                    for (0..ec.val) |_| {
                        pos += 8;
                        pos = skipVecU8(buf, pos) orelse return null;
                    }
                }
            }
        } else return null;

        // idOperation
        pos += 1;
    }
    return pos;
}

fn parseBalanceCoinType(buf: []const u8, pos: u32, s: *Scratch) ?struct { end: u32 } {
    if (pos >= buf.len or buf[pos] != 7) return null; // must be struct
    var p = pos + 1;
    if (p + 32 > buf.len) return null;
    p += 32; // skip address
    const mod = readBcsStr(buf, p) orelse return null;
    p = mod.end;
    const name = readBcsStr(buf, p) orelse return null;
    p = name.end;
    const pc = readUleb(buf, p) orelse return null;
    p = pc.end;

    // Check if balance::Balance
    const mod_s = buf[mod.off .. mod.off + mod.len];
    const name_s = buf[name.off .. name.off + name.len];
    if (!std.mem.eql(u8, mod_s, "balance") or !std.mem.eql(u8, name_s, "Balance") or pc.val == 0) {
        for (0..pc.val) |_| p = skipBcsTypeTag(buf, p) orelse return null;
        return null;
    }

    // First type param is the coin type — format it
    p = formatTypeTag(buf, p, s) orelse return null;
    // Skip remaining params
    for (1..pc.val) |_| p = skipBcsTypeTag(buf, p) orelse return null;
    return .{ .end = p };
}

// Skip BCS MoveLocation: { module: ModuleId{address(32)+name(string)}, function: u16, instruction: u16, functionName: option<string> }
fn skipMoveLocation(buf: []const u8, pos: u32) ?u32 {
    var p = pos;
    p += 32; // ModuleId.address
    const ns = readUleb(buf, p) orelse return null; p = ns.end + ns.val; // ModuleId.name
    p += 2; // function u16
    p += 2; // instruction u16
    // option<string>
    if (p >= buf.len) return null;
    if (buf[p] == 1) { p += 1; const fs = readUleb(buf, p) orelse return null; p = fs.end + fs.val; } else p += 1;
    return p;
}

// Skip BCS CommandArgumentError enum (19 variants, most null)
fn skipCmdArgError(buf: []const u8, pos: u32) ?u32 {
    const v = readUleb(buf, pos) orelse return null;
    const p = v.end;
    return switch (v.val) {
        4 => p + 2, // IndexOutOfBounds: u16
        5 => p + 4, // SecondaryIndexOutOfBounds: 2x u16
        6 => p + 2, // InvalidResultArity: u16
        else => p, // all others: null payload
    };
}

// Skip ExecutionFailureStatus BCS enum (42 variants)
fn skipExecutionFailureStatus(buf: []const u8, pos: u32) ?u32 {
    const v = readUleb(buf, pos) orelse return null;
    var p = v.end;
    return switch (v.val) {
        0, 1, 2, 3, 7, 8, 9, 10, 13, 14, 15, 16, 17, 18, 22, 24, 25, 29, 30, 31, 32, 36, 39, 40 => p, // null
        4, 5, 23, 27, 37, 38 => p + 16, // 2x u64
        6, 41 => p + 32, // Address
        11 => blk: { // MovePrimitiveRuntimeError: option<MoveLocation>
            if (p >= buf.len) break :blk null;
            if (buf[p] == 1) { p += 1; break :blk skipMoveLocation(buf, p); } else break :blk p + 1;
        },
        12 => blk: { // MoveAbort: tuple(MoveLocation, u64)
            p = skipMoveLocation(buf, p) orelse break :blk null;
            break :blk p + 8;
        },
        19 => blk: { // CommandArgumentError: { argIdx: u16, kind: enum }
            p += 2;
            break :blk skipCmdArgError(buf, p);
        },
        20 => p + 4, // UnusedValueWithoutDrop: 2x u16
        21 => p + 2, // InvalidPublicFunctionReturnType: u16
        26 => blk: { // PackageUpgradeError: { upgradeError: enum(6 variants) }
            const inner = readUleb(buf, p) orelse break :blk null;
            p = inner.end;
            break :blk switch (inner.val) {
                0, 1 => p + 32, // Address
                2 => p, // null
                3 => blk2: { const l = readUleb(buf, p) orelse break :blk2 null; break :blk2 l.end + l.val; }, // vec<u8>
                4 => p + 1, // u8
                5 => p + 64, // 2x Address
                else => null,
            };
        },
        28 => p + 2, // TypeArgumentError: { u16, enum(2 variants, both null) }
        33 => blk: { // ExecutionCancelledDueToSharedObjectCongestion: vec<Address>
            const c = readUleb(buf, p) orelse break :blk null;
            break :blk c.end + c.val * 32;
        },
        34 => blk: { // AddressDeniedForCoin: Address + string
            p += 32;
            const l = readUleb(buf, p) orelse break :blk null;
            break :blk l.end + l.val;
        },
        35 => blk: { // CoinTypeGlobalPause: string
            const l = readUleb(buf, p) orelse break :blk null;
            break :blk l.end + l.val;
        },
        else => null,
    };
}

fn parseEffects(
    buf: []const u8,
    eff_off: u32,
    eff_len: u32,
    created: *[MAX_CREATED][32]u8,
    created_n: *u32,
    deleted: *[MAX_DELETED][32]u8,
    deleted_n: *u32,
    agg: []AggEntry,
    scratch: *Scratch,
) bool {
    const raw = buf[eff_off .. eff_off + eff_len];
    if (raw.len < 2 or raw[0] != 1) return true; // V1: skip

    var pos: u32 = 2;
    if (raw[1] != 0) {
        // Failure: skip ExecutionFailureStatus enum + option<u64> command
        pos = skipExecutionFailureStatus(raw, pos) orelse return true;
        // option<u64> command
        if (pos >= raw.len) return true;
        if (raw[pos] == 1) { pos += 9; } else { pos += 1; }
    }

    // Success path: skip to changedObjects
    pos += 8; // executedEpoch
    pos += 32; // gasUsed (4 x u64)
    pos = skipVecU8(raw, pos) orelse return false; // transactionDigest
    if (pos >= raw.len) return false;
    if (raw[pos] == 1) { pos += 5; } else { pos += 1; } // gasObjectIndex
    if (pos >= raw.len) return false;
    if (raw[pos] == 1) { pos += 1; pos = skipVecU8(raw, pos) orelse return false; } else { pos += 1; } // eventsDigest
    const dc = readUleb(raw, pos) orelse return false;
    pos = dc.end;
    for (0..dc.val) |_| pos = skipVecU8(raw, pos) orelse return false;
    pos += 8; // lamportVersion

    // changedObjects — need to adjust offset back to buf coordinates
    _ = parseChangedObjects(raw, pos, created, created_n, deleted, deleted_n, agg, scratch) orelse return false;
    return true;
}

// --- Coin parser ---

fn parseCoinObject(
    bcs: []const u8,
    scratch: *Scratch,
) ?CoinSnap {
    if (bcs.len < 3 or bcs[0] != 0) return null;
    const kind = bcs[1];
    if (kind != 1 and kind != 3) return null;

    var pos: u32 = 2;
    var ct_off: u32 = undefined;
    var ct_len: u16 = undefined;

    if (kind == 1) { // GasCoin
        ct_off = scratch.pos;
        _ = scratch.write(NORMALIZED_SUI);
        ct_len = @intCast(NORMALIZED_SUI.len);
    } else { // Coin<T>
        ct_off = scratch.pos;
        pos = formatTypeTag(bcs, pos, scratch) orelse return null;
        ct_len = @intCast(scratch.pos - ct_off);
    }

    pos += 1; // hasPublicTransfer
    pos += 8; // version
    const cl = readUleb(bcs, pos) orelse return null;
    pos = cl.end;
    if (cl.val < 40) return null;

    const balance = std.mem.readInt(u64, bcs[pos + 32 ..][0..8], .little);
    pos += cl.val;

    if (pos >= bcs.len) return null;
    const owner_kind = bcs[pos];
    pos += 1;

    var snap = CoinSnap{
        .obj_id = undefined,
        .version = 0,
        .owner = undefined,
        .has_owner = false,
        .ct_off = ct_off,
        .ct_len = ct_len,
        .balance = balance,
    };

    if (owner_kind == 0 and pos + 32 <= bcs.len) {
        snap.has_owner = true;
        @memcpy(&snap.owner, bcs[pos .. pos + 32]);
    }

    return snap;
}

// --- Sorting ---

fn cmpCoinSnap(a: CoinSnap, b: CoinSnap) bool {
    const id_cmp = std.mem.order(u8, &a.obj_id, &b.obj_id);
    if (id_cmp != .eq) return id_cmp == .lt;
    return a.version < b.version;
}

fn sortCoins(coins: []CoinSnap) void {
    std.sort.pdq(CoinSnap, coins, {}, struct {
        fn f(_: void, a: CoinSnap, b: CoinSnap) bool { return cmpCoinSnap(a, b); }
    }.f);
}

fn sortIds(ids: [][32]u8) void {
    std.sort.pdq([32]u8, ids, {}, struct {
        fn f(_: void, a: [32]u8, b: [32]u8) bool { return std.mem.order(u8, &a, &b) == .lt; }
    }.f);
}

fn bsearchId(ids: [][32]u8, needle: [32]u8) bool {
    var lo: usize = 0;
    var hi: usize = ids.len;
    while (lo < hi) {
        const mid = lo + (hi - lo) / 2;
        const cmp = std.mem.order(u8, &ids[mid], &needle);
        if (cmp == .lt) lo = mid + 1 else if (cmp == .gt) hi = mid else return true;
    }
    return false;
}

fn filterMatch(filter: []const u8, ct: []const u8) bool {
    var start: usize = 0;
    for (filter, 0..) |b, i| {
        if (b == 0) {
            if (std.mem.eql(u8, filter[start..i], ct)) return true;
            start = i + 1;
        }
    }
    if (start < filter.len) {
        if (std.mem.eql(u8, filter[start..], ct)) return true;
    }
    return false;
}

// --- Event BCS parser ---
// TransactionEvents BCS layout: vec<SuiEvent>
// SuiEvent: Address(32) + String(transactionModule) + Address(32 sender) + StructTag(type) + vec<u8>(contents)

const MAX_EVENTS: u32 = 8192;

fn parseEventsToOutput(buf: []const u8, events_off: u32, events_len: u32, w: *OutputWriter) u32 {
    const raw = buf[events_off .. events_off + events_len];
    var pos: u32 = 0;
    const cnt = readUleb(raw, pos) orelse return 0;
    pos = cnt.end;

    var event_count: u32 = 0;
    var ev_scratch_buf: [64 * 1024]u8 = undefined;

    for (0..cnt.val) |_| {
        if (event_count >= MAX_EVENTS) break;

        // packageId: Address (32 bytes)
        if (pos + 32 > raw.len) return event_count;
        var pkg_hex: [66]u8 = undefined;
        bytesToHex(raw[pos .. pos + 32], &pkg_hex);
        pos += 32;

        // transactionModule: String
        const mod = readBcsStr(raw, pos) orelse return event_count;
        pos = mod.end;

        // sender: Address (32 bytes)
        if (pos + 32 > raw.len) return event_count;
        var sender_hex: [66]u8 = undefined;
        bytesToHex(raw[pos .. pos + 32], &sender_hex);
        pos += 32;

        // type: StructTag → format to eventType string
        var ev_scratch = Scratch{ .buf = &ev_scratch_buf, .pos = 0 };
        // StructTag: address(32) + module(string) + name(string) + typeParams(vec<TypeTag>)
        if (pos + 32 > raw.len) return event_count;
        _ = ev_scratch.writeHex32(raw[pos .. pos + 32]);
        pos += 32;
        _ = ev_scratch.write("::");
        const type_mod = readBcsStr(raw, pos) orelse return event_count;
        _ = ev_scratch.write(raw[type_mod.off .. type_mod.off + type_mod.len]);
        pos = type_mod.end;
        _ = ev_scratch.write("::");
        const type_name = readBcsStr(raw, pos) orelse return event_count;
        _ = ev_scratch.write(raw[type_name.off .. type_name.off + type_name.len]);
        pos = type_name.end;
        const pc = readUleb(raw, pos) orelse return event_count;
        pos = pc.end;
        if (pc.val > 0) {
            _ = ev_scratch.write("<");
            for (0..pc.val) |pi| {
                if (pi > 0) _ = ev_scratch.write(", ");
                pos = formatTypeTag(raw, pos, &ev_scratch) orelse return event_count;
            }
            _ = ev_scratch.write(">");
        }
        const eventType = ev_scratch_buf[0..ev_scratch.pos];

        // contents: vec<u8>
        const contents = readBcsStr(raw, pos) orelse return event_count;
        pos = contents.end;

        // Write event to output
        // packageId
        w.writeU16(66);
        w.writeBytes(&pkg_hex);
        // module
        w.writeU16(@intCast(mod.len));
        w.writeBytes(raw[mod.off .. mod.off + mod.len]);
        // sender
        w.writeU16(66);
        w.writeBytes(&sender_hex);
        // eventType
        w.writeU16(@intCast(eventType.len));
        w.writeBytes(eventType);
        // contents (copy from decompressed buffer since it's stack-allocated)
        w.writeU32(contents.len);
        w.writeBytes(raw[contents.off .. contents.off + contents.len]);

        event_count += 1;
    }

    return event_count;
}

fn countEventsInSlice(buf: []const u8, events_off: u32, events_len: u32) u16 {
    if (events_len == 0) return 0;
    const raw = buf[events_off .. events_off + events_len];
    const cnt = readUleb(raw, 0) orelse return 0;
    return @intCast(@min(cnt.val, std.math.maxInt(u16)));
}

// --- Output writer (for the unified entry point) ---

const OutputWriter = struct {
    buf: []u8,
    pos: u32,

    fn writeU16(self: *OutputWriter, v: u16) void {
        if (self.pos + 2 > self.buf.len) return;
        std.mem.writeInt(u16, self.buf[self.pos..][0..2], v, .little);
        self.pos += 2;
    }
    fn writeU32(self: *OutputWriter, v: u32) void {
        if (self.pos + 4 > self.buf.len) return;
        std.mem.writeInt(u32, self.buf[self.pos..][0..4], v, .little);
        self.pos += 4;
    }
    fn writeU64(self: *OutputWriter, v: u64) void {
        if (self.pos + 8 > self.buf.len) return;
        std.mem.writeInt(u64, self.buf[self.pos..][0..8], v, .little);
        self.pos += 8;
    }
    fn writeU8(self: *OutputWriter, v: u8) void {
        if (self.pos + 1 > self.buf.len) return;
        self.buf[self.pos] = v;
        self.pos += 1;
    }
    fn writeI64(self: *OutputWriter, v: i64) void {
        if (self.pos + 8 > self.buf.len) return;
        std.mem.writeInt(i64, self.buf[self.pos..][0..8], v, .little);
        self.pos += 8;
    }
    fn writeBytes(self: *OutputWriter, data: []const u8) void {
        if (self.pos + data.len > self.buf.len) return;
        @memcpy(self.buf[self.pos..][0..data.len], data);
        self.pos += @intCast(data.len);
    }
};

const TxEntry = struct {
    eff_off: u32 = 0,
    eff_len: u32 = 0,
    evt_off: u32 = 0,
    evt_len: u32 = 0,
    tx_off: u32 = 0,
    tx_len: u32 = 0,
};

const TxEffectsMeta = struct {
    digest_len: u16 = 0,
    digest: [64]u8 = undefined,
    success: bool = true,
    computation_cost: u64 = 0,
    storage_cost: u64 = 0,
    storage_rebate: u64 = 0,
};

const TxDataMeta = struct {
    sender_len: u16 = 0,
    sender: [66]u8 = undefined,
    move_call_count: u16 = 0,
    move_calls_len: u32 = 0,
};

fn skipSystemPackages(buf: []const u8, pos: u32) ?u32 {
    const cnt = readUleb(buf, pos) orelse return null;
    var p = cnt.end;
    for (0..cnt.val) |_| {
        if (p + 8 > buf.len) return null;
        p += 8;
        p = skipVecVecU8(buf, p) orelse return null;
        p = skipVecAddresses(buf, p) orelse return null;
    }
    return p;
}

fn skipActiveJwk(buf: []const u8, pos: u32) ?u32 {
    var p = pos;
    const iss = readBcsStr(buf, p) orelse return null;
    p = iss.end;
    const kid = readBcsStr(buf, p) orelse return null;
    p = kid.end;
    const kty = readBcsStr(buf, p) orelse return null;
    p = kty.end;
    const e = readBcsStr(buf, p) orelse return null;
    p = e.end;
    const n = readBcsStr(buf, p) orelse return null;
    p = n.end;
    const alg = readBcsStr(buf, p) orelse return null;
    p = alg.end;
    return if (p + 8 <= buf.len) p + 8 else null;
}

fn skipAuthenticatorStateUpdate(buf: []const u8, pos: u32) ?u32 {
    var p = pos;
    if (p + 16 > buf.len) return null;
    p += 16;
    const cnt = readUleb(buf, p) orelse return null;
    p = cnt.end;
    for (0..cnt.val) |_| p = skipActiveJwk(buf, p) orelse return null;
    return if (p + 8 <= buf.len) p + 8 else null;
}

fn skipStoredExecutionTimeObservations(buf: []const u8, pos: u32) ?u32 {
    const tag = readUleb(buf, pos) orelse return null;
    if (tag.val != 0) return null;
    var p = tag.end;
    const outer = readUleb(buf, p) orelse return null;
    p = outer.end;
    for (0..outer.val) |_| {
        if (p + 2 > buf.len) return null;
        p += 2;
        const inner = readUleb(buf, p) orelse return null;
        p = inner.end;
        for (0..inner.val) |_| {
            if (p + 32 + 8 + 4 > buf.len) return null;
            p += 44;
        }
    }
    return p;
}

fn skipEndOfEpochTxKind(buf: []const u8, pos: u32) ?u32 {
    const tag = readUleb(buf, pos) orelse return null;
    var p = tag.end;
    return switch (tag.val) {
        0 => blk: {
            if (p + 56 > buf.len) break :blk null;
            p += 56;
            break :blk skipSystemPackages(buf, p);
        },
        1, 3, 4, 8, 9, 10 => p,
        2 => if (p + 16 <= buf.len) p + 16 else null,
        5 => skipVecU8(buf, p),
        6 => if (p + 8 <= buf.len) p + 8 else null,
        7 => skipStoredExecutionTimeObservations(buf, p),
        else => null,
    };
}

fn skipConsensusDeterminedVersionAssignments(buf: []const u8, pos: u32) ?u32 {
    const tag = readUleb(buf, pos) orelse return null;
    var p = tag.end;
    const outer = readUleb(buf, p) orelse return null;
    p = outer.end;
    return switch (tag.val) {
        0 => blk: {
            for (0..outer.val) |_| {
                if (p + 32 > buf.len) break :blk null;
                p += 32;
                const inner = readUleb(buf, p) orelse break :blk null;
                p = inner.end;
                for (0..inner.val) |_| {
                    if (p + 32 + 8 > buf.len) break :blk null;
                    p += 40;
                }
            }
            break :blk p;
        },
        1 => blk: {
            for (0..outer.val) |_| {
                if (p + 32 > buf.len) break :blk null;
                p += 32;
                const inner = readUleb(buf, p) orelse break :blk null;
                p = inner.end;
                for (0..inner.val) |_| {
                    if (p + 32 + 8 + 8 > buf.len) break :blk null;
                    p += 48;
                }
            }
            break :blk p;
        },
        else => null,
    };
}

fn writeMoveCall(buf: []const u8, package: []const u8, module: []const u8, function_name: []const u8, w: *OutputWriter) void {
    var package_hex: [66]u8 = undefined;
    bytesToHex(package[0..32], &package_hex);
    w.writeU16(66);
    w.writeBytes(&package_hex);
    w.writeU16(@intCast(module.len));
    w.writeBytes(module);
    w.writeU16(@intCast(function_name.len));
    w.writeBytes(function_name);
    _ = buf;
}

fn parseCommandsToOutput(buf: []const u8, pos: u32, w: *OutputWriter, move_call_count: *u16) ?u32 {
    const cnt = readUleb(buf, pos) orelse return null;
    var p = cnt.end;
    for (0..cnt.val) |_| {
        const tag = readUleb(buf, p) orelse return null;
        p = tag.end;
        switch (tag.val) {
            0 => { // MoveCall
                if (p + 32 > buf.len) return null;
                const package = buf[p .. p + 32];
                p += 32;
                const module = readBcsStr(buf, p) orelse return null;
                p = module.end;
                const function_name = readBcsStr(buf, p) orelse return null;
                p = function_name.end;
                p = skipVecTypeTags(buf, p) orelse return null;
                p = skipVecArguments(buf, p) orelse return null;
                writeMoveCall(buf, package, buf[module.off .. module.off + module.len], buf[function_name.off .. function_name.off + function_name.len], w);
                if (move_call_count.* < std.math.maxInt(u16)) move_call_count.* += 1;
            },
            1 => { // TransferObjects
                p = skipVecArguments(buf, p) orelse return null;
                p = skipArgument(buf, p) orelse return null;
            },
            2 => { // SplitCoins
                p = skipArgument(buf, p) orelse return null;
                p = skipVecArguments(buf, p) orelse return null;
            },
            3 => { // MergeCoins
                p = skipArgument(buf, p) orelse return null;
                p = skipVecArguments(buf, p) orelse return null;
            },
            4 => { // Publish
                p = skipVecVecU8(buf, p) orelse return null;
                p = skipVecAddresses(buf, p) orelse return null;
            },
            5 => { // MakeMoveVec
                p = skipOptionTypeTag(buf, p) orelse return null;
                p = skipVecArguments(buf, p) orelse return null;
            },
            6 => { // Upgrade
                p = skipVecVecU8(buf, p) orelse return null;
                p = skipVecAddresses(buf, p) orelse return null;
                if (p + 32 > buf.len) return null;
                p += 32;
                p = skipArgument(buf, p) orelse return null;
            },
            else => return null,
        }
    }
    return p;
}

fn parseTransactionKindToOutput(buf: []const u8, pos: u32, w: *OutputWriter, move_call_count: *u16) ?u32 {
    const tag = readUleb(buf, pos) orelse return null;
    var p = tag.end;
    return switch (tag.val) {
        0, 10 => blk: { // ProgrammableTransaction / ProgrammableSystemTransaction
            p = skipVecCallArgs(buf, p) orelse break :blk null;
            break :blk parseCommandsToOutput(buf, p, w, move_call_count);
        },
        1 => blk: { // ChangeEpoch
            if (p + 56 > buf.len) break :blk null;
            p += 56;
            break :blk skipSystemPackages(buf, p);
        },
        2 => null, // Genesis unsupported in fast path
        3 => if (p + 24 <= buf.len) p + 24 else null,
        4 => skipAuthenticatorStateUpdate(buf, p),
        5 => blk: { // EndOfEpochTransaction
            const cnt = readUleb(buf, p) orelse break :blk null;
            p = cnt.end;
            for (0..cnt.val) |_| p = skipEndOfEpochTxKind(buf, p) orelse break :blk null;
            break :blk p;
        },
        6 => blk: { // RandomnessStateUpdate
            if (p + 16 > buf.len) break :blk null;
            p += 16;
            p = skipVecU8(buf, p) orelse break :blk null;
            break :blk if (p + 8 <= buf.len) p + 8 else null;
        },
        7 => blk: { // ConsensusCommitPrologueV2
            if (p + 24 > buf.len) break :blk null;
            p += 24;
            break :blk skipVecU8(buf, p);
        },
        8 => blk: { // ConsensusCommitPrologueV3
            if (p + 16 > buf.len) break :blk null;
            p += 16;
            p = skipOptionU64(buf, p) orelse break :blk null;
            if (p + 8 > buf.len) break :blk null;
            p += 8;
            p = skipVecU8(buf, p) orelse break :blk null;
            break :blk skipConsensusDeterminedVersionAssignments(buf, p);
        },
        9 => blk: { // ConsensusCommitPrologueV4
            if (p + 16 > buf.len) break :blk null;
            p += 16;
            p = skipOptionU64(buf, p) orelse break :blk null;
            if (p + 8 > buf.len) break :blk null;
            p += 8;
            p = skipVecU8(buf, p) orelse break :blk null;
            p = skipConsensusDeterminedVersionAssignments(buf, p) orelse break :blk null;
            break :blk skipVecU8(buf, p);
        },
        else => null,
    };
}

fn parseTxDataMeta(buf: []const u8, tx_off: u32, tx_len: u32, move_calls_out: *OutputWriter) TxDataMeta {
    var meta = TxDataMeta{};
    if (tx_len == 0) return meta;

    const raw = buf[tx_off .. tx_off + tx_len];
    const version = readUleb(raw, 0) orelse return meta;
    if (version.val != 0) return meta;

    const p = parseTransactionKindToOutput(raw, version.end, move_calls_out, &meta.move_call_count) orelse return meta;
    if (p + 32 > raw.len) return meta;

    bytesToHex(raw[p .. p + 32], &meta.sender);
    meta.sender_len = 66;
    meta.move_calls_len = move_calls_out.pos;
    return meta;
}

fn parseTxEffectsMeta(buf: []const u8, eff_off: u32, eff_len: u32) TxEffectsMeta {
    var meta = TxEffectsMeta{};
    if (eff_len == 0) return meta;

    const raw = buf[eff_off .. eff_off + eff_len];
    if (raw.len < 2) return meta;

    if (raw[0] == 1) { // V2
        var p: u32 = 1;
        meta.success = raw[p] == 0;
        p += 1;
        if (!meta.success) {
            p = skipExecutionFailureStatus(raw, p) orelse return meta;
            p = skipOptionU64(raw, p) orelse return meta;
        }

        if (p + 8 + 32 > raw.len) return meta;
        p += 8; // executedEpoch
        meta.computation_cost = std.mem.readInt(u64, raw[p..][0..8], .little);
        meta.storage_cost = std.mem.readInt(u64, raw[p + 8 ..][0..8], .little);
        meta.storage_rebate = std.mem.readInt(u64, raw[p + 16 ..][0..8], .little);
        p += 32;

        const digest = readBcsStr(raw, p) orelse return meta;
        meta.digest_len = base58Encode(raw[digest.off .. digest.off + digest.len], &meta.digest) orelse 0;
        return meta;
    }

    return meta;
}

// --- Core processing logic ---
// Output format (20-byte header + balance changes + transactions + events):
//   u32 num_balance_changes
//   u32 num_events
//   u32 num_transactions
//   u64 timestamp_ms
//   Balance changes: u16 owner_len + owner + u16 coinType_len + coinType + i64 amount
//   Transactions:
//     u16 digest_len + digest
//     u16 sender_len + sender
//     u8 success
//     u64 computation_cost + u64 storage_cost + u64 storage_rebate
//     u16 num_events
//     u16 num_move_calls
//     Move calls: u16 package_len + package + u16 module_len + module + u16 function_len + function
//   Events:
//     Per event: u16 packageId_len + packageId + u16 module_len + module
//              + u16 sender_len + sender + u16 eventType_len + eventType
//              + u32 contents_len + contents

fn processInternal(
    buf: []const u8,
    out: []u8,
    filter: []const u8,
) u32 {
    // Working memory — large arrays use threadlocal to avoid stack overflow in worker threads.
    // Each worker thread gets its own copy; no synchronization needed.
    const TLS = struct {
        threadlocal var coins: [MAX_COINS]CoinSnap = undefined;
        threadlocal var agg: [MAX_AGG]AggEntry = undefined;
        threadlocal var scratch_buf: [SCRATCH_SIZE]u8 = undefined;
        threadlocal var created: [MAX_CREATED][32]u8 = undefined;
        threadlocal var deleted: [MAX_DELETED][32]u8 = undefined;
    };

    var created_n: u32 = 0;
    var deleted_n: u32 = 0;
    var coins_n: u32 = 0;
    for (&TLS.agg) |*e| e.occupied = false;
    var scratch = Scratch{ .buf = &TLS.scratch_buf, .pos = 0 };

    // Proto scan: extract per-transaction BCS slices, objects, summary
    var tx_entries: [MAX_TX]TxEntry = undefined;
    var num_tx: u32 = 0;
    var obj_entries: [MAX_OBJECTS]struct { bcs_off: u32, bcs_len: u32, id_off: u32, id_len: u32, version: u64 } = undefined;
    var num_obj: u32 = 0;
    var summary_bcs_off: u32 = 0;
    var summary_bcs_len: u32 = 0;

    var pos: u32 = 0;
    while (pos < buf.len) {
        const t = readVarint(buf, pos) orelse return 0;
        pos = t.end;
        const fn_: u32 = @intCast(t.val >> 3);
        const wt: u3 = @intCast(t.val & 7);
        if (wt == 0) {
            pos = skipPbField(buf, pos, 0) orelse return 0;
        } else if (wt == 2) {
            const ld = readLen(buf, pos) orelse return 0;
            pos = ld.end;
            if (fn_ == 3) { // summary
                if (bcsWrapper(buf, ld.off, ld.len)) |s| {
                    summary_bcs_off = s.off;
                    summary_bcs_len = s.len;
                }
            } else if (fn_ == 6) { // transaction
                if (num_tx >= MAX_TX) continue;
                var tx_entry = TxEntry{};
                const txend = ld.off + ld.len;
                var tp = ld.off;
                while (tp < txend) {
                    const tt = readVarint(buf, tp) orelse return 0;
                    tp = tt.end;
                    if (@as(u3, @intCast(tt.val & 7)) == 2) {
                        const tld = readLen(buf, tp) orelse return 0;
                        tp = tld.end;
                        const tfn: u32 = @intCast(tt.val >> 3);
                        if (tfn == 2) { // transaction
                            if (bcsWrapper(buf, tld.off, tld.len)) |s| {
                                tx_entry.tx_off = s.off;
                                tx_entry.tx_len = s.len;
                            }
                        } else if (tfn == 4) { // effects
                            if (bcsWrapper(buf, tld.off, tld.len)) |s| {
                                tx_entry.eff_off = s.off;
                                tx_entry.eff_len = s.len;
                            }
                        } else if (tfn == 5) { // events
                            if (bcsWrapper(buf, tld.off, tld.len)) |s| {
                                tx_entry.evt_off = s.off;
                                tx_entry.evt_len = s.len;
                            }
                        }
                    } else {
                        tp = skipPbField(buf, tp, @intCast(tt.val & 7)) orelse return 0;
                    }
                }
                tx_entries[num_tx] = tx_entry;
                num_tx += 1;
            } else if (fn_ == 7) { // objects
                const oend = ld.off + ld.len;
                var op = ld.off;
                while (op < oend) {
                    const ot = readVarint(buf, op) orelse return 0;
                    op = ot.end;
                    if (@as(u3, @intCast(ot.val & 7)) == 2) {
                        const old = readLen(buf, op) orelse return 0;
                        op = old.end;
                        if (@as(u32, @intCast(ot.val >> 3)) == 1) {
                            var o_bcs_off: u32 = 0; var o_bcs_len: u32 = 0;
                            var o_id_off: u32 = 0; var o_id_len: u32 = 0;
                            var o_ver: u64 = 0;
                            const oiend = old.off + old.len;
                            var oip = old.off;
                            while (oip < oiend) {
                                const oit = readVarint(buf, oip) orelse return 0;
                                oip = oit.end;
                                const ofn: u32 = @intCast(oit.val >> 3);
                                const owt: u3 = @intCast(oit.val & 7);
                                if (owt == 2) {
                                    const oild = readLen(buf, oip) orelse return 0;
                                    oip = oild.end;
                                    if (ofn == 1) { if (bcsValue(buf, oild.off, oild.len)) |s| { o_bcs_off = s.off; o_bcs_len = s.len; } }
                                    else if (ofn == 2) { o_id_off = oild.off; o_id_len = oild.len; }
                                } else if (owt == 0) {
                                    const vr = readVarint(buf, oip) orelse return 0;
                                    oip = vr.end;
                                    if (ofn == 3) o_ver = vr.val;
                                } else { oip = skipPbField(buf, oip, owt) orelse return 0; }
                            }
                            if (o_bcs_len > 0 and num_obj < MAX_OBJECTS) {
                                obj_entries[num_obj] = .{ .bcs_off = o_bcs_off, .bcs_len = o_bcs_len, .id_off = o_id_off, .id_len = o_id_len, .version = o_ver };
                                num_obj += 1;
                            }
                        }
                    } else { op = skipPbField(buf, op, @intCast(ot.val & 7)) orelse return 0; }
                }
            }
        } else { pos = skipPbField(buf, pos, wt) orelse return 0; }
    }

    // Extract timestamp from summary BCS (field: timestampMs at BCS offset 6*8 + 2*vec)
    // CheckpointSummary BCS: epoch(u64) + seqNum(u64) + networkTotalTx(u64) + contentDigest(vec<u8>) + prevDigest(option<vec<u8>>) + gasSummary(4*u64) + timestampMs(u64)
    var timestamp_ms: u64 = 0;
    if (summary_bcs_len > 0) {
        const sbcs = buf[summary_bcs_off .. summary_bcs_off + summary_bcs_len];
        var sp: u32 = 24; // skip epoch(8) + seqNum(8) + networkTotalTx(8)
        sp = skipVecU8(sbcs, sp) orelse 0; // contentDigest
        if (sp > 0 and sp < sbcs.len) {
            // option<vec<u8>> previousDigest
            if (sbcs[sp] == 1) { sp += 1; sp = skipVecU8(sbcs, sp) orelse 0; } else sp += 1;
        }
        if (sp > 0) sp += 32; // gasSummary (4*u64)
        if (sp > 0 and sp + 8 <= sbcs.len) {
            timestamp_ms = std.mem.readInt(u64, sbcs[sp..][0..8], .little);
        }
    }

    // Phase 2: Parse effects (balance computation)
    for (0..num_tx) |i| {
        const tx_entry = tx_entries[i];
        if (tx_entry.eff_len == 0) continue;
        if (!parseEffects(buf, tx_entry.eff_off, tx_entry.eff_len, &TLS.created, &created_n, &TLS.deleted, &deleted_n, &TLS.agg, &scratch)) return 0;
    }

    // Phase 3: Parse coin objects
    for (0..num_obj) |i| {
        const oe = obj_entries[i];
        const raw = buf[oe.bcs_off .. oe.bcs_off + oe.bcs_len];
        if (raw.len < 3 or raw[0] != 0 or (raw[1] != 1 and raw[1] != 3)) continue;
        var snap = parseCoinObject(raw, &scratch) orelse continue;
        if (filter.len > 0) {
            const ct = scratch.buf[snap.ct_off .. snap.ct_off + snap.ct_len];
            if (!filterMatch(filter, ct)) { scratch.pos = snap.ct_off; continue; }
        }
        if (oe.id_len > 0) { if (!hexDecode(buf[oe.id_off .. oe.id_off + oe.id_len], &snap.obj_id)) continue; }
        snap.version = oe.version;
        if (coins_n < MAX_COINS) { TLS.coins[coins_n] = snap; coins_n += 1; }
    }

    // Phase 4: Diff
    sortCoins(TLS.coins[0..coins_n]);
    sortIds(TLS.created[0..created_n]);
    sortIds(TLS.deleted[0..deleted_n]);
    var ci: u32 = 0;
    while (ci < coins_n) {
        var group_end = ci + 1;
        while (group_end < coins_n and std.mem.eql(u8, &TLS.coins[ci].obj_id, &TLS.coins[group_end].obj_id)) group_end += 1;
        if (group_end - ci >= 2) {
            const input = TLS.coins[ci]; const output = TLS.coins[group_end - 1];
            if (input.has_owner and output.has_owner and std.mem.eql(u8, &input.owner, &output.owner)) {
                addDelta(&TLS.agg, input.owner, input.ct_off, input.ct_len, @as(i128, output.balance) - @as(i128, input.balance), &TLS.scratch_buf);
            } else {
                if (input.has_owner) addDelta(&TLS.agg, input.owner, input.ct_off, input.ct_len, -@as(i128, input.balance), &TLS.scratch_buf);
                if (output.has_owner) addDelta(&TLS.agg, output.owner, output.ct_off, output.ct_len, @as(i128, output.balance), &TLS.scratch_buf);
            }
        } else {
            const snap = TLS.coins[ci];
            if (snap.has_owner) {
                if (bsearchId(TLS.created[0..created_n], snap.obj_id)) addDelta(&TLS.agg, snap.owner, snap.ct_off, snap.ct_len, @as(i128, snap.balance), &TLS.scratch_buf)
                else if (bsearchId(TLS.deleted[0..deleted_n], snap.obj_id)) addDelta(&TLS.agg, snap.owner, snap.ct_off, snap.ct_len, -@as(i128, snap.balance), &TLS.scratch_buf);
            }
        }
        ci = group_end;
    }

    // Phase 5: Write output
    var total_events: u32 = 0;
    var tx_event_counts: [MAX_TX]u16 = [_]u16{0} ** MAX_TX;
    for (0..num_tx) |i| {
        const event_count = countEventsInSlice(buf, tx_entries[i].evt_off, tx_entries[i].evt_len);
        tx_event_counts[i] = event_count;
        total_events += event_count;
    }

    var w = OutputWriter{ .buf = out, .pos = 20 }; // skip header (4+4+4+8 = 20)

    // Write balance changes
    var bal_count: u32 = 0;
    for (&TLS.agg) |*e| {
        if (!e.occupied or e.delta == 0) continue;
        const ct = TLS.scratch_buf[e.key.ct_off .. e.key.ct_off + e.key.ct_len];
        w.writeU16(66);
        var owner_hex: [66]u8 = undefined;
        bytesToHex(&e.key.owner, &owner_hex);
        w.writeBytes(&owner_hex);
        w.writeU16(@intCast(ct.len));
        w.writeBytes(ct);
        const amt: i64 = @intCast(@min(@max(e.delta, std.math.minInt(i64)), std.math.maxInt(i64)));
        w.writeI64(amt);
        bal_count += 1;
    }

    // Write transactions
    const TxTLS = struct {
        threadlocal var move_call_buf: [64 * 1024]u8 = undefined;
    };
    for (0..num_tx) |i| {
        var move_writer = OutputWriter{ .buf = &TxTLS.move_call_buf, .pos = 0 };
        const tx_data = parseTxDataMeta(buf, tx_entries[i].tx_off, tx_entries[i].tx_len, &move_writer);
        const tx_effects = parseTxEffectsMeta(buf, tx_entries[i].eff_off, tx_entries[i].eff_len);

        w.writeU16(tx_effects.digest_len);
        w.writeBytes(tx_effects.digest[0..tx_effects.digest_len]);
        w.writeU16(tx_data.sender_len);
        w.writeBytes(tx_data.sender[0..tx_data.sender_len]);
        w.writeU8(if (tx_effects.success) 1 else 0);
        w.writeU64(tx_effects.computation_cost);
        w.writeU64(tx_effects.storage_cost);
        w.writeU64(tx_effects.storage_rebate);
        w.writeU16(tx_event_counts[i]);
        w.writeU16(tx_data.move_call_count);
        w.writeBytes(TxTLS.move_call_buf[0..tx_data.move_calls_len]);
    }

    // Write events
    for (0..num_tx) |i| {
        _ = parseEventsToOutput(buf, tx_entries[i].evt_off, tx_entries[i].evt_len, &w);
    }

    // Write header (20 bytes: u32 + u32 + u32 + u64)
    std.mem.writeInt(u32, out[0..4], bal_count, .little);
    std.mem.writeInt(u32, out[4..8], total_events, .little);
    std.mem.writeInt(u32, out[8..12], num_tx, .little);
    std.mem.writeInt(u64, out[12..20], timestamp_ms, .little);

    return w.pos;
}

// --- Exported entry points ---

const zstd_c = @cImport(@cInclude("zstd.h"));
const DECOMPRESS_BUF_SIZE = 4 * 1024 * 1024;

/// Process a decompressed protobuf checkpoint → balance changes + events.
export fn process_checkpoint(
    input_ptr: [*]const u8,
    input_len: u32,
    output_ptr: [*]u8,
    output_capacity: u32,
    filter_ptr: [*]const u8,
    filter_len: u32,
) u32 {
    const buf = input_ptr[0..input_len];
    const out = output_ptr[0..output_capacity];
    const filter = if (filter_len > 0) filter_ptr[0..filter_len] else @as([]const u8, &.{});
    return processInternal(buf, out, filter);
}

/// Process a zstd-compressed checkpoint → balance changes + events.
/// Decompresses internally using a threadlocal buffer (safe in worker threads),
/// then calls the same core logic.
export fn process_checkpoint_compressed(
    compressed_ptr: [*]const u8,
    compressed_len: u32,
    output_ptr: [*]u8,
    output_capacity: u32,
    filter_ptr: [*]const u8,
    filter_len: u32,
) u32 {
    // threadlocal avoids 4MB stack allocation that overflows worker thread stacks
    const S = struct {
        threadlocal var decompress_buf: [DECOMPRESS_BUF_SIZE]u8 = undefined;
    };
    const result = zstd_c.ZSTD_decompress(&S.decompress_buf, DECOMPRESS_BUF_SIZE, compressed_ptr, compressed_len);
    if (zstd_c.ZSTD_isError(result) != 0) return 0;

    const buf = S.decompress_buf[0..result];
    const out = output_ptr[0..output_capacity];
    const filter = if (filter_len > 0) filter_ptr[0..filter_len] else @as([]const u8, &.{});
    return processInternal(buf, out, filter);
}

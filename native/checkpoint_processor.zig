// Unified checkpoint processor — single Zig FFI call from compressed bytes to
// fully decoded balance changes + events.
//
// Entry points:
//   process_checkpoint_compressed — compressed bytes → balance changes + events (main)
//   compute_balance_changes       — decompressed proto → balance changes only (legacy)
//   compute_balance_changes_compressed — compressed → balance changes only (legacy)
//
// Output format for process_checkpoint_compressed:
//   u32 num_changes
//   Per change:
//     u16 owner_len (always 66)
//     [66]u8 owner ("0x" + 64 hex)
//     u16 coinType_len
//     [N]u8 coinType
//     i64 amount

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

// --- Core processing logic ---
// Output format (16-byte header + balance changes + events):
//   u32 num_balance_changes
//   u32 num_events
//   u64 timestamp_ms
//   Balance changes:
//     Per change: u16 owner_len + owner + u16 coinType_len + coinType + i64 amount
//   Events:
//     Per event: u16 packageId_len + packageId + u16 module_len + module
//              + u16 sender_len + sender + u16 eventType_len + eventType
//              + u32 contents_len + contents

fn processInternal(
    buf: []const u8,
    out: []u8,
    filter: []const u8,
) u32 {
    // Working memory
    var created: [MAX_CREATED][32]u8 = undefined;
    var created_n: u32 = 0;
    var deleted: [MAX_DELETED][32]u8 = undefined;
    var deleted_n: u32 = 0;
    var coins: [MAX_COINS]CoinSnap = undefined;
    var coins_n: u32 = 0;
    var agg: [MAX_AGG]AggEntry = undefined;
    for (&agg) |*e| e.occupied = false;
    var scratch_buf: [SCRATCH_SIZE]u8 = undefined;
    var scratch = Scratch{ .buf = &scratch_buf, .pos = 0 };
    // filter param is used in Phase 3 coin type filtering

    // Proto scan: extract effects, events, objects, summary
    var eff_slices: [MAX_TX]struct { off: u32, len: u32 } = undefined;
    var num_eff: u32 = 0;
    var evt_slices: [MAX_TX]struct { off: u32, len: u32 } = undefined;
    var num_evt: u32 = 0;
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
                const txend = ld.off + ld.len;
                var tp = ld.off;
                while (tp < txend) {
                    const tt = readVarint(buf, tp) orelse return 0;
                    tp = tt.end;
                    if (@as(u3, @intCast(tt.val & 7)) == 2) {
                        const tld = readLen(buf, tp) orelse return 0;
                        tp = tld.end;
                        const tfn: u32 = @intCast(tt.val >> 3);
                        if (tfn == 4) { // effects
                            if (bcsWrapper(buf, tld.off, tld.len)) |s| {
                                if (num_eff < MAX_TX) { eff_slices[num_eff] = .{ .off = s.off, .len = s.len }; num_eff += 1; }
                            }
                        } else if (tfn == 5) { // events
                            if (bcsWrapper(buf, tld.off, tld.len)) |s| {
                                if (num_evt < MAX_TX) { evt_slices[num_evt] = .{ .off = s.off, .len = s.len }; num_evt += 1; }
                            }
                        }
                    } else {
                        tp = skipPbField(buf, tp, @intCast(tt.val & 7)) orelse return 0;
                    }
                }
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
    for (0..num_eff) |i| {
        if (!parseEffects(buf, eff_slices[i].off, eff_slices[i].len, &created, &created_n, &deleted, &deleted_n, &agg, &scratch)) return 0;
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
        if (coins_n < MAX_COINS) { coins[coins_n] = snap; coins_n += 1; }
    }

    // Phase 4: Diff
    sortCoins(coins[0..coins_n]);
    sortIds(created[0..created_n]);
    sortIds(deleted[0..deleted_n]);
    var ci: u32 = 0;
    while (ci < coins_n) {
        var group_end = ci + 1;
        while (group_end < coins_n and std.mem.eql(u8, &coins[ci].obj_id, &coins[group_end].obj_id)) group_end += 1;
        if (group_end - ci >= 2) {
            const input = coins[ci]; const output = coins[group_end - 1];
            if (input.has_owner and output.has_owner and std.mem.eql(u8, &input.owner, &output.owner)) {
                addDelta(&agg, input.owner, input.ct_off, input.ct_len, @as(i128, output.balance) - @as(i128, input.balance), &scratch_buf);
            } else {
                if (input.has_owner) addDelta(&agg, input.owner, input.ct_off, input.ct_len, -@as(i128, input.balance), &scratch_buf);
                if (output.has_owner) addDelta(&agg, output.owner, output.ct_off, output.ct_len, @as(i128, output.balance), &scratch_buf);
            }
        } else {
            const snap = coins[ci];
            if (snap.has_owner) {
                if (bsearchId(created[0..created_n], snap.obj_id)) addDelta(&agg, snap.owner, snap.ct_off, snap.ct_len, @as(i128, snap.balance), &scratch_buf)
                else if (bsearchId(deleted[0..deleted_n], snap.obj_id)) addDelta(&agg, snap.owner, snap.ct_off, snap.ct_len, -@as(i128, snap.balance), &scratch_buf);
            }
        }
        ci = group_end;
    }

    // Phase 5: Write output
    var w = OutputWriter{ .buf = out, .pos = 16 }; // skip header (4+4+8 = 16)

    // Write balance changes
    var bal_count: u32 = 0;
    for (&agg) |*e| {
        if (!e.occupied or e.delta == 0) continue;
        const ct = scratch_buf[e.key.ct_off .. e.key.ct_off + e.key.ct_len];
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

    // Write events
    var total_events: u32 = 0;
    for (0..num_evt) |i| {
        total_events += parseEventsToOutput(buf, evt_slices[i].off, evt_slices[i].len, &w);
    }

    // Write header (16 bytes: u32 + u32 + u64)
    std.mem.writeInt(u32, out[0..4], bal_count, .little);
    std.mem.writeInt(u32, out[4..8], total_events, .little);
    std.mem.writeInt(u64, out[8..16], timestamp_ms, .little);

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

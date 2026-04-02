// Fast protobuf wire format parser for Sui checkpoint messages.
//
// Takes decompressed protobuf bytes + pre-allocated output buffer.
// Returns a flat binary index of offset+length pairs pointing into
// the original input buffer. JS creates subarray views — zero copy.
//
// Output layout (all little-endian):
//   Header (56 bytes):
//     u64  sequence_number
//     u32  summary_bcs_offset, summary_bcs_length
//     u32  signature_offset, signature_length
//     u32  bitmap_offset, bitmap_length
//     u64  signature_epoch
//     u32  contents_bcs_offset, contents_bcs_length
//     u32  num_transactions
//     u32  num_objects
//   Per transaction (36 bytes + 24*num_bal_changes):
//     u32  digest_offset, digest_length
//     u32  effects_bcs_offset, effects_bcs_length
//     u32  events_bcs_offset, events_bcs_length
//     u32  tx_bcs_offset, tx_bcs_length
//     u32  num_balance_changes
//     Per balance_change (24 bytes):
//       u32 address_offset, address_length
//       u32 coin_type_offset, coin_type_length
//       u32 amount_offset, amount_length
//   Per object (24 bytes):
//     u32  bcs_offset, bcs_length
//     u32  object_id_offset, object_id_length
//     u64  version

const std = @import("std");

const Slice = struct { offset: u32, length: u32 };
const BalChange = struct { addr_off: u32, addr_len: u32, ct_off: u32, ct_len: u32, amt_off: u32, amt_len: u32 };

// Protobuf wire types
const VARINT: u3 = 0;
const FIXED64: u3 = 1;
const LEN: u3 = 2;
const FIXED32: u3 = 5;

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

fn skipField(buf: []const u8, pos: u32, wt: u3) ?u32 {
    return switch (wt) {
        VARINT => blk: {
            var p = pos;
            while (p < buf.len) {
                const b = buf[p];
                p += 1;
                if (b & 0x80 == 0) break :blk p;
            }
            break :blk null;
        },
        FIXED64 => if (pos + 8 <= buf.len) pos + 8 else null,
        LEN => blk: {
            const r = readVarint(buf, pos) orelse break :blk null;
            const e = r.end + @as(u32, @intCast(r.val));
            break :blk if (e <= buf.len) e else null;
        },
        FIXED32 => if (pos + 4 <= buf.len) pos + 4 else null,
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

// --- Output writer ---

const W = struct {
    out: []u8,
    pos: u32,

    fn u32_(self: *W, v: u32) void {
        if (self.pos + 4 > self.out.len) return;
        std.mem.writeInt(u32, self.out[self.pos..][0..4], v, .little);
        self.pos += 4;
    }
    fn u64_(self: *W, v: u64) void {
        if (self.pos + 8 > self.out.len) return;
        std.mem.writeInt(u64, self.out[self.pos..][0..8], v, .little);
        self.pos += 8;
    }
    fn slice(self: *W, s: Slice) void {
        self.u32_(s.offset);
        self.u32_(s.length);
    }
    fn patch32(self: *W, at: u32, v: u32) void {
        std.mem.writeInt(u32, self.out[at..][0..4], v, .little);
    }
};

// --- Message parsers ---

fn bcsValue(buf: []const u8, off: u32, len: u32) ?Slice {
    const end = off + len;
    var p = off;
    while (p < end) {
        const t = readVarint(buf, p) orelse return null;
        p = t.end;
        const fn_: u32 = @intCast(t.val >> 3);
        const wt: u3 = @intCast(t.val & 7);
        if (wt == LEN) {
            const ld = readLen(buf, p) orelse return null;
            if (fn_ == 2) return .{ .offset = ld.off, .length = ld.len };
            p = ld.end;
        } else {
            p = skipField(buf, p, wt) orelse return null;
        }
    }
    return null;
}

fn bcsWrapper(buf: []const u8, off: u32, len: u32) ?Slice {
    const end = off + len;
    var p = off;
    while (p < end) {
        const t = readVarint(buf, p) orelse return null;
        p = t.end;
        const fn_: u32 = @intCast(t.val >> 3);
        const wt: u3 = @intCast(t.val & 7);
        if (wt == LEN) {
            const ld = readLen(buf, p) orelse return null;
            if (fn_ == 1) return bcsValue(buf, ld.off, ld.len);
            p = ld.end;
        } else {
            p = skipField(buf, p, wt) orelse return null;
        }
    }
    return null;
}

fn parseBalChange(buf: []const u8, off: u32, len: u32) BalChange {
    const end = off + len;
    var p = off;
    var r = BalChange{ .addr_off = 0, .addr_len = 0, .ct_off = 0, .ct_len = 0, .amt_off = 0, .amt_len = 0 };
    while (p < end) {
        const t = readVarint(buf, p) orelse return r;
        p = t.end;
        const fn_: u32 = @intCast(t.val >> 3);
        const wt: u3 = @intCast(t.val & 7);
        if (wt == LEN) {
            const ld = readLen(buf, p) orelse return r;
            p = ld.end;
            switch (fn_) {
                1 => { r.addr_off = ld.off; r.addr_len = ld.len; },
                2 => { r.ct_off = ld.off; r.ct_len = ld.len; },
                3 => { r.amt_off = ld.off; r.amt_len = ld.len; },
                else => {},
            }
        } else {
            p = skipField(buf, p, wt) orelse return r;
        }
    }
    return r;
}

fn parseTx(buf: []const u8, off: u32, len: u32, w: *W) bool {
    const end = off + len;
    var p = off;
    var digest = Slice{ .offset = 0, .length = 0 };
    var effects = Slice{ .offset = 0, .length = 0 };
    var events = Slice{ .offset = 0, .length = 0 };
    var tx_bcs = Slice{ .offset = 0, .length = 0 };
    var bals: [256]BalChange = undefined;
    var bal_n: u32 = 0;

    while (p < end) {
        const t = readVarint(buf, p) orelse return false;
        p = t.end;
        const fn_: u32 = @intCast(t.val >> 3);
        const wt: u3 = @intCast(t.val & 7);
        if (wt == LEN) {
            const ld = readLen(buf, p) orelse return false;
            p = ld.end;
            switch (fn_) {
                1 => { digest = .{ .offset = ld.off, .length = ld.len }; },
                2 => { if (bcsWrapper(buf, ld.off, ld.len)) |s| { tx_bcs = s; } },
                4 => { if (bcsWrapper(buf, ld.off, ld.len)) |s| { effects = s; } },
                5 => { if (bcsWrapper(buf, ld.off, ld.len)) |s| { events = s; } },
                8 => { if (bal_n < 256) { bals[bal_n] = parseBalChange(buf, ld.off, ld.len); bal_n += 1; } },
                else => {},
            }
        } else {
            p = skipField(buf, p, wt) orelse return false;
        }
    }

    // Write: 36 bytes fixed + 24*bal_n
    w.slice(digest);
    w.slice(effects);
    w.slice(events);
    w.slice(tx_bcs);
    w.u32_(bal_n);
    for (0..bal_n) |i| {
        const b = bals[i];
        w.u32_(b.addr_off); w.u32_(b.addr_len);
        w.u32_(b.ct_off); w.u32_(b.ct_len);
        w.u32_(b.amt_off); w.u32_(b.amt_len);
    }
    return true;
}

fn parseObj(buf: []const u8, off: u32, len: u32, w: *W) bool {
    const end = off + len;
    var p = off;
    var bcs = Slice{ .offset = 0, .length = 0 };
    var id = Slice{ .offset = 0, .length = 0 };
    var ver: u64 = 0;

    while (p < end) {
        const t = readVarint(buf, p) orelse return false;
        p = t.end;
        const fn_: u32 = @intCast(t.val >> 3);
        const wt: u3 = @intCast(t.val & 7);
        if (wt == LEN) {
            const ld = readLen(buf, p) orelse return false;
            p = ld.end;
            switch (fn_) {
                1 => { if (bcsValue(buf, ld.off, ld.len)) |s| { bcs = s; } },
                2 => { id = .{ .offset = ld.off, .length = ld.len }; },
                else => {},
            }
        } else if (wt == VARINT) {
            const vr = readVarint(buf, p) orelse return false;
            p = vr.end;
            if (fn_ == 3) ver = vr.val;
        } else {
            p = skipField(buf, p, wt) orelse return false;
        }
    }

    if (bcs.length == 0) return false;
    w.slice(bcs);
    w.slice(id);
    w.u64_(ver);
    return true;
}

fn parseSig(buf: []const u8, off: u32, len: u32, sig: *Slice, bmp: *Slice, epoch: *u64) void {
    const end = off + len;
    var p = off;
    while (p < end) {
        const t = readVarint(buf, p) orelse return;
        p = t.end;
        const fn_: u32 = @intCast(t.val >> 3);
        const wt: u3 = @intCast(t.val & 7);
        if (wt == VARINT) {
            const vr = readVarint(buf, p) orelse return;
            p = vr.end;
            if (fn_ == 1) epoch.* = vr.val;
        } else if (wt == LEN) {
            const ld = readLen(buf, p) orelse return;
            p = ld.end;
            if (fn_ == 2) sig.* = .{ .offset = ld.off, .length = ld.len };
            if (fn_ == 3) bmp.* = .{ .offset = ld.off, .length = ld.len };
        } else {
            p = skipField(buf, p, wt) orelse return;
        }
    }
}

// --- Main entry point ---

export fn parse_checkpoint(
    input_ptr: [*]const u8,
    input_len: u32,
    output_ptr: [*]u8,
    output_capacity: u32,
) u32 {
    const buf = input_ptr[0..input_len];
    var w = W{ .out = output_ptr[0..output_capacity], .pos = 56 }; // skip header

    var seq: u64 = 0;
    var sum = Slice{ .offset = 0, .length = 0 };
    var sig = Slice{ .offset = 0, .length = 0 };
    var bmp = Slice{ .offset = 0, .length = 0 };
    var sig_epoch: u64 = 0;
    var cnt = Slice{ .offset = 0, .length = 0 };
    var tx_msgs: [4096]struct { off: u32, len: u32 } = undefined;
    var num_tx: u32 = 0;
    var obj_off: u32 = 0;
    var obj_len: u32 = 0;

    // Scan top-level Checkpoint fields
    var pos: u32 = 0;
    while (pos < buf.len) {
        const t = readVarint(buf, pos) orelse return 0;
        pos = t.end;
        const fn_: u32 = @intCast(t.val >> 3);
        const wt: u3 = @intCast(t.val & 7);

        if (wt == VARINT) {
            const vr = readVarint(buf, pos) orelse return 0;
            pos = vr.end;
            if (fn_ == 1) seq = vr.val;
        } else if (wt == LEN) {
            const ld = readLen(buf, pos) orelse return 0;
            pos = ld.end;
            switch (fn_) {
                3 => { if (bcsWrapper(buf, ld.off, ld.len)) |s| { sum = s; } },
                4 => { parseSig(buf, ld.off, ld.len, &sig, &bmp, &sig_epoch); },
                5 => { if (bcsWrapper(buf, ld.off, ld.len)) |s| { cnt = s; } },
                6 => { if (num_tx < 4096) { tx_msgs[num_tx] = .{ .off = ld.off, .len = ld.len }; num_tx += 1; } },
                7 => { obj_off = ld.off; obj_len = ld.len; },
                else => {},
            }
        } else {
            pos = skipField(buf, pos, wt) orelse return 0;
        }
    }

    // Write header (56 bytes)
    var h = W{ .out = w.out, .pos = 0 };
    h.u64_(seq);
    h.slice(sum);
    h.slice(sig);
    h.slice(bmp);
    h.u64_(sig_epoch);
    h.slice(cnt);
    h.u32_(num_tx);
    // num_objects placeholder at offset 52
    const num_obj_pos: u32 = 52;
    h.u32_(0);

    // Write transactions (variable size)
    for (0..num_tx) |i| {
        if (!parseTx(buf, tx_msgs[i].off, tx_msgs[i].len, &w)) return 0;
    }

    // Write objects
    var num_obj: u32 = 0;
    if (obj_len > 0) {
        const obj_end = obj_off + obj_len;
        var op = obj_off;
        while (op < obj_end) {
            const t = readVarint(buf, op) orelse return 0;
            op = t.end;
            const fn_: u32 = @intCast(t.val >> 3);
            const wt: u3 = @intCast(t.val & 7);
            if (wt == LEN) {
                const ld = readLen(buf, op) orelse return 0;
                op = ld.end;
                if (fn_ == 1) {
                    if (parseObj(buf, ld.off, ld.len, &w)) num_obj += 1;
                }
            } else {
                op = skipField(buf, op, wt) orelse return 0;
            }
        }
    }

    // Patch num_objects
    h.pos = num_obj_pos;
    h.u32_(num_obj);

    return w.pos;
}

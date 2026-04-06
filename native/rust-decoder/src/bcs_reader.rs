use crate::binary::BinaryWriter;

pub type Result<T> = std::result::Result<T, &'static str>;

const SUI_BALANCE_PREFIX: &str =
    "0x0000000000000000000000000000000000000000000000000000000000000002::balance::Balance<";

pub struct BcsReader<'a> {
    buf: &'a [u8],
    pos: usize,
}

#[allow(dead_code)]
impl<'a> BcsReader<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    pub fn position(&self) -> usize {
        self.pos
    }

    pub fn remaining(&self) -> usize {
        self.buf.len().saturating_sub(self.pos)
    }

    pub fn read_u8(&mut self) -> Result<u8> {
        if self.pos >= self.buf.len() {
            return Err("unexpected eof");
        }
        let value = self.buf[self.pos];
        self.pos += 1;
        Ok(value)
    }

    pub fn read_u16(&mut self) -> Result<u16> {
        let bytes = self.read_bytes(2)?;
        Ok(u16::from_le_bytes([bytes[0], bytes[1]]))
    }

    pub fn read_u32(&mut self) -> Result<u32> {
        let bytes = self.read_bytes(4)?;
        Ok(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    pub fn read_u64(&mut self) -> Result<u64> {
        let bytes = self.read_bytes(8)?;
        Ok(u64::from_le_bytes(
            bytes.try_into().map_err(|_| "invalid u64")?,
        ))
    }

    pub fn read_u128(&mut self) -> Result<u128> {
        let bytes = self.read_bytes(16)?;
        Ok(u128::from_le_bytes(
            bytes.try_into().map_err(|_| "invalid u128")?,
        ))
    }

    pub fn read_bool(&mut self) -> Result<bool> {
        match self.read_u8()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err("invalid bool"),
        }
    }

    pub fn read_uleb128(&mut self) -> Result<u64> {
        let mut value = 0u64;
        let mut shift = 0u32;
        loop {
            let byte = self.read_u8()?;
            value |= ((byte & 0x7f) as u64) << shift;
            if byte & 0x80 == 0 {
                return Ok(value);
            }
            shift += 7;
            if shift >= 64 {
                return Err("uleb128 overflow");
            }
        }
    }

    pub fn read_bytes(&mut self, n: usize) -> Result<&'a [u8]> {
        let end = self.pos.checked_add(n).ok_or("overflow")?;
        if end > self.buf.len() {
            return Err("unexpected eof");
        }
        let bytes = &self.buf[self.pos..end];
        self.pos = end;
        Ok(bytes)
    }

    pub fn read_fixed<const N: usize>(&mut self) -> Result<[u8; N]> {
        self.read_bytes(N)?
            .try_into()
            .map_err(|_| "invalid fixed width field")
    }

    pub fn read_vec_len(&mut self) -> Result<usize> {
        let len = self.read_uleb128()?;
        let len = usize::try_from(len).map_err(|_| "vec too large")?;
        if len > self.remaining() {
            return Err("vec length exceeds remaining bytes");
        }
        Ok(len)
    }

    pub fn read_option_tag(&mut self) -> Result<bool> {
        match self.read_u8()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err("invalid option tag"),
        }
    }

    pub fn read_string(&mut self) -> Result<&'a str> {
        let len = self.read_vec_len()?;
        let bytes = self.read_bytes(len)?;
        std::str::from_utf8(bytes).map_err(|_| "invalid utf8")
    }

    pub fn skip_bytes(&mut self, n: usize) -> Result<()> {
        self.read_bytes(n).map(|_| ())
    }

    pub fn read_digest32(&mut self) -> Result<[u8; 32]> {
        let len = self.read_vec_len()?;
        if len != 32 {
            return Err("invalid digest length");
        }
        self.read_fixed::<32>()
    }

    pub fn skip_digest32(&mut self) -> Result<()> {
        let len = self.read_vec_len()?;
        if len != 32 {
            return Err("invalid digest length");
        }
        self.skip_bytes(32)
    }

    pub fn skip_vec_of_fixed(&mut self, elem_size: usize) -> Result<()> {
        let len = self.read_vec_len()?;
        self.skip_bytes(len.checked_mul(elem_size).ok_or("overflow")?)
    }

    pub fn skip_string(&mut self) -> Result<()> {
        let len = self.read_vec_len()?;
        self.skip_bytes(len)
    }

    pub fn skip_option_fixed(&mut self, size: usize) -> Result<()> {
        if self.read_option_tag()? {
            self.skip_bytes(size)?;
        }
        Ok(())
    }

    pub fn capture_slice<F>(&mut self, f: F) -> Result<&'a [u8]>
    where
        F: FnOnce(&mut Self) -> Result<()>,
    {
        let start = self.pos;
        f(self)?;
        Ok(&self.buf[start..self.pos])
    }

    pub fn finish(self) -> Result<()> {
        if self.remaining() == 0 {
            Ok(())
        } else {
            Err("trailing bytes")
        }
    }
}

#[derive(Clone, Copy)]
pub struct GasCostSummary {
    pub computation_cost: u64,
    pub storage_cost: u64,
    pub storage_rebate: u64,
    pub non_refundable_storage_fee: u64,
}

pub enum StatusSummary<'a> {
    Success,
    Failure(FailureSummary<'a>),
}

impl StatusSummary<'_> {
    pub fn is_success(&self) -> bool {
        matches!(self, Self::Success)
    }
}

pub struct FailureSummary<'a> {
    pub kind_name: &'static str,
    pub command: Option<u64>,
    pub move_abort: Option<MoveAbortDetails<'a>>,
}

pub struct MoveAbortDetails<'a> {
    pub module_address: [u8; 32],
    pub module_name: &'a str,
    pub function_name: Option<&'a str>,
    pub abort_code: u64,
}

pub struct EffectsExtract<'a> {
    pub digest: [u8; 32],
    pub status: StatusSummary<'a>,
    pub epoch: u64,
    pub gas_used: GasCostSummary,
    pub lamport_version: u64,
    pub dependency_count: usize,
    pub gas_object_index: Option<u32>,
    pub events_digest: Option<[u8; 32]>,
    pub dependencies: Vec<[u8; 32]>,
    pub changed_objects: Vec<&'a [u8]>,
    pub unchanged_consensus_objects: Vec<&'a [u8]>,
}

#[derive(Clone, Copy)]
pub enum ProgrammableKind {
    User,
    System,
}

pub struct TxDataExtract<'a> {
    pub sender: [u8; 32],
    pub kind: ProgrammableKind,
    pub move_call_count: usize,
    pub inputs: Vec<&'a [u8]>,
    pub commands: Vec<&'a [u8]>,
}

#[derive(Clone, Copy)]
pub struct MoveCallInfo<'a> {
    pub package: [u8; 32],
    pub module: &'a str,
    pub function: &'a str,
}

#[derive(Clone, Copy)]
pub enum IdOperation {
    None,
    Created,
    Deleted,
}

#[derive(Clone, Copy)]
pub enum OwnerSummary {
    Address([u8; 32]),
    Object([u8; 32]),
    Shared,
    Immutable,
    ConsensusAddress([u8; 32]),
}

#[derive(Clone, Copy)]
pub enum ObjectInSummary {
    NotExist,
    Exist {
        version: u64,
        digest: [u8; 32],
        owner: OwnerSummary,
    },
}

pub enum AccumulatorOperation {
    Merge,
    Split,
}

pub enum AccumulatorValueSummary {
    Integer(u64),
    IntegerTuple,
    EventDigest,
}

pub struct AccumulatorWrite<'a> {
    pub address: [u8; 32],
    pub type_tag_bcs: &'a [u8],
    pub operation: AccumulatorOperation,
    pub value: AccumulatorValueSummary,
}

pub enum ObjectOutSummary<'a> {
    NotExist,
    ObjectWrite {
        digest: [u8; 32],
        owner: OwnerSummary,
    },
    PackageWrite {
        version: u64,
        digest: [u8; 32],
    },
    AccumulatorWrite(AccumulatorWrite<'a>),
}

pub struct ChangedObject<'a> {
    pub id: [u8; 32],
    pub input: ObjectInSummary,
    pub output: ObjectOutSummary<'a>,
    pub id_operation: IdOperation,
}

pub enum UnchangedConsensusKindSummary {
    ReadOnlyRoot { version: u64, digest: [u8; 32] },
    MutateConsensusStreamEnded(u64),
    ReadConsensusStreamEnded(u64),
    Cancelled(u64),
    PerEpochConfig,
}

pub struct UnchangedConsensusObject {
    pub id: [u8; 32],
    pub kind: UnchangedConsensusKindSummary,
}

#[derive(Clone, Copy)]
enum ArgumentSummary {
    GasCoin,
    Input(u16),
    Result(u16),
    NestedResult(u16, u16),
}

#[allow(dead_code)]
pub fn extract_effects<'a>(bcs: &'a [u8], _w: &mut BinaryWriter) -> Result<EffectsExtract<'a>> {
    parse_effects(bcs)
}

pub fn parse_effects<'a>(bcs: &'a [u8]) -> Result<EffectsExtract<'a>> {
    let mut reader = BcsReader::new(bcs);
    match reader.read_uleb128().map_err(|_| "effects.version")? {
        1 => {}
        0 => return Err("effects v1"),
        _ => return Err("unsupported effects variant"),
    }

    let status = parse_execution_status(&mut reader).map_err(|_| "effects.status")?;
    let epoch = reader.read_u64().map_err(|_| "effects.epoch")?;
    let gas_used = GasCostSummary {
        computation_cost: reader.read_u64().map_err(|_| "effects.gas.computation")?,
        storage_cost: reader.read_u64().map_err(|_| "effects.gas.storage")?,
        storage_rebate: reader.read_u64().map_err(|_| "effects.gas.rebate")?,
        non_refundable_storage_fee: reader
            .read_u64()
            .map_err(|_| "effects.gas.non_refundable")?,
    };
    let digest = reader.read_digest32().map_err(|_| "effects.digest")?;
    let gas_object_index = if reader
        .read_option_tag()
        .map_err(|_| "effects.gas_object_index.tag")?
    {
        Some(
            reader
                .read_u32()
                .map_err(|_| "effects.gas_object_index.value")?,
        )
    } else {
        None
    };
    let events_digest = if reader.read_option_tag().map_err(|_| "effects.events.tag")? {
        Some(reader.read_digest32().map_err(|_| "effects.events.value")?)
    } else {
        None
    };

    let dependency_count = reader
        .read_vec_len()
        .map_err(|_| "effects.dependencies.len")?;
    let mut dependencies = Vec::with_capacity(dependency_count);
    for _ in 0..dependency_count {
        dependencies.push(
            reader
                .read_digest32()
                .map_err(|_| "effects.dependencies.entry")?,
        );
    }

    let lamport_version = reader.read_u64().map_err(|_| "effects.lamport_version")?;

    let changed_len = reader
        .read_vec_len()
        .map_err(|_| "effects.changed_objects.len")?;
    let mut changed_objects = Vec::with_capacity(changed_len);
    for _ in 0..changed_len {
        changed_objects.push(
            reader
                .capture_slice(skip_changed_object_entry)
                .map_err(|_| "effects.changed_objects.entry")?,
        );
    }

    let unchanged_len = reader
        .read_vec_len()
        .map_err(|_| "effects.unchanged_consensus.len")?;
    let mut unchanged_consensus_objects = Vec::with_capacity(unchanged_len);
    for _ in 0..unchanged_len {
        unchanged_consensus_objects.push(
            reader
                .capture_slice(skip_unchanged_consensus_object_entry)
                .map_err(|_| "effects.unchanged_consensus.entry")?,
        );
    }

    if reader
        .read_option_tag()
        .map_err(|_| "effects.aux_data.tag")?
    {
        reader
            .skip_digest32()
            .map_err(|_| "effects.aux_data.value")?;
    }
    reader.finish().map_err(|_| "effects.trailing_bytes")?;

    Ok(EffectsExtract {
        digest,
        status,
        epoch,
        gas_used,
        lamport_version,
        dependency_count,
        gas_object_index,
        events_digest,
        dependencies,
        changed_objects,
        unchanged_consensus_objects,
    })
}

#[allow(dead_code)]
pub fn extract_tx_data<'a>(bcs: &'a [u8], _w: &mut BinaryWriter) -> Result<TxDataExtract<'a>> {
    parse_tx_data(bcs)
}

pub fn parse_tx_data<'a>(bcs: &'a [u8]) -> Result<TxDataExtract<'a>> {
    let mut reader = BcsReader::new(bcs);
    match reader.read_uleb128().map_err(|_| "tx.version")? {
        0 => {}
        _ => return Err("unsupported transaction data variant"),
    }

    let kind = match reader.read_uleb128().map_err(|_| "tx.kind")? {
        0 => ProgrammableKind::User,
        10 => ProgrammableKind::System,
        _ => return Err("unsupported transaction kind"),
    };

    let inputs_len = reader.read_vec_len().map_err(|_| "tx.inputs.len")?;
    let mut inputs = Vec::with_capacity(inputs_len);
    for _ in 0..inputs_len {
        inputs.push(
            reader
                .capture_slice(skip_call_arg)
                .map_err(|_| "tx.inputs.entry")?,
        );
    }

    let commands_len = reader.read_vec_len().map_err(|_| "tx.commands.len")?;
    let mut commands = Vec::with_capacity(commands_len);
    let mut move_call_count = 0usize;
    for _ in 0..commands_len {
        let command = reader
            .capture_slice(skip_command)
            .map_err(|_| "tx.commands.entry")?;
        if BcsReader::new(command)
            .read_uleb128()
            .map_err(|_| "tx.commands.variant")?
            == 0
        {
            move_call_count += 1;
        }
        commands.push(command);
    }

    let sender = reader.read_fixed::<32>().map_err(|_| "tx.sender")?;
    skip_gas_data(&mut reader).map_err(|_| "tx.gas_data")?;
    skip_transaction_expiration(&mut reader).map_err(|_| "tx.expiration")?;
    reader.finish().map_err(|_| "tx.trailing_bytes")?;

    Ok(TxDataExtract {
        sender,
        kind,
        move_call_count,
        inputs,
        commands,
    })
}

pub fn parse_changed_object<'a>(bcs: &'a [u8]) -> Result<ChangedObject<'a>> {
    let mut reader = BcsReader::new(bcs);
    let id = reader.read_fixed::<32>()?;
    let input = parse_object_in(&mut reader)?;
    let output = parse_object_out(&mut reader)?;
    let id_operation = parse_id_operation(&mut reader)?;
    reader.finish()?;
    Ok(ChangedObject {
        id,
        input,
        output,
        id_operation,
    })
}

pub fn parse_unchanged_consensus_object(bcs: &[u8]) -> Result<UnchangedConsensusObject> {
    let mut reader = BcsReader::new(bcs);
    let id = reader.read_fixed::<32>()?;
    let kind = match reader.read_uleb128()? {
        0 => UnchangedConsensusKindSummary::ReadOnlyRoot {
            version: reader.read_u64()?,
            digest: reader.read_digest32()?,
        },
        1 => UnchangedConsensusKindSummary::MutateConsensusStreamEnded(reader.read_u64()?),
        2 => UnchangedConsensusKindSummary::ReadConsensusStreamEnded(reader.read_u64()?),
        3 => UnchangedConsensusKindSummary::Cancelled(reader.read_u64()?),
        4 => UnchangedConsensusKindSummary::PerEpochConfig,
        _ => return Err("unsupported unchanged consensus kind"),
    };
    reader.finish()?;
    Ok(UnchangedConsensusObject { id, kind })
}

pub fn write_input_fields_from_bcs(bcs: &[u8], w: &mut BinaryWriter) -> Result<()> {
    let mut reader = BcsReader::new(bcs);
    match reader.read_uleb128()? {
        0 => {
            let bytes_len = reader.read_vec_len()?;
            let bytes = reader.read_bytes(bytes_len)?;
            w.write_str("PURE");
            w.write_null();
            w.write_null();
            w.write_null();
            w.write_null();
            w.write_null();
            w.write_hex(bytes);
            w.write_null();
            w.write_null();
            w.write_null();
        }
        1 => {
            match reader.read_uleb128()? {
                0 => {
                    let id = reader.read_fixed::<32>()?;
                    let version = reader.read_u64()?;
                    let digest = reader.read_digest32()?;
                    w.write_str("IMMUTABLE_OR_OWNED");
                    w.write_0x_hex(&id);
                    w.write_u64_dec(version);
                    w.write_base58(&digest);
                    w.write_null();
                    w.write_null();
                }
                1 => {
                    let id = reader.read_fixed::<32>()?;
                    let initial_shared_version = reader.read_u64()?;
                    let mutability = reader.read_uleb128()?;
                    w.write_str("SHARED");
                    w.write_0x_hex(&id);
                    w.write_null();
                    w.write_null();
                    w.write_str(shared_mutability_name(mutability)?);
                    w.write_u64_dec(initial_shared_version);
                }
                2 => {
                    let id = reader.read_fixed::<32>()?;
                    let version = reader.read_u64()?;
                    let digest = reader.read_digest32()?;
                    w.write_str("RECEIVING");
                    w.write_0x_hex(&id);
                    w.write_u64_dec(version);
                    w.write_base58(&digest);
                    w.write_null();
                    w.write_null();
                }
                _ => return Err("unsupported object arg"),
            }
            w.write_null();
            w.write_null();
            w.write_null();
            w.write_null();
        }
        2 => {
            w.write_str("FUNDS_WITHDRAWAL");
            w.write_null();
            w.write_null();
            w.write_null();
            w.write_null();
            w.write_null();
            w.write_null();

            match reader.read_uleb128()? {
                0 => w.write_u64_dec(reader.read_u64()?),
                _ => return Err("unsupported reservation"),
            }

            match reader.read_uleb128()? {
                0 => {
                    let tag = reader.capture_slice(skip_type_like)?;
                    let len_pos = w.begin_string();
                    w.write_raw_str(SUI_BALANCE_PREFIX);
                    write_type_like_canonical_from_bytes(tag, w, true)?;
                    w.write_raw_byte(b'>');
                    w.finish_string(len_pos);
                }
                _ => return Err("unsupported withdrawal type"),
            }

            match reader.read_uleb128()? {
                0 => w.write_str("SENDER"),
                1 => w.write_str("SPONSOR"),
                _ => return Err("unsupported withdrawal source"),
            }
        }
        _ => return Err("unsupported call arg"),
    }
    reader.finish()
}

pub fn write_command_fields_from_bcs<'a>(
    bcs: &'a [u8],
    w: &mut BinaryWriter,
) -> Result<Option<MoveCallInfo<'a>>> {
    let mut reader = BcsReader::new(bcs);
    let command = match reader.read_uleb128()? {
        0 => {
            let package = reader.read_fixed::<32>()?;
            let module = reader.read_string()?;
            let function = reader.read_string()?;
            let type_args_len = reader.read_vec_len()?;
            w.write_str("MoveCall");
            w.write_0x_hex(&package);
            w.write_str(module);
            w.write_str(function);

            if type_args_len == 0 {
                w.write_str("[]");
            } else {
                let len_pos = w.begin_string();
                w.write_raw_byte(b'[');
                for i in 0..type_args_len {
                    if i > 0 {
                        w.write_raw_byte(b',');
                    }
                    let tag = reader.capture_slice(skip_type_like)?;
                    w.write_raw_byte(b'"');
                    write_type_like_canonical_from_bytes(tag, w, true)?;
                    w.write_raw_byte(b'"');
                }
                w.write_raw_byte(b']');
                w.finish_string(len_pos);
            }

            let args_len = reader.read_vec_len()?;
            for _ in 0..args_len {
                skip_argument(&mut reader)?;
            }
            w.write_null();
            Some(MoveCallInfo {
                package,
                module,
                function,
            })
        }
        1 => {
            w.write_str("TransferObjects");
            w.write_null();
            w.write_null();
            w.write_null();
            w.write_null();

            let len_pos = w.begin_string();
            w.write_raw_str("{\"objects\":[");
            let objects_len = reader.read_vec_len()?;
            for i in 0..objects_len {
                if i > 0 {
                    w.write_raw_byte(b',');
                }
                write_argument_json(&parse_argument(&mut reader)?, w);
            }
            w.write_raw_str("],\"address\":");
            write_argument_json(&parse_argument(&mut reader)?, w);
            w.write_raw_byte(b'}');
            w.finish_string(len_pos);
            None
        }
        2 => {
            w.write_str("SplitCoins");
            w.write_null();
            w.write_null();
            w.write_null();
            w.write_null();

            let len_pos = w.begin_string();
            w.write_raw_str("{\"coin\":");
            write_argument_json(&parse_argument(&mut reader)?, w);
            w.write_raw_str(",\"amounts\":[");
            let amounts_len = reader.read_vec_len()?;
            for i in 0..amounts_len {
                if i > 0 {
                    w.write_raw_byte(b',');
                }
                write_argument_json(&parse_argument(&mut reader)?, w);
            }
            w.write_raw_str("]}");
            w.finish_string(len_pos);
            None
        }
        3 => {
            w.write_str("MergeCoins");
            w.write_null();
            w.write_null();
            w.write_null();
            w.write_null();

            let len_pos = w.begin_string();
            w.write_raw_str("{\"coin\":");
            write_argument_json(&parse_argument(&mut reader)?, w);
            w.write_raw_str(",\"coins\":[");
            let coins_len = reader.read_vec_len()?;
            for i in 0..coins_len {
                if i > 0 {
                    w.write_raw_byte(b',');
                }
                write_argument_json(&parse_argument(&mut reader)?, w);
            }
            w.write_raw_str("]}");
            w.finish_string(len_pos);
            None
        }
        4 => {
            w.write_str("Publish");
            w.write_null();
            w.write_null();
            w.write_null();
            w.write_null();

            let len_pos = w.begin_string();
            w.write_raw_str("{\"modules\":[");
            let modules_len = reader.read_vec_len()?;
            for i in 0..modules_len {
                if i > 0 {
                    w.write_raw_byte(b',');
                }
                let bytes = read_vec_bytes(&mut reader)?;
                write_quoted_0x_hex_contents(bytes, w);
            }
            w.write_raw_str("],\"dependencies\":[");
            let deps_len = reader.read_vec_len()?;
            for i in 0..deps_len {
                if i > 0 {
                    w.write_raw_byte(b',');
                }
                write_quoted_0x_hex_contents(&reader.read_fixed::<32>()?, w);
            }
            w.write_raw_str("]}");
            w.finish_string(len_pos);
            None
        }
        5 => {
            w.write_str("MakeMoveVector");
            w.write_null();
            w.write_null();
            w.write_null();
            w.write_null();

            let len_pos = w.begin_string();
            w.write_raw_str("{\"elementType\":");
            if reader.read_option_tag()? {
                let tag = reader.capture_slice(skip_type_like)?;
                w.write_raw_byte(b'"');
                write_type_like_canonical_from_bytes(tag, w, true)?;
                w.write_raw_byte(b'"');
            } else {
                w.write_raw_str("null");
            }
            w.write_raw_str(",\"elements\":[");
            let elems_len = reader.read_vec_len()?;
            for i in 0..elems_len {
                if i > 0 {
                    w.write_raw_byte(b',');
                }
                write_argument_json(&parse_argument(&mut reader)?, w);
            }
            w.write_raw_str("]}");
            w.finish_string(len_pos);
            None
        }
        6 => {
            w.write_str("Upgrade");
            w.write_null();
            w.write_null();
            w.write_null();
            w.write_null();

            let len_pos = w.begin_string();
            w.write_raw_str("{\"modules\":[");
            let modules_len = reader.read_vec_len()?;
            for i in 0..modules_len {
                if i > 0 {
                    w.write_raw_byte(b',');
                }
                let bytes = read_vec_bytes(&mut reader)?;
                write_quoted_0x_hex_contents(bytes, w);
            }
            w.write_raw_str("],\"dependencies\":[");
            let deps_len = reader.read_vec_len()?;
            for i in 0..deps_len {
                if i > 0 {
                    w.write_raw_byte(b',');
                }
                write_quoted_0x_hex_contents(&reader.read_fixed::<32>()?, w);
            }
            let package = reader.read_fixed::<32>()?;
            w.write_raw_str("],\"package\":");
            write_quoted_0x_hex_contents(&package, w);
            w.write_raw_str(",\"ticket\":");
            write_argument_json(&parse_argument(&mut reader)?, w);
            w.write_raw_byte(b'}');
            w.finish_string(len_pos);
            None
        }
        _ => return Err("unsupported command"),
    };

    reader.finish()?;
    Ok(command)
}

fn parse_execution_status<'a>(reader: &mut BcsReader<'a>) -> Result<StatusSummary<'a>> {
    match reader.read_uleb128()? {
        0 => Ok(StatusSummary::Success),
        1 => {
            let (kind_name, move_abort) = parse_execution_error(reader)?;
            let command = if reader.read_option_tag()? {
                Some(reader.read_u64()?)
            } else {
                None
            };
            Ok(StatusSummary::Failure(FailureSummary {
                kind_name,
                command,
                move_abort,
            }))
        }
        _ => Err("unsupported execution status"),
    }
}

fn parse_execution_error<'a>(
    reader: &mut BcsReader<'a>,
) -> Result<(&'static str, Option<MoveAbortDetails<'a>>)> {
    let tag = reader.read_uleb128()?;
    let name = execution_error_kind_name(tag)?;
    let move_abort = if tag == 12 {
        let module_address = reader.read_fixed::<32>()?;
        let module_name = reader.read_string()?;
        let _function = reader.read_u16()?;
        let _instruction = reader.read_u16()?;
        let function_name = if reader.read_option_tag()? {
            Some(reader.read_string()?)
        } else {
            None
        };
        let abort_code = reader.read_u64()?;
        Some(MoveAbortDetails {
            module_address,
            module_name,
            function_name,
            abort_code,
        })
    } else {
        skip_execution_error_fields(reader, tag)?;
        None
    };
    Ok((name, move_abort))
}

fn parse_object_in<'a>(reader: &mut BcsReader<'a>) -> Result<ObjectInSummary> {
    match reader.read_uleb128()? {
        0 => Ok(ObjectInSummary::NotExist),
        1 => Ok(ObjectInSummary::Exist {
            version: reader.read_u64()?,
            digest: reader.read_digest32()?,
            owner: parse_owner(reader)?,
        }),
        _ => Err("unsupported object input"),
    }
}

fn parse_object_out<'a>(reader: &mut BcsReader<'a>) -> Result<ObjectOutSummary<'a>> {
    match reader.read_uleb128()? {
        0 => Ok(ObjectOutSummary::NotExist),
        1 => Ok(ObjectOutSummary::ObjectWrite {
            digest: reader.read_digest32()?,
            owner: parse_owner(reader)?,
        }),
        2 => Ok(ObjectOutSummary::PackageWrite {
            version: reader.read_u64()?,
            digest: reader.read_digest32()?,
        }),
        3 => Ok(ObjectOutSummary::AccumulatorWrite(parse_accumulator_write(
            reader,
        )?)),
        _ => Err("unsupported object output"),
    }
}

fn parse_owner(reader: &mut BcsReader<'_>) -> Result<OwnerSummary> {
    match reader.read_uleb128()? {
        0 => Ok(OwnerSummary::Address(reader.read_fixed::<32>()?)),
        1 => Ok(OwnerSummary::Object(reader.read_fixed::<32>()?)),
        2 => {
            let _ = reader.read_u64()?;
            Ok(OwnerSummary::Shared)
        }
        3 => Ok(OwnerSummary::Immutable),
        4 => {
            let _ = reader.read_u64()?;
            Ok(OwnerSummary::ConsensusAddress(reader.read_fixed::<32>()?))
        }
        _ => Err("unsupported owner"),
    }
}

fn parse_accumulator_write<'a>(reader: &mut BcsReader<'a>) -> Result<AccumulatorWrite<'a>> {
    let address = reader.read_fixed::<32>()?;
    let type_tag_bcs = reader.capture_slice(skip_type_like)?;
    let operation = match reader.read_uleb128()? {
        0 => AccumulatorOperation::Merge,
        1 => AccumulatorOperation::Split,
        _ => return Err("unsupported accumulator operation"),
    };
    let value = match reader.read_uleb128()? {
        0 => AccumulatorValueSummary::Integer(reader.read_u64()?),
        1 => {
            reader.read_u64()?;
            reader.read_u64()?;
            AccumulatorValueSummary::IntegerTuple
        }
        2 => {
            let len = reader.read_vec_len()?;
            for _ in 0..len {
                reader.read_u64()?;
                reader.skip_digest32()?;
            }
            AccumulatorValueSummary::EventDigest
        }
        _ => return Err("unsupported accumulator value"),
    };
    Ok(AccumulatorWrite {
        address,
        type_tag_bcs,
        operation,
        value,
    })
}

fn parse_id_operation(reader: &mut BcsReader<'_>) -> Result<IdOperation> {
    match reader.read_uleb128()? {
        0 => Ok(IdOperation::None),
        1 => Ok(IdOperation::Created),
        2 => Ok(IdOperation::Deleted),
        _ => Err("unsupported id operation"),
    }
}

fn parse_argument(reader: &mut BcsReader<'_>) -> Result<ArgumentSummary> {
    match reader.read_uleb128()? {
        0 => Ok(ArgumentSummary::GasCoin),
        1 => Ok(ArgumentSummary::Input(reader.read_u16()?)),
        2 => Ok(ArgumentSummary::Result(reader.read_u16()?)),
        3 => Ok(ArgumentSummary::NestedResult(
            reader.read_u16()?,
            reader.read_u16()?,
        )),
        _ => Err("unsupported argument"),
    }
}

fn read_vec_bytes<'a>(reader: &mut BcsReader<'a>) -> Result<&'a [u8]> {
    let len = reader.read_vec_len()?;
    reader.read_bytes(len)
}

fn write_argument_json(argument: &ArgumentSummary, w: &mut BinaryWriter) {
    match argument {
        ArgumentSummary::GasCoin => w.write_raw_str("\"GasCoin\""),
        ArgumentSummary::Input(index) => {
            w.write_raw_str("{\"Input\":");
            write_raw_u64(*index as u64, w);
            w.write_raw_byte(b'}');
        }
        ArgumentSummary::Result(index) => {
            w.write_raw_str("{\"Result\":");
            write_raw_u64(*index as u64, w);
            w.write_raw_byte(b'}');
        }
        ArgumentSummary::NestedResult(first, second) => {
            w.write_raw_str("{\"NestedResult\":[");
            write_raw_u64(*first as u64, w);
            w.write_raw_byte(b',');
            write_raw_u64(*second as u64, w);
            w.write_raw_str("]}");
        }
    }
}

fn write_raw_u64(value: u64, w: &mut BinaryWriter) {
    let mut tmp = [0u8; 20];
    let mut n = value;
    let mut i = tmp.len();
    if n == 0 {
        w.write_raw_byte(b'0');
        return;
    }
    while n > 0 {
        i -= 1;
        tmp[i] = b'0' + (n % 10) as u8;
        n /= 10;
    }
    w.write_raw_bytes(&tmp[i..]);
}

fn write_quoted_0x_hex_contents(bytes: &[u8], w: &mut BinaryWriter) {
    w.write_raw_byte(b'"');
    w.write_raw_str("0x");
    w.write_raw_hex_contents(bytes);
    w.write_raw_byte(b'"');
}

fn write_canonical_address_contents(bytes: &[u8; 32], w: &mut BinaryWriter, with_prefix: bool) {
    if with_prefix {
        w.write_raw_str("0x");
    }
    w.write_raw_hex_contents(bytes);
}

fn write_type_like_canonical_from_bytes(
    bcs: &[u8],
    w: &mut BinaryWriter,
    with_prefix: bool,
) -> Result<()> {
    let mut reader = BcsReader::new(bcs);
    write_type_like_canonical(&mut reader, w, with_prefix)?;
    reader.finish()
}

fn write_type_like_canonical(
    reader: &mut BcsReader<'_>,
    w: &mut BinaryWriter,
    with_prefix: bool,
) -> Result<()> {
    match reader.read_uleb128()? {
        0 => w.write_raw_str("bool"),
        1 => w.write_raw_str("u8"),
        2 => w.write_raw_str("u64"),
        3 => w.write_raw_str("u128"),
        4 => w.write_raw_str("address"),
        5 => w.write_raw_str("signer"),
        6 => {
            w.write_raw_str("vector<");
            write_type_like_canonical(reader, w, with_prefix)?;
            w.write_raw_byte(b'>');
        }
        7 => {
            let address = reader.read_fixed::<32>()?;
            let module = reader.read_string()?;
            let name = reader.read_string()?;
            let len = reader.read_vec_len()?;
            write_canonical_address_contents(&address, w, with_prefix);
            w.write_raw_str("::");
            w.write_raw_str(module);
            w.write_raw_str("::");
            w.write_raw_str(name);
            if len > 0 {
                w.write_raw_byte(b'<');
                for i in 0..len {
                    if i > 0 {
                        w.write_raw_byte(b',');
                    }
                    write_type_like_canonical(reader, w, with_prefix)?;
                }
                w.write_raw_byte(b'>');
            }
        }
        8 => w.write_raw_str("u16"),
        9 => w.write_raw_str("u32"),
        10 => w.write_raw_str("u256"),
        _ => return Err("unsupported type tag"),
    }
    Ok(())
}

fn skip_changed_object_entry(reader: &mut BcsReader<'_>) -> Result<()> {
    reader.skip_bytes(32)?;
    skip_object_in(reader)?;
    skip_object_out(reader)?;
    skip_id_operation(reader)
}

fn skip_unchanged_consensus_object_entry(reader: &mut BcsReader<'_>) -> Result<()> {
    reader.skip_bytes(32)?;
    match reader.read_uleb128()? {
        0 => {
            reader.skip_bytes(8)?;
            reader.skip_digest32()?;
        }
        1..=3 => {
            reader.skip_bytes(8)?;
        }
        4 => {}
        _ => return Err("unsupported unchanged consensus kind"),
    }
    Ok(())
}

fn skip_object_in(reader: &mut BcsReader<'_>) -> Result<()> {
    match reader.read_uleb128()? {
        0 => Ok(()),
        1 => {
            reader.skip_bytes(8)?;
            reader.skip_digest32()?;
            skip_owner(reader)
        }
        _ => Err("unsupported object input"),
    }
}

fn skip_object_out(reader: &mut BcsReader<'_>) -> Result<()> {
    match reader.read_uleb128()? {
        0 => Ok(()),
        1 => {
            reader.skip_digest32()?;
            skip_owner(reader)
        }
        2 => {
            reader.skip_bytes(8)?;
            reader.skip_digest32()
        }
        3 => skip_accumulator_write(reader),
        _ => Err("unsupported object output"),
    }
}

fn skip_owner(reader: &mut BcsReader<'_>) -> Result<()> {
    match reader.read_uleb128()? {
        0 | 1 => reader.skip_bytes(32),
        2 => reader.skip_bytes(8),
        3 => Ok(()),
        4 => {
            reader.skip_bytes(8)?;
            reader.skip_bytes(32)
        }
        _ => Err("unsupported owner"),
    }
}

fn skip_accumulator_write(reader: &mut BcsReader<'_>) -> Result<()> {
    reader.skip_bytes(32)?;
    skip_type_like(reader)?;
    match reader.read_uleb128()? {
        0 | 1 => {}
        _ => return Err("unsupported accumulator operation"),
    }
    match reader.read_uleb128()? {
        0 => reader.skip_bytes(8)?,
        1 => {
            reader.skip_bytes(8)?;
            reader.skip_bytes(8)?;
        }
        2 => {
            let len = reader.read_vec_len()?;
            for _ in 0..len {
                reader.skip_bytes(8)?;
                reader.skip_digest32()?;
            }
        }
        _ => return Err("unsupported accumulator value"),
    }
    Ok(())
}

fn skip_id_operation(reader: &mut BcsReader<'_>) -> Result<()> {
    match reader.read_uleb128()? {
        0..=2 => Ok(()),
        _ => Err("unsupported id operation"),
    }
}

fn skip_call_arg(reader: &mut BcsReader<'_>) -> Result<()> {
    match reader.read_uleb128()? {
        0 => {
            let len = reader.read_vec_len()?;
            reader.skip_bytes(len)
        }
        1 => skip_object_arg(reader),
        2 => skip_funds_withdrawal(reader),
        _ => Err("unsupported call arg"),
    }
}

fn skip_object_arg(reader: &mut BcsReader<'_>) -> Result<()> {
    match reader.read_uleb128()? {
        0 | 2 => {
            reader.skip_bytes(32)?;
            reader.skip_bytes(8)?;
            reader.skip_digest32()
        }
        1 => {
            reader.skip_bytes(32)?;
            reader.skip_bytes(8)?;
            match reader.read_uleb128()? {
                0..=2 => Ok(()),
                _ => Err("unsupported shared object mutability"),
            }
        }
        _ => Err("unsupported object arg"),
    }
}

fn skip_funds_withdrawal(reader: &mut BcsReader<'_>) -> Result<()> {
    match reader.read_uleb128()? {
        0 => reader.skip_bytes(8)?,
        _ => return Err("unsupported reservation"),
    }
    match reader.read_uleb128()? {
        0 => skip_type_like(reader)?,
        _ => return Err("unsupported withdrawal type"),
    }
    match reader.read_uleb128()? {
        0 | 1 => Ok(()),
        _ => Err("unsupported withdrawal source"),
    }
}

fn skip_command(reader: &mut BcsReader<'_>) -> Result<()> {
    match reader.read_uleb128()? {
        0 => skip_move_call(reader),
        1 => {
            let len = reader.read_vec_len()?;
            for _ in 0..len {
                skip_argument(reader)?;
            }
            skip_argument(reader)
        }
        2 | 3 => {
            skip_argument(reader)?;
            let len = reader.read_vec_len()?;
            for _ in 0..len {
                skip_argument(reader)?;
            }
            Ok(())
        }
        4 => {
            let modules = reader.read_vec_len()?;
            for _ in 0..modules {
                let len = reader.read_vec_len()?;
                reader.skip_bytes(len)?;
            }
            reader.skip_vec_of_fixed(32)
        }
        5 => {
            if reader.read_option_tag()? {
                skip_type_like(reader)?;
            }
            let len = reader.read_vec_len()?;
            for _ in 0..len {
                skip_argument(reader)?;
            }
            Ok(())
        }
        6 => {
            let modules = reader.read_vec_len()?;
            for _ in 0..modules {
                let len = reader.read_vec_len()?;
                reader.skip_bytes(len)?;
            }
            let deps = reader.read_vec_len()?;
            reader.skip_bytes(deps.checked_mul(32).ok_or("overflow")?)?;
            reader.skip_bytes(32)?;
            skip_argument(reader)
        }
        _ => Err("unsupported command"),
    }
}

fn skip_move_call(reader: &mut BcsReader<'_>) -> Result<()> {
    reader.skip_bytes(32)?;
    reader.skip_string()?;
    reader.skip_string()?;
    let type_args = reader.read_vec_len()?;
    for _ in 0..type_args {
        skip_type_like(reader)?;
    }
    let args = reader.read_vec_len()?;
    for _ in 0..args {
        skip_argument(reader)?;
    }
    Ok(())
}

fn skip_argument(reader: &mut BcsReader<'_>) -> Result<()> {
    match reader.read_uleb128()? {
        0 => Ok(()),
        1 | 2 => reader.skip_bytes(2),
        3 => reader.skip_bytes(4),
        _ => Err("unsupported argument"),
    }
}

fn skip_gas_data(reader: &mut BcsReader<'_>) -> Result<()> {
    let payment = reader.read_vec_len()?;
    for _ in 0..payment {
        reader.skip_bytes(32)?;
        reader.skip_bytes(8)?;
        reader.skip_digest32()?;
    }
    reader.skip_bytes(32)?;
    reader.skip_bytes(8)?;
    reader.skip_bytes(8)
}

fn skip_transaction_expiration(reader: &mut BcsReader<'_>) -> Result<()> {
    match reader.read_uleb128()? {
        0 => Ok(()),
        1 => reader.skip_bytes(8),
        2 => {
            for _ in 0..4 {
                if reader.read_option_tag()? {
                    reader.skip_bytes(8)?;
                }
            }
            reader.skip_bytes(32)?;
            reader.skip_bytes(4)
        }
        _ => Err("unsupported transaction expiration"),
    }
}

fn skip_type_like(reader: &mut BcsReader<'_>) -> Result<()> {
    match reader.read_uleb128()? {
        0 | 1 | 2 | 3 | 4 | 5 | 8 | 9 | 10 => Ok(()),
        6 => skip_type_like(reader),
        7 => {
            reader.skip_bytes(32)?;
            reader.skip_string()?;
            reader.skip_string()?;
            let len = reader.read_vec_len()?;
            for _ in 0..len {
                skip_type_like(reader)?;
            }
            Ok(())
        }
        _ => Err("unsupported type tag"),
    }
}

fn skip_execution_error_fields(reader: &mut BcsReader<'_>, tag: u64) -> Result<()> {
    match tag {
        0 | 1 | 2 | 3 | 7 | 8 | 9 | 10 | 13 | 14 | 15 | 16 | 17 | 18 | 23 | 25 | 26 | 29 | 30
        | 31 | 32 | 36 | 39 | 40 => Ok(()),
        4 | 5 | 24 | 28 | 37 | 38 => {
            reader.skip_bytes(8)?;
            reader.skip_bytes(8)
        }
        6 | 41 => reader.skip_bytes(32),
        11 => skip_move_location_opt(reader),
        19 => {
            reader.skip_bytes(2)?;
            skip_command_argument_error(reader)
        }
        20 => {
            reader.skip_bytes(2)?;
            match reader.read_uleb128()? {
                0 | 1 => Ok(()),
                _ => Err("unsupported type argument error"),
            }
        }
        21 => reader.skip_bytes(4),
        22 => reader.skip_bytes(2),
        27 => skip_package_upgrade_error(reader),
        33 => {
            let len = reader.read_vec_len()?;
            reader.skip_bytes(len.checked_mul(32).ok_or("overflow")?)
        }
        34 => {
            reader.skip_bytes(32)?;
            reader.skip_string()
        }
        35 => reader.skip_string(),
        _ => Err("unsupported execution error"),
    }
}

fn skip_move_location_opt(reader: &mut BcsReader<'_>) -> Result<()> {
    if reader.read_option_tag()? {
        reader.skip_bytes(32)?;
        reader.skip_string()?;
        reader.skip_bytes(2)?;
        reader.skip_bytes(2)?;
        if reader.read_option_tag()? {
            reader.skip_string()?;
        }
    }
    Ok(())
}

fn skip_command_argument_error(reader: &mut BcsReader<'_>) -> Result<()> {
    match reader.read_uleb128()? {
        0 | 1 | 2 | 3 | 7 | 8 | 9 | 10 | 11 | 12 | 13 | 14 | 15 | 16 | 17 | 18 => Ok(()),
        4 | 6 => reader.skip_bytes(2),
        5 => reader.skip_bytes(4),
        _ => Err("unsupported command argument error"),
    }
}

fn skip_package_upgrade_error(reader: &mut BcsReader<'_>) -> Result<()> {
    match reader.read_uleb128()? {
        0 | 1 => reader.skip_bytes(32),
        2 => Ok(()),
        3 => {
            let len = reader.read_vec_len()?;
            reader.skip_bytes(len)
        }
        4 => reader.skip_bytes(1),
        5 => {
            reader.skip_bytes(32)?;
            reader.skip_bytes(32)
        }
        _ => Err("unsupported package upgrade error"),
    }
}

fn shared_mutability_name(tag: u64) -> Result<&'static str> {
    match tag {
        0 => Ok("IMMUTABLE"),
        1 => Ok("MUTABLE"),
        2 => Ok("NON_EXCLUSIVE_WRITE"),
        _ => Err("unsupported shared object mutability"),
    }
}

fn execution_error_kind_name(tag: u64) -> Result<&'static str> {
    match tag {
        0 => Ok("InsufficientGas"),
        1 => Ok("InvalidGasObject"),
        2 => Ok("InvariantViolation"),
        3 => Ok("FeatureNotYetSupported"),
        4 => Ok("MoveObjectTooBig"),
        5 => Ok("MovePackageTooBig"),
        6 => Ok("CircularObjectOwnership"),
        7 => Ok("InsufficientCoinBalance"),
        8 => Ok("CoinBalanceOverflow"),
        9 => Ok("PublishErrorNonZeroAddress"),
        10 => Ok("SuiMoveVerificationError"),
        11 => Ok("MovePrimitiveRuntimeError"),
        12 => Ok("MoveAbort"),
        13 => Ok("VMVerificationOrDeserializationError"),
        14 => Ok("VMInvariantViolation"),
        15 => Ok("FunctionNotFound"),
        16 => Ok("ArityMismatch"),
        17 => Ok("TypeArityMismatch"),
        18 => Ok("NonEntryFunctionInvoked"),
        19 => Ok("CommandArgumentError"),
        20 => Ok("TypeArgumentError"),
        21 => Ok("UnusedValueWithoutDrop"),
        22 => Ok("InvalidPublicFunctionReturnType"),
        23 => Ok("InvalidTransferObject"),
        24 => Ok("EffectsTooLarge"),
        25 => Ok("PublishUpgradeMissingDependency"),
        26 => Ok("PublishUpgradeDependencyDowngrade"),
        27 => Ok("PackageUpgradeError"),
        28 => Ok("WrittenObjectsTooLarge"),
        29 => Ok("CertificateDenied"),
        30 => Ok("SuiMoveVerificationTimedout"),
        31 => Ok("SharedObjectOperationNotAllowed"),
        32 => Ok("InputObjectDeleted"),
        33 => Ok("ExecutionCancelledDueToSharedObjectCongestion"),
        34 => Ok("AddressDeniedForCoin"),
        35 => Ok("CoinTypeGlobalPause"),
        36 => Ok("ExecutionCancelledDueToRandomnessUnavailable"),
        37 => Ok("MoveVectorElemTooBig"),
        38 => Ok("MoveRawValueTooBig"),
        39 => Ok("InvalidLinkage"),
        40 => Ok("InsufficientFundsForWithdraw"),
        41 => Ok("NonExclusiveWriteInputObjectModified"),
        _ => Err("unsupported execution error"),
    }
}

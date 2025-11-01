const std = @import("std");

pub const ChunkType = enum(u8) {
    document = 0,
    change = 1,
    _,
};

pub const Chunk = union(ChunkType) {
    document: DocumentChunk,
    change: ChangeChunk,

    pub fn read(reader: *std.Io.Reader, allocator: std.mem.Allocator) !Chunk {
        const header = try ChunkHeader.read(reader);

        const content = try reader.readAlloc(allocator, header.content_len);
        var r = std.Io.Reader.fixed(content);
        // TODO:
        // var hash = std.crypto.hash.Sha1.init(.{});
        // &hash.update(hea);

        return switch (header.type) {
            .document => Chunk{ .document = try DocumentChunk.read(&r, allocator) },
            .change => Chunk{ .change = try ChangeChunk.read(&r, allocator) },
            else => error.TODO,
        };
    }
};

pub const ColumnMetadata = struct {
    spec: ColumSpecification,
    len: u64,
};

const ColumSpecification = packed struct(u32) {
    type: ColumnSpecificationType,
    deflate: bool,
    id: u28,
};

test "ColumSpecification" {
    const spec: ColumSpecification = @bitCast(@as(u32, 0b11001));
    try std.testing.expect(spec.id == 1);
    try std.testing.expect(spec.deflate == true);
    try std.testing.expect(spec.type == .actor);
}

pub const ColumnSpecificationType = enum(u3) {
    group,
    actor,
    ulEB,
    delta,
    boolean,
    string,
    value_metadata,
    value,
};

const ValueMetadataColumn = packed struct(u64) {
    type: u4, // Raw 4-bit value instead of enum
    len: u60,
};

const ValueType = enum(u4) {
    null = 0,
    false,
    true,
    u64,
    i64,
    f64,
    uft8,
    bytes,
    counter,
    timestamp,
};

// Value metadata structure
pub const ValueMetadata = struct {
    value_type: ValueType,
    length: u60,
};

// Parsed value data
pub const ParsedValue = union(ValueType) {
    null: void,
    false: void,
    true: void,
    u64: u64,
    i64: i64,
    f64: f64,
    uft8: []u8,
    bytes: []u8,
    counter: i64,
    timestamp: i64,
};

pub const ChangeColumnType = enum(u32) {
    actor = 1,
    sequence_number = 3,
    max_op = 19,
    time = 35,
    message = 53,
    dependencies_group = 64,
    dependencies_index = 67,
    extra_metadata = 86,
    extra_data = 85,
};

pub const ChangeColumn = union(ChangeColumnType) {
    actor: []u32,
    sequence_number: []i64,
    max_op: []i64,
    time: []i64,
    message: [][]u8,
    dependencies_group: []u8,
    dependencies_index: []u8,
    extra_metadata: []ValueMetadata,
    extra_data: []ParsedValue,
};

// Parsed column data types
pub const ColumnData = union(ColumnSpecificationType) {
    group: []u8, // Raw group data
    actor: []u32, // Actor IDs as uleb128 values
    ulEB: []u64, // Unsigned LEB128 values
    delta: []i64, // Delta encoded signed values
    boolean: []bool, // Boolean values
    string: [][]u8, // String data
    value_metadata: []ValueMetadata, // Parsed value metadata
    value: []ParsedValue, // Parsed value data
};

pub const DocumentChunk = struct {
    actors: std.ArrayList([]u8),
    change_hashes: std.ArrayList([32]u8),
    change_columns_metadata: std.ArrayList(ColumnMetadata),
    operation_columns_metadata: std.ArrayList(ColumnMetadata),
    change_columns_data: std.ArrayList(ChangeColumn),
    operation_columns_data: std.ArrayList(ColumnData),

    pub fn read(reader: *std.Io.Reader, allocator: std.mem.Allocator) !DocumentChunk {
        const actors = try readActorIds(reader, allocator);
        const change_hashes = try readChangeHashes(reader, allocator);
        const change_columns_metadata = try readColumMetadata(reader, allocator);
        const operation_columns_metadata = try readColumMetadata(reader, allocator);
        const change_columns_data = try readChangeColumns(reader, allocator, change_columns_metadata);
        const operation_columns_data = try readColumnData(reader, allocator, operation_columns_metadata);

        return .{
            .actors = actors,
            .change_hashes = change_hashes,
            .change_columns_metadata = change_columns_metadata,
            .operation_columns_metadata = operation_columns_metadata,
            .change_columns_data = change_columns_data,
            .operation_columns_data = operation_columns_data,
        };
    }

    pub fn readActorIds(reader: *std.Io.Reader, allocator: std.mem.Allocator) !std.ArrayList([]u8) {
        const len = try std.leb.readUleb128(usize, reader.adaptToOldInterface());
        var ret = try std.ArrayList([]u8).initCapacity(allocator, len);

        for (0..len) |_| {
            const actor_len = try std.leb.readUleb128(usize, reader.adaptToOldInterface());
            const id = try ret.addOne(allocator);
            id.* = try allocator.alloc(u8, actor_len);
            try reader.readSliceAll(id.*);
        }

        return ret;
    }

    pub fn readChangeHashes(reader: *std.Io.Reader, allocator: std.mem.Allocator) !std.ArrayList([32]u8) {
        const len = try std.leb.readUleb128(usize, reader.adaptToOldInterface());
        var ret = try std.ArrayList([32]u8).initCapacity(allocator, len);

        for (0..len) |_| {
            var hash: [32]u8 = undefined;
            try reader.readSliceAll(&hash);
            try ret.append(allocator, hash);
        }

        return ret;
    }
    pub fn readColumMetadata(reader: *std.Io.Reader, allocator: std.mem.Allocator) !std.ArrayList(ColumnMetadata) {
        const legacy_reader = reader.adaptToOldInterface();
        const len = try std.leb.readUleb128(usize, legacy_reader);
        var ret = try std.ArrayList(ColumnMetadata).initCapacity(allocator, len);

        for (0..len) |_| {
            try ret.append(allocator, .{
                .spec = @bitCast(try std.leb.readUleb128(u32, legacy_reader)),
                .len = try std.leb.readUleb128(u64, legacy_reader),
            });
        }

        return ret;
    }

    pub fn readChangeColumns(reader: *std.Io.Reader, allocator: std.mem.Allocator, metadata: std.ArrayList(ColumnMetadata)) !std.ArrayList(ChangeColumn) {
        var ret = try std.ArrayList(ChangeColumn).initCapacity(allocator, metadata.items.len);

        for (metadata.items) |meta| {
            const raw_data = try allocator.alloc(u8, meta.len);
            try reader.readSliceAll(raw_data);

            const column_data = try parseColumnData(allocator, meta.spec.type, raw_data);
            const changeColType: ChangeColumnType = @enumFromInt(@as(u32, @bitCast(meta.spec)));
            const c = switch (changeColType) {
                .actor => ChangeColumn{ .actor = column_data.actor },
                .sequence_number => ChangeColumn{ .sequence_number = column_data.delta },
                .max_op => ChangeColumn{ .max_op = column_data.delta },
                .time => ChangeColumn{ .time = column_data.delta },
                .message => ChangeColumn{ .message = column_data.string },
                .dependencies_group => ChangeColumn{ .dependencies_group = column_data.group },
                .dependencies_index => ChangeColumn{ .dependencies_index = column_data.group },
                .extra_metadata => ChangeColumn{ .extra_metadata = column_data.value_metadata },
                .extra_data => ChangeColumn{ .extra_data = column_data.value },
            };
            try ret.append(allocator, c);
        }

        return ret;
    }

    pub fn readColumnData(reader: *std.Io.Reader, allocator: std.mem.Allocator, metadata: std.ArrayList(ColumnMetadata)) !std.ArrayList(ColumnData) {
        var ret = try std.ArrayList(ColumnData).initCapacity(allocator, metadata.items.len);

        for (metadata.items) |meta| {
            const raw_data = try allocator.alloc(u8, meta.len);
            try reader.readSliceAll(raw_data);

            const column_data = try parseColumnData(allocator, meta.spec.type, raw_data);
            try ret.append(allocator, column_data);
        }

        return ret;
    }

    fn parseColumnData(allocator: std.mem.Allocator, column_type: ColumnSpecificationType, raw_data: []u8) !ColumnData {
        switch (column_type) {
            .group => {
                // For now, just return raw data for group columns
                return ColumnData{ .group = raw_data };
            },
            .actor => {
                // Parse actor IDs as sequence of uleb128 values
                var actor_ids = std.ArrayList(u32).initCapacity(allocator, 0) catch unreachable;
                var stream = std.io.fixedBufferStream(raw_data);
                const reader = stream.reader();

                while (stream.pos < raw_data.len) {
                    const actor_id = std.leb.readUleb128(u32, reader) catch break;
                    try actor_ids.append(allocator, actor_id);
                }

                return ColumnData{ .actor = actor_ids.items };
            },
            .ulEB => {
                // Parse unsigned LEB128 values
                var values = std.ArrayList(u64).initCapacity(allocator, 0) catch unreachable;
                var stream = std.io.fixedBufferStream(raw_data);
                const reader = stream.reader();

                while (stream.pos < raw_data.len) {
                    const value = std.leb.readUleb128(u64, reader) catch break;
                    try values.append(allocator, value);
                }

                return ColumnData{ .ulEB = values.items };
            },
            .delta => {
                // Parse delta-encoded signed values
                var values = std.ArrayList(i64).initCapacity(allocator, 0) catch unreachable;
                var stream = std.io.fixedBufferStream(raw_data);
                const reader = stream.reader();
                var current: i64 = 0;

                while (stream.pos < raw_data.len) {
                    const delta = std.leb.readIleb128(i64, reader) catch break;
                    current += delta;
                    try values.append(allocator, current);
                }

                return ColumnData{ .delta = values.items };
            },
            .boolean => {
                // Parse boolean values (each byte represents a boolean)
                var bools = try allocator.alloc(bool, raw_data.len);
                for (raw_data, 0..) |byte, i| {
                    bools[i] = byte != 0;
                }

                return ColumnData{ .boolean = bools };
            },
            .string => {
                // Parse string data - format: length-prefixed strings
                var strings = std.ArrayList([]u8).initCapacity(allocator, 0) catch unreachable;
                var stream = std.io.fixedBufferStream(raw_data);
                const reader = stream.reader();

                while (stream.pos < raw_data.len) {
                    const str_len = std.leb.readUleb128(usize, reader) catch break;
                    if (stream.pos + str_len > raw_data.len) break;

                    const str_data = try allocator.alloc(u8, str_len);
                    const bytes_read = reader.readAll(str_data) catch break;
                    if (bytes_read != str_len) break;

                    try strings.append(allocator, str_data);
                }

                return ColumnData{ .string = strings.items };
            },
            .value_metadata => {
                // Parse value metadata - each entry is a packed 64-bit struct
                var metadata_list = std.ArrayList(ValueMetadata).initCapacity(allocator, 0) catch unreachable;
                var stream = std.io.fixedBufferStream(raw_data);
                const reader = stream.reader();

                while (stream.pos + 8 <= raw_data.len) {
                    const metadata_raw = std.leb.readUleb128(u64, reader) catch break;
                    const metadata_packed: ValueMetadataColumn = @bitCast(metadata_raw);

                    // Validate the raw value and convert to enum if valid
                    const valid_type: ValueType = if (metadata_packed.type <= 9)
                        @enumFromInt(metadata_packed.type)
                    else
                        ValueType.null;

                    try metadata_list.append(allocator, .{
                        .value_type = valid_type,
                        .length = metadata_packed.len,
                    });
                }

                return ColumnData{ .value_metadata = metadata_list.items };
            },
            .value => {
                // Parse value data based on their types
                // Note: This requires corresponding value_metadata to know types and lengths
                // For now, we'll parse basic types and leave complex parsing for later
                var values_list = std.ArrayList(ParsedValue).initCapacity(allocator, 0) catch unreachable;
                var stream = std.io.fixedBufferStream(raw_data);
                const reader = stream.reader();

                // Since we don't have metadata context here, we'll try to parse what we can
                // In a real implementation, this would be coordinated with value_metadata
                while (stream.pos < raw_data.len) {
                    // Try to read a type indicator first (this is a simplified approach)
                    const type_indicator = reader.readByte() catch break;

                    const parsed_value = switch (type_indicator) {
                        0 => ParsedValue{ .null = {} },
                        1 => ParsedValue{ .false = {} },
                        2 => ParsedValue{ .true = {} },
                        3 => blk: {
                            const val = std.leb.readUleb128(u64, reader) catch break;
                            break :blk ParsedValue{ .u64 = val };
                        },
                        4 => blk: {
                            const val = std.leb.readIleb128(i64, reader) catch break;
                            break :blk ParsedValue{ .i64 = val };
                        },
                        5 => blk: {
                            // Read f64 as 8 bytes and convert
                            var f64_bytes: [8]u8 = undefined;
                            const bytes_read = reader.readAll(&f64_bytes) catch break;
                            if (bytes_read != 8) break;
                            const val: f64 = @bitCast(std.mem.readInt(u64, &f64_bytes, .little));
                            break :blk ParsedValue{ .f64 = val };
                        },
                        6 => blk: { // UTF-8 string
                            const str_len = std.leb.readUleb128(usize, reader) catch break;
                            if (stream.pos + str_len > raw_data.len) break;

                            const str_data = try allocator.alloc(u8, str_len);
                            const bytes_read = reader.readAll(str_data) catch break;
                            if (bytes_read != str_len) break;

                            break :blk ParsedValue{ .uft8 = str_data };
                        },
                        7 => blk: { // Bytes
                            const bytes_len = std.leb.readUleb128(usize, reader) catch break;
                            if (stream.pos + bytes_len > raw_data.len) break;

                            const bytes_data = try allocator.alloc(u8, bytes_len);
                            const bytes_read = reader.readAll(bytes_data) catch break;
                            if (bytes_read != bytes_len) break;

                            break :blk ParsedValue{ .bytes = bytes_data };
                        },
                        8 => blk: { // Counter
                            const val = std.leb.readIleb128(i64, reader) catch break;
                            break :blk ParsedValue{ .counter = val };
                        },
                        9 => blk: { // Timestamp
                            const val = std.leb.readIleb128(i64, reader) catch break;
                            break :blk ParsedValue{ .timestamp = val };
                        },
                        else => break, // Unknown type, stop parsing
                    };

                    try values_list.append(allocator, parsed_value);
                }

                return ColumnData{ .value = values_list.items };
            },
        }
    }
};
pub const ChangeChunk = struct {
    actor_index: u32,
    sequence_number: u64,
    max_op: u64,
    timestamp: i64,
    message: ?[]u8,
    deps: std.ArrayList(u32),
    extra_bytes: []u8,
    columns_metadata: std.ArrayList(ColumnMetadata),
    columns_data: std.ArrayList(ColumnData),

    pub fn read(reader: *std.Io.Reader, allocator: std.mem.Allocator) !ChangeChunk {
        const legacy_reader = reader.adaptToOldInterface();

        // Read change header information
        const actor_index = try std.leb.readUleb128(u32, legacy_reader);
        const sequence_number = try std.leb.readUleb128(u64, legacy_reader);
        const max_op = try std.leb.readUleb128(u64, legacy_reader);
        const timestamp = try std.leb.readIleb128(i64, legacy_reader);

        // Read message (optional string)
        const message_len = try std.leb.readUleb128(usize, legacy_reader);
        var message: ?[]u8 = null;
        if (message_len > 0) {
            message = try allocator.alloc(u8, message_len);
            try reader.readSliceAll(message.?);
        }

        // Read dependencies
        const deps_len = try std.leb.readUleb128(usize, legacy_reader);
        var deps = try std.ArrayList(u32).initCapacity(allocator, deps_len);
        for (0..deps_len) |_| {
            const dep = try std.leb.readUleb128(u32, legacy_reader);
            try deps.append(allocator, dep);
        }

        // Read extra bytes
        const extra_len = try std.leb.readUleb128(usize, legacy_reader);
        const extra_bytes = try allocator.alloc(u8, extra_len);
        if (extra_len > 0) {
            try reader.readSliceAll(extra_bytes);
        }

        // Read column metadata and data
        const columns_metadata = try DocumentChunk.readColumMetadata(reader, allocator);
        const columns_data = try DocumentChunk.readColumnData(reader, allocator, columns_metadata);

        return .{
            .actor_index = actor_index,
            .sequence_number = sequence_number,
            .max_op = max_op,
            .timestamp = timestamp,
            .message = message,
            .deps = deps,
            .extra_bytes = extra_bytes,
            .columns_metadata = columns_metadata,
            .columns_data = columns_data,
        };
    }
};

pub const ChunkHeader = struct {
    const Self = @This();

    checksum: [4]u8,
    type: ChunkType,
    content_len: usize,

    pub fn read(reader: *std.Io.Reader) !Self {
        var buffer: [4]u8 = undefined;
        try reader.readSliceAll(&buffer);

        if (!std.mem.eql(u8, buffer[0..4], &MAGIC)) {
            return error.InvalidDocument;
        }

        try reader.readSliceAll(&buffer);
        var ct: [1]u8 = undefined;
        try reader.readSliceAll(&ct);
        const len = try std.leb.readUleb128(usize, reader.adaptToOldInterface());

        return Self{
            .checksum = buffer,
            .type = @enumFromInt(ct[0]),
            .content_len = len,
        };
    }
};

const MAGIC: [4]u8 = [4]u8{ 0x85, 0x6f, 0x4a, 0x83 };

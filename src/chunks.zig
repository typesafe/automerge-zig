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
            // .change => Chunk{ .document = .{} },
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
    type: ValueType,
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

// Parsed column data types
pub const ColumnData = union(ColumnSpecificationType) {
    group: []u8, // Raw group data for now
    actor: []u32, // Actor IDs as uleb128 values
    ulEB: []u64, // Unsigned LEB128 values
    delta: []i64, // Delta encoded signed values
    boolean: []bool, // Boolean values
    string: [][]u8, // String data
    value_metadata: []u8, // Raw value metadata for now
    value: []u8, // Raw value data for now
};

pub const DocumentChunk = struct {
    actors: std.ArrayList([]u8),
    change_hashes: std.ArrayList([32]u8),
    change_columns_metadata: std.ArrayList(ColumnMetadata),
    operation_columns_metadata: std.ArrayList(ColumnMetadata),
    change_columns_data: std.ArrayList(ColumnData),
    operation_columns_data: std.ArrayList(ColumnData),

    pub fn read(reader: *std.Io.Reader, allocator: std.mem.Allocator) !DocumentChunk {
        const actors = try readActorIds(reader, allocator);
        const change_hashes = try readChangeHashes(reader, allocator);
        const change_columns_metadata = try readColumMetadata(reader, allocator);
        const operation_columns_metadata = try readColumMetadata(reader, allocator);
        const change_columns_data = try readColumnData(reader, allocator, change_columns_metadata);
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
                // For now, just return raw data for value metadata
                return ColumnData{ .value_metadata = raw_data };
            },
            .value => {
                // For now, just return raw data for values
                return ColumnData{ .value = raw_data };
            },
        }
    }
};
pub const ChangeChunk = struct {};

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

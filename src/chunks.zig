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

const ColumSpecification = packed struct {
    id: u28,
    deflate: u1,
    type: ColumnSpecificationType,
};

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

pub const DocumentChunk = struct {
    actors: std.ArrayList([]u8),
    change_hashes: std.ArrayList([32]u8),
    change_columns_metadata: std.ArrayList(ColumnMetadata),
    operation_columns_metadata: std.ArrayList(ColumnMetadata),
    change_columns_data: std.ArrayList([]u8),
    operation_columns_data: std.ArrayList([]u8),

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

    pub fn readColumnData(reader: *std.Io.Reader, allocator: std.mem.Allocator, metadata: std.ArrayList(ColumnMetadata)) !std.ArrayList([]u8) {
        var ret = try std.ArrayList([]u8).initCapacity(allocator, metadata.items.len);

        for (metadata.items) |meta| {
            const data = try allocator.alloc(u8, meta.len);
            try reader.readSliceAll(data);
            try ret.append(allocator, data);
        }

        return ret;
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

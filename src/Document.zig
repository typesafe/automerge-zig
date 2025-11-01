//! Represents an Automerge document

const std = @import("std");
const Chunk = @import("chunks.zig").Chunk;
const Self = @This();

allocator: std.mem.Allocator,
arena: *std.heap.ArenaAllocator,
chunks: std.ArrayList(Chunk),

pub fn read(reader: *std.Io.Reader, allocator: std.mem.Allocator) !Self {
    var arena = try allocator.create(std.heap.ArenaAllocator);
    arena.* = std.heap.ArenaAllocator.init(allocator);

    var chunks = std.ArrayList(Chunk).initBuffer(&.{});
    while (true) {
        const chunk = try Chunk.read(reader, arena.allocator());
        try chunks.append(allocator, chunk);
        break;
    }

    return Self{
        .arena = arena,
        .allocator = allocator,
        .chunks = chunks,
    };
}

pub fn write(writer: *std.Io.Writer) !void {
    _ = writer;
    // TODO
}

pub fn init(allocator: std.mem.Allocator) Self {
    return Self{
        .arena = std.heap.ArenaAllocator.init(allocator),
        .allocator = allocator,
    };
}

pub fn deinit(self: *Self) void {
    self.arena.deinit();
    self.allocator.destroy(self.arena);
}

test "Document.read" {
    const file = try std.fs.cwd().openFile(
        "./tests/fixtures/exemplar",
        .{ .mode = .read_only },
    );

    defer file.close();
    var r = file.reader(&.{});

    var doc = try Self.read(&r.interface, std.testing.allocator_instance.allocator());
    defer doc.deinit();
}

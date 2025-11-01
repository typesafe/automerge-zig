//! Represents an Automerge document

const std = @import("std");
const Chunk = @import("chunks.zig").Chunk;
const Self = @This();

allocator: std.mem.Allocator,
arena: *std.heap.ArenaAllocator,
chunks: std.ArrayList(Chunk),

pub fn read(reader: *std.io.Reader, allocator: std.mem.Allocator) !Self {
    var arena = try allocator.create(std.heap.ArenaAllocator);
    arena.* = std.heap.ArenaAllocator.init(allocator);

    var chunks = try std.ArrayList(Chunk).initCapacity(arena.allocator(), 0);
    while (true) {
        const chunk = Chunk.read(reader, arena.allocator()) catch |err| switch (err) {
            error.EndOfStream => break,
            else => return err,
        };
        try chunks.append(arena.allocator(), chunk);
    }

    return Self{
        .arena = arena,
        .allocator = allocator,
        .chunks = chunks,
    };
}

pub fn write(writer: *std.io.Writer) !void {
    _ = writer;
    // TODO
}

pub fn init(allocator: std.mem.Allocator) !Self {
    var arena = try allocator.create(std.heap.ArenaAllocator);
    arena.* = std.heap.ArenaAllocator.init(allocator);

    return Self{
        .arena = arena,
        .allocator = allocator,
        .chunks = std.ArrayList(Chunk).init(arena.allocator()),
    };
}

pub fn deinit(self: *Self) void {
    self.chunks.deinit(self.allocator);
    self.arena.deinit();
    self.allocator.destroy(self.arena);
}

fn print(self: *const Self) void {
    std.debug.print("\n=== Automerge Document Parse Results ===\n", .{});
    std.debug.print("Number of chunks: {}\n\n", .{self.chunks.items.len});

    for (self.chunks.items, 0..) |chunk, i| {
        std.debug.print("Chunk {}: ", .{i});
        switch (chunk) {
            .document => |doc_chunk| {
                std.debug.print("DOCUMENT CHUNK\n", .{});
                std.debug.print("  Actors ({}):\n", .{doc_chunk.actors.items.len});
                for (doc_chunk.actors.items, 0..) |actor, j| {
                    std.debug.print("    [{}]: ", .{j});
                    for (actor) |byte| {
                        std.debug.print("{x:0>2}", .{byte});
                    }
                    std.debug.print("\n", .{});
                }

                std.debug.print("  Change Hashes ({}):\n", .{doc_chunk.change_hashes.items.len});
                for (doc_chunk.change_hashes.items, 0..) |hash, j| {
                    std.debug.print("    [{}]: ", .{j});
                    for (hash) |byte| {
                        std.debug.print("{x:0>2}", .{byte});
                    }
                    std.debug.print("\n", .{});
                }

                std.debug.print("  Change Column Metadata ({}):\n", .{doc_chunk.change_columns_metadata.items.len});
                for (doc_chunk.change_columns_metadata.items, 0..) |meta, j| {
                    std.debug.print("    [{}]: id={}, deflate={}, type={s}, len={}\n", .{ j, meta.spec.id, meta.spec.deflate, @tagName(meta.spec.type), meta.len });
                }

                std.debug.print("  Change Column Data ({}):\n", .{doc_chunk.change_columns_data.items.len});
                for (doc_chunk.change_columns_data.items, 0..) |data, j| {
                    std.debug.print("    [{}]: type={s}, ", .{ j, @tagName(data) });
                    switch (data) {
                        .actor => |ids| {
                            std.debug.print("actors=[", .{});
                            for (ids, 0..) |id, k| {
                                if (k > 0) std.debug.print(",", .{});
                                std.debug.print("{}", .{id});
                            }
                            std.debug.print("] (count={})\n", .{ids.len});
                        },
                        .sequence_number => |values| {
                            std.debug.print("sequence_numbers=[", .{});
                            for (values, 0..) |val, k| {
                                if (k > 0) std.debug.print(",", .{});
                                std.debug.print("{}", .{val});
                            }
                            std.debug.print("] (count={})\n", .{values.len});
                        },
                        .max_op => |values| {
                            std.debug.print("max_ops=[", .{});
                            for (values, 0..) |val, k| {
                                if (k > 0) std.debug.print(",", .{});
                                std.debug.print("{}", .{val});
                            }
                            std.debug.print("] (count={})\n", .{values.len});
                        },
                        .time => |values| {
                            std.debug.print("times=[", .{});
                            for (values, 0..) |val, k| {
                                if (k > 0) std.debug.print(",", .{});
                                std.debug.print("{}", .{val});
                            }
                            std.debug.print("] (count={})\n", .{values.len});
                        },
                        .message => |strings| {
                            std.debug.print("messages=[", .{});
                            for (strings, 0..) |str, k| {
                                if (k > 0) std.debug.print(",", .{});
                                std.debug.print("\"{s}\"", .{str});
                            }
                            std.debug.print("] (count={})\n", .{strings.len});
                        },
                        .dependencies_group => |bytes| {
                            for (bytes) |byte| {
                                std.debug.print("{x:0>2}", .{byte});
                            }
                            std.debug.print(" (len={})\n", .{bytes.len});
                        },
                        .dependencies_index => |bytes| {
                            for (bytes) |byte| {
                                std.debug.print("{x:0>2}", .{byte});
                            }
                            std.debug.print(" (len={})\n", .{bytes.len});
                        },
                        .extra_metadata => |metadata_list| {
                            std.debug.print("metadata=[", .{});
                            for (metadata_list, 0..) |meta, k| {
                                if (k > 0) std.debug.print(",", .{});
                                std.debug.print("{{type:{s},len:{}}}", .{ @tagName(meta.value_type), meta.length });
                            }
                            std.debug.print("] (count={})\n", .{metadata_list.len});
                        },
                        .extra_data => |values| {
                            std.debug.print("values=[", .{});
                            for (values, 0..) |val, k| {
                                if (k > 0) std.debug.print(",", .{});
                                switch (val) {
                                    .null => std.debug.print("null", .{}),
                                    .false => std.debug.print("false", .{}),
                                    .true => std.debug.print("true", .{}),
                                    .u64 => |v| std.debug.print("{}u", .{v}),
                                    .i64 => |v| std.debug.print("{}i", .{v}),
                                    .f64 => |v| std.debug.print("{d}f", .{v}),
                                    .uft8 => |s| std.debug.print("\"{s}\"", .{s}),
                                    .bytes => |b| {
                                        std.debug.print("bytes(", .{});
                                        for (b) |byte| std.debug.print("{x:0>2}", .{byte});
                                        std.debug.print(")", .{});
                                    },
                                    .counter => |c| std.debug.print("counter({})", .{c}),
                                    .timestamp => |t| std.debug.print("timestamp({})", .{t}),
                                }
                            }
                            std.debug.print("] (count={})\n", .{values.len});
                        },
                    }
                }

                std.debug.print("  Operation Column Metadata ({}):\n", .{doc_chunk.operation_columns_metadata.items.len});
                for (doc_chunk.operation_columns_metadata.items, 0..) |meta, j| {
                    std.debug.print("    [{}]: id={}, deflate={}, type={s}, len={}\n", .{ j, meta.spec.id, meta.spec.deflate, @tagName(meta.spec.type), meta.len });
                }

                std.debug.print("  Operation Column Data ({}):\n", .{doc_chunk.operation_columns_data.items.len});
                for (doc_chunk.operation_columns_data.items, 0..) |data, j| {
                    std.debug.print("    [{}]: type={s}, ", .{ j, @tagName(data) });
                    switch (data) {
                        .group => |bytes| {
                            for (bytes) |byte| {
                                std.debug.print("{x:0>2}", .{byte});
                            }
                            std.debug.print(" (len={})\n", .{bytes.len});
                        },
                        .actor => |ids| {
                            std.debug.print("actors=[", .{});
                            for (ids, 0..) |id, k| {
                                if (k > 0) std.debug.print(",", .{});
                                std.debug.print("{}", .{id});
                            }
                            std.debug.print("] (count={})\n", .{ids.len});
                        },
                        .ulEB => |values| {
                            std.debug.print("values=[", .{});
                            for (values, 0..) |val, k| {
                                if (k > 0) std.debug.print(",", .{});
                                std.debug.print("{}", .{val});
                            }
                            std.debug.print("] (count={})\n", .{values.len});
                        },
                        .delta => |values| {
                            std.debug.print("deltas=[", .{});
                            for (values, 0..) |val, k| {
                                if (k > 0) std.debug.print(",", .{});
                                std.debug.print("{}", .{val});
                            }
                            std.debug.print("] (count={})\n", .{values.len});
                        },
                        .boolean => |bools| {
                            std.debug.print("bools=[", .{});
                            for (bools, 0..) |b, k| {
                                if (k > 0) std.debug.print(",", .{});
                                std.debug.print("{}", .{b});
                            }
                            std.debug.print("] (count={})\n", .{bools.len});
                        },
                        .string => |strings| {
                            std.debug.print("strings=[", .{});
                            for (strings, 0..) |str, k| {
                                if (k > 0) std.debug.print(",", .{});
                                std.debug.print("\"{s}\"", .{str});
                            }
                            std.debug.print("] (count={})\n", .{strings.len});
                        },
                        .value_metadata => |metadata_list| {
                            std.debug.print("metadata=[", .{});
                            for (metadata_list, 0..) |meta, k| {
                                if (k > 0) std.debug.print(",", .{});
                                std.debug.print("{{type:{s},len:{}}}", .{ @tagName(meta.value_type), meta.length });
                            }
                            std.debug.print("] (count={})\n", .{metadata_list.len});
                        },
                        .value => |values| {
                            std.debug.print("values=[", .{});
                            for (values, 0..) |val, k| {
                                if (k > 0) std.debug.print(",", .{});
                                switch (val) {
                                    .null => std.debug.print("null", .{}),
                                    .false => std.debug.print("false", .{}),
                                    .true => std.debug.print("true", .{}),
                                    .u64 => |v| std.debug.print("{}u", .{v}),
                                    .i64 => |v| std.debug.print("{}i", .{v}),
                                    .f64 => |v| std.debug.print("{d}f", .{v}),
                                    .uft8 => |s| std.debug.print("\"{s}\"", .{s}),
                                    .bytes => |b| {
                                        std.debug.print("bytes(", .{});
                                        for (b) |byte| std.debug.print("{x:0>2}", .{byte});
                                        std.debug.print(")", .{});
                                    },
                                    .counter => |c| std.debug.print("counter({})", .{c}),
                                    .timestamp => |t| std.debug.print("timestamp({})", .{t}),
                                }
                            }
                            std.debug.print("] (count={})\n", .{values.len});
                        },
                    }
                }
            },
            .change => |change_chunk| {
                std.debug.print("CHANGE CHUNK\n", .{});
                std.debug.print("  Actor Index: {}\n", .{change_chunk.actor_index});
                std.debug.print("  Sequence Number: {}\n", .{change_chunk.sequence_number});
                std.debug.print("  Max Op: {}\n", .{change_chunk.max_op});
                std.debug.print("  Timestamp: {}\n", .{change_chunk.timestamp});

                if (change_chunk.message) |msg| {
                    std.debug.print("  Message: \"{s}\"\n", .{msg});
                } else {
                    std.debug.print("  Message: (none)\n", .{});
                }

                std.debug.print("  Dependencies ({}):\n", .{change_chunk.deps.items.len});
                for (change_chunk.deps.items, 0..) |dep, j| {
                    std.debug.print("    [{}]: {}\n", .{ j, dep });
                }

                std.debug.print("  Extra bytes: {} bytes\n", .{change_chunk.extra_bytes.len});

                std.debug.print("  Column Metadata ({}):\n", .{change_chunk.columns_metadata.items.len});
                for (change_chunk.columns_metadata.items, 0..) |meta, j| {
                    std.debug.print("    [{}]: id={}, deflate={}, type={s}, len={}\n", .{ j, meta.spec.id, meta.spec.deflate, @tagName(meta.spec.type), meta.len });
                }

                std.debug.print("  Column Data ({}):\n", .{change_chunk.columns_data.items.len});
                for (change_chunk.columns_data.items, 0..) |data, j| {
                    std.debug.print("    [{}]: type={s}, ", .{ j, @tagName(data) });
                    switch (data) {
                        .group => |bytes| {
                            for (bytes) |byte| {
                                std.debug.print("{x:0>2}", .{byte});
                            }
                            std.debug.print(" (len={})\n", .{bytes.len});
                        },
                        .actor => |ids| {
                            std.debug.print("actors=[", .{});
                            for (ids, 0..) |id, k| {
                                if (k > 0) std.debug.print(",", .{});
                                std.debug.print("{}", .{id});
                            }
                            std.debug.print("] (count={})\n", .{ids.len});
                        },
                        .ulEB => |values| {
                            std.debug.print("values=[", .{});
                            for (values, 0..) |val, k| {
                                if (k > 0) std.debug.print(",", .{});
                                std.debug.print("{}", .{val});
                            }
                            std.debug.print("] (count={})\n", .{values.len});
                        },
                        .delta => |values| {
                            std.debug.print("deltas=[", .{});
                            for (values, 0..) |val, k| {
                                if (k > 0) std.debug.print(",", .{});
                                std.debug.print("{}", .{val});
                            }
                            std.debug.print("] (count={})\n", .{values.len});
                        },
                        .boolean => |bools| {
                            std.debug.print("bools=[", .{});
                            for (bools, 0..) |b, k| {
                                if (k > 0) std.debug.print(",", .{});
                                std.debug.print("{}", .{b});
                            }
                            std.debug.print("] (count={})\n", .{bools.len});
                        },
                        .string => |strings| {
                            std.debug.print("strings=[", .{});
                            for (strings, 0..) |str, k| {
                                if (k > 0) std.debug.print(",", .{});
                                std.debug.print("\"{s}\"", .{str});
                            }
                            std.debug.print("] (count={})\n", .{strings.len});
                        },
                        .value_metadata => |metadata_list| {
                            std.debug.print("metadata=[", .{});
                            for (metadata_list, 0..) |meta, k| {
                                if (k > 0) std.debug.print(",", .{});
                                std.debug.print("{{type:{s},len:{}}}", .{ @tagName(meta.value_type), meta.length });
                            }
                            std.debug.print("] (count={})\n", .{metadata_list.len});
                        },
                        .value => |values| {
                            std.debug.print("values=[", .{});
                            for (values, 0..) |val, k| {
                                if (k > 0) std.debug.print(",", .{});
                                switch (val) {
                                    .null => std.debug.print("null", .{}),
                                    .false => std.debug.print("false", .{}),
                                    .true => std.debug.print("true", .{}),
                                    .u64 => |v| std.debug.print("{}u", .{v}),
                                    .i64 => |v| std.debug.print("{}i", .{v}),
                                    .f64 => |v| std.debug.print("{d}f", .{v}),
                                    .uft8 => |s| std.debug.print("\"{s}\"", .{s}),
                                    .bytes => |b| {
                                        std.debug.print("bytes(", .{});
                                        for (b) |byte| std.debug.print("{x:0>2}", .{byte});
                                        std.debug.print(")", .{});
                                    },
                                    .counter => |c| std.debug.print("counter({})", .{c}),
                                    .timestamp => |t| std.debug.print("timestamp({})", .{t}),
                                }
                            }
                            std.debug.print("] (count={})\n", .{values.len});
                        },
                    }
                }
            },
        }
        std.debug.print("\n", .{});
    }
    std.debug.print("=== End Parse Results ===\n\n", .{});
}

test "Document.read" {
    const file = try std.fs.cwd().openFile(
        "./tests/fixtures/exemplar",
        .{ .mode = .read_only },
    );

    defer file.close();
    var r = file.reader(&.{});

    var doc = try Self.read(&r.interface, std.testing.allocator);
    defer doc.deinit();

    doc.print();
}

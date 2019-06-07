namespace c_glib Thrift

enum FileSystemModel {
    PASTIS,     // Pastis model: overwrite everything
    BASIC,      // Basic dhtfs model: use indexing overlay
    SNAPSHOT    // Basic + snapshotting
}

enum InodeType {
    FILE,
    DIRECTORY,
    SYMLINK
}

enum InodeFlags {
    EXECUTABLE = 1
}

enum DirEntryDiffType {
    ADD,
    REMOVE
}

struct FileSystem {
    1: required string name,
    2: required i32 block_size,
    3: required i64 root,
    4: required i64 inception,
    5: FileSystemModel model = FileSystemModel.BASIC
}

struct FileData {
    1: required i64 size,
    2: list<binary> blocks,
    3: list<binary> indirect
}

struct FileDataIndirect {
    1: required list<binary> blocks,
    2: bool valid = true
}

struct DirEntry {
    1: required i64 inumber,
    2: required InodeType type
}

struct DirEntryDiff {
    1: required DirEntryDiffType diff_type,
    2: required DirEntry entry,
    3: required i64 mtime,
    4: required string name
}

struct DirData {
    1: map<string, DirEntry> entries,
    2: i64 count,
    3: list<binary> indirect
}

struct DirDataIndirect {
    1: map<string, DirEntry> entries,
    2: bool valid = true
}

struct SymLinkData {
    1: required string target
}

struct Inode {
    1: required i64 id,
    2: required i64 inumber,
    3: required InodeType type,
    4: required i64 mtime,
    5: required i32 flags = 0,
    6: optional FileData file_data,
    7: optional DirData directory_data
    8: optional SymLinkData symlink_data
}

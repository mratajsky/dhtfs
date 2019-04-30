namespace c_glib Thrift

enum FileSystemModel {
    PASTIS,     // Pastis model: overwrite everything
    BASIC,      // Basic dhtfs model: use indexing overlay
    PUBSUB,     // Basic + publish/subscribe
}

enum InodeType {
    FILE,
    DIRECTORY,
    SYMLINK
}

enum InodeFlags {
    EXECUTABLE = 1,
    DELETED = 2
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
    2: list<binary> blocks
}

struct DirEntry {
    1: required i64 inumber,
    2: required InodeType type,
    3: required string name
}

struct DirData {
    1: map<string, DirEntry> entries
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

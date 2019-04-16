namespace c_glib Thrift

struct FileSystem {
    1: string name,
    2: i8 model,
    3: i32 block_size,
    4: i64 root
}

union FileBlock {
    1: binary block,
    2: binary ptr
}

struct FileInode {
    1: i64 inumber,
    2: i64 size,
    3: i64 mtime,
    4: i32 mode,
    5: list<FileBlock> blocks
}

struct DirInode {
    1: i64 inumber,
    2: i64 mtime
}

union Inode {
    1: FileInode file,
    2: DirInode directory
}

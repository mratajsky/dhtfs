namespace c_glib Thrift

struct Peer {
	1: string host,
	2: i32 port
}

struct Bucket {
	1: list<BucketValue> values,
	2: i64 searchKeyMin,
	3: i64 searchKeyMax
}

struct BucketValue {
	1: i64 searchKey,
	2: binary value
}

exception StorageException {
	1: i32 errorCode,
	2: string errorMessage
}

service Rpc {
	// Retrieve a list of peers closest to the given key
	list<Peer> FindClosestPeers(1: binary key),

	// Store value under the given key
	void Put(1: binary key, 2: binary value),
	// Add value to a leaf bucket identified by the given key
	void Add(1: binary key, 2: BucketValue value),

	// Get value under the given key
	binary Get(1: binary key) throws (1: StorageException err),
	// Get value from a leaf bucket with the largest search key
	BucketValue GetLatest(1: binary key) throws (1: StorageException err),
	// Get values from a leaf bucket in the inclusive search range
	list<BucketValue> GetRange(1: binary key, 2: i64 searchKeyLow, 3: i64 searchKeyHigh)
		throws (1: StorageException err)
}

namespace c_glib Thrift

struct Peer {
	1: required string host,
	2: required i32 port
}

struct BucketValue {
	1: required i64 search_key,
	2: required binary value
}

struct Bucket {
	1: required i64 search_key_min,
	2: required i64 search_key_max,
	3: list<BucketValue> values
}

struct BucketKeys {
	1: required i64 search_key_min,
	2: required i64 search_key_max
}

exception StorageException {
	1: required i32 error_code,
	2: required string error_message
}

service Rpc {
	// Retrieve a list of peers closest to the given key
	list<Peer> FindClosestPeers(1: binary key),

	// Find DHT key for storing an entry with the given search key
	binary FindKey(1: string ident, 2: i64 search_key)
		throws (1: StorageException err)

	// Find search key bounds for the given DHT key
	BucketKeys GetBucketKeys(1: binary key)
		throws (1: StorageException err)

	// Store value under the given key
	void Put(1: binary key, 2: binary value),
	// Add value to a leaf bucket identified by the given key
	void Add(1: binary key, 2: BucketValue value, 3: string name,
		4: i64 search_key_min, 5: i64 search_key_max)
		throws (1: StorageException err)

	// Get value under the given key
	binary Get(1: binary key) throws (1: StorageException err),

	// Get value from a leaf bucket with the largest search key, but where
	// the key doesn't exceed to given maximum
	BucketValue GetLatestMax(1: binary key, 2: i64 search_key_max)
		throws (1: StorageException err),

	// Get values from a leaf bucket in the inclusive search range
	list<BucketValue> GetRange(1: string name, 2: i64 search_key_min,
		3: i64 search_key_max),
}

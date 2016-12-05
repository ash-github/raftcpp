#include <string>
#include "kv_storage.hpp"
#include "rocksdb_storage.hpp"
#include "raft_consensus.hpp"

int main(void)
{
	using db_type = timax::db::kv_storage<timax::db::rocksdb_storage>;

	db_type db{ "d:/temp/tmp/test_rocksdb" };
	db["key"] = "I have an apple pen.";
	std::string value = db["key"];
	return 0;
}
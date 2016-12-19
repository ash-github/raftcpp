#include <string>
#include <rest_rpc/rpc.hpp>
#include <storage/kv_storage.hpp>
#include <storage/rocksdb_storage.hpp>
#include <storage/raft_consensus.hpp>
#include "test_db.hpp"

int main(void)
{
	test_db();

	return 0;
}
#include <string>
#include <rest_rpc/rpc.hpp>
#include <storage/kv_storage.hpp>
#include <storage/rocksdb_storage.hpp>
#include <storage/raft_consensus.hpp>
#include "test_db.hpp"
#include "test_sequence_list.hpp"

int main(void)
{
	test_db();
	test_sequence_list();
	return 0;
}
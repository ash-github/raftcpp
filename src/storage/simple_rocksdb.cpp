#include <string>
#include <rest_rpc/rpc.hpp>
#include "kv_storage.hpp"
#include "rocksdb_storage.hpp"
#include "raft_consensus.hpp"

int main(int argc, char* argv[])
{
	// our rpc server type
	using timax::rpc::server;
	using timax::rpc::msgpack_codec;
	
	using server_type = server<msgpack_codec>;

	// our rocksdb storage engine
	using timax::db::kv_storage;
	using timax::db::rocksdb_storage;
	using timax::db::raft_consensus;
	using db_type = kv_storage<rocksdb_storage, raft_consensus>;

	// using these two types later
	using timax::rpc::exception;
	using timax::rpc::error_code;

	if (argc != 4)
		std::cerr << "Usage: kvcarbin <port-number> <db_path-string> <config_path-string>";

	auto port = boost::lexical_cast<uint16_t>(argv[1]);
	std::string db_path = argv[2];
	std::string config_path = argv[3];

	// initialize our db object
	//db_type db{ "d:/temp/tmp/test_rocksdb", "raft_config.txt" };
	db_type db{ db_path, config_path };

	// instantialize our server object
	server_type kv_store_service{ port, std::thread::hardware_concurrency() };

	// register put operation
	kv_store_service.register_handler("put", 
		[&db](std::string const& key, std::string const& value)
	{
		try
		{
			db[key] = value;
		}
		catch (std::exception const& e)
		{
			std::cout << e.what() << std::endl;
			throw exception{ error_code::FAIL, e.what() };
		}
	});

	// register get operation
	kv_store_service.register_handler("get", 
		[&db](std::string const& key) -> std::string
	{
		try
		{
			return db[key];
		}
		catch (std::exception const& e)
		{
			std::cout << e.what() << std::endl;
			throw exception{ error_code::FAIL, e.what() };
		}
	});

	// register del operation
	kv_store_service.register_handler("del",
		[&db](std::string const& key)
	{
		try
		{
			db.del(key);
		}
		catch (std::exception const& e)
		{
			std::cout << e.what() << std::endl;
			throw exception{ error_code::FAIL, e.what() };
		}
	});

	kv_store_service.start();
	std::getchar();
	kv_store_service.stop();
	return 0;
}
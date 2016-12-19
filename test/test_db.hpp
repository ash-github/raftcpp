#pragma once

using db_type = timax::db::rocksdb_storage;

void test_db_put_get()
{
	std::cout << "test_db_put_get" << std::endl;
	try
	{
		db_type db{ "d:/temp/tmp/test_db" };
		std::string key, value;
		key = "test_put";
		value = "timax";
		db.put(key, value);
		std::cout << "	Put key(" << key << ") value(" << value << ").\n";

		auto r = db.get("test_put");
		std::cout << "	Get key(" << key << ") value(" << r << ").\n";
		std::cout << "test_db_put_get success." << std::endl;
	}
	catch (std::exception const& e)
	{
		std::cout << e.what() << std::endl;
		std::cout << "test_db_put_get failed." << std::endl;
	}
}

void test_db_del()
{
	std::cout << "test_db_del" << std::endl;
	db_type db{ "d:/temp/tmp/test_db" };

	// delete a key put from the previous example
	try
	{
		std::string key = "test_put";
		std::cout << "	Del key(" << key << ").\n";
		db.del(key);
	}
	catch (std::exception const& e)
	{
		std::cout << e.what() << std::endl;
		std::cout << "test_db_del1 failed." << std::endl;
	}

	// delete a key that not exist
	try
	{
		std::string key = "key_not_exist";
		std::cout << "	Del key(" << key << ").\n";
		db.del(key);
		std::cout << "test_db_del1 surcess." << std::endl;
	}
	catch (std::exception const& e)
	{
		std::cout << e.what() << std::endl;
		std::cout << "test_db_del1 failed." << std::endl;
	}
}

void test_snapshot()
{
	std::cout << "test_snapshot" << std::endl;
	try
	{
		db_type db{ "d:/temp/tmp/test_db" };
		std::string key, value;
		key = "test_snapshot";
		value = "timax";

		std::cout << "	Put key(" << key << ") value(" << value << ").\n";
		db.put(key, value);
		
		auto snapshot = db.get_snapshot();
		std::cout << "	Get snapshot(" << snapshot << ").\n";
		db.release_snapshot(snapshot);
		db.del(key);
		std::cout << "test_snapshot success." << std::endl;
	}
	catch (std::exception const& e)
	{
		std::cout << e.what() << std::endl;
		std::cout << "test_snapshot failed." << std::endl;
	}
}

namespace detail
{
	using namespace std::string_literals;

	std::string keys[] = 
	{
		"test1"s, "test2"s, "test3"s, "test4"s,
		"test5"s, "test6"s, "test7"s, "test8"s,
	};

	std::string values[] =
	{
		"value1"s, "value2"s, "value3"s, "value4"s,
		"value5"s, "value6"s, "value7"s, "value8"s,
	};

	void prepare_data(db_type& db)
	{
		for (auto loop = 0ull; loop < sizeof(keys) / sizeof(std::string); ++loop)
		{
			db.put(keys[loop], values[loop]);
		}
	}

	void destroy_data(db_type& db)
	{
		for (auto const& key : keys)
		{
			db.del(key);
		}
	}
}

void test_snapshot_traverse()
{
	std::cout << "test_snapshot_traverse" << std::endl;
	try
	{
		db_type db{ "d:/temp/tmp/test_db" };
		detail::prepare_data(db);

		std::cout << "	Traverse throw snapshot iterator....\n";
		auto snapshot = db.get_snapshot();
		db.write_snapshot(snapshot, 
			[](std::string const& buffer) -> bool
		{
			std::string key, value;
			timax::db::snapshot_serializer::unpack(buffer, key, value);
			std::cout << "		Key(" << key << ") value(" << value << ")\n";
			return true;
		});

		db.release_snapshot(snapshot);
		detail::destroy_data(db);
		std::cout << "test_snapshot_traverse success." << std::endl;
	}
	catch (std::exception const& e)
	{
		std::cout << e.what() << std::endl;
		std::cout << "test_snapshot_traverse failed." << std::endl;
	}
}

void test_db()
{
	test_db_put_get();
	test_db_del();
	test_snapshot();
	test_snapshot_traverse();
}
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

void test_db()
{
	test_db_put_get();
	test_db_del();
}
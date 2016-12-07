#pragma once

#include <thread>
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <functional>

#include "serializer.hpp"

namespace timax { namespace db 
{
	class rocksdb_storage
	{
	public:
		using snapshot_ptr = rocksdb::Snapshot const*;

	public:
		explicit rocksdb_storage(std::string const& path)
		{
			init(path);
		}

		void put(std::string const& key, std::string const& value)
		{
			auto s = db_->Put(rocksdb::WriteOptions{}, key, value);
			if (!s.ok())
				throw std::runtime_error{ s.getState() };
		}

		void del(std::string const& key)
		{
			auto s = db_->Delete(rocksdb::WriteOptions{}, key);
			if (!s.ok())
				throw std::runtime_error{ s.getState() };
		}

		std::string get(std::string const& key)
		{
			std::string value;
			auto s = db_->Get(rocksdb::ReadOptions{}, key, &value);
			if (!s.ok())
				throw std::runtime_error{ s.getState() };
			return value;
		}

		snapshot_ptr get_snapshot() const
		{
			return db_->GetSnapshot();
		}

		bool write_snapshot(snapshot_ptr snapshot, std::function<bool(std::string const&)> const& writer)
		{
			if (nullptr == snapshot)
				return false;

			rocksdb::ReadOptions options;
			options.snapshot = snapshot;
			auto itr = db_->NewIterator(options);
			if (!itr->Valid())
				return false;

			while (itr->Valid())
			{
				std::string buffer;
				auto key = itr->value().ToString();
				auto value = itr->key().ToString();
				snapshot_serializer::pack(key, value, buffer);

				if (!writer(buffer))
					return false;

				itr->Next();
			}
			return true;
		}

	private:
		void init(std::string const& path)
		{
			rocksdb::Status s;

			rocksdb::Options op;
			op.IncreaseParallelism(std::thread::hardware_concurrency());
			op.OptimizeLevelStyleCompaction();
			op.create_if_missing = true;
			op.compression_per_level.resize(2);

			rocksdb::DB* db_raw = nullptr;
			s = rocksdb::DB::Open(op, path, &db_raw);
			if (rocksdb::Status::OK() != s)
			{
				throw std::runtime_error{ s.getState() };
			}

			db_.reset(db_raw);
		}

	private:
		std::unique_ptr<rocksdb::DB>	db_;
	};
} }
#pragma once

#include <iterator>
#include "../raft/raft.hpp"
#include "semaphore.hpp"

namespace timax { namespace db
{
	template <typename SnapshotPtr>
	class sequence_list
	{
	public:
		static constexpr size_t array_size = 512;
		static constexpr uint32_t max_array_count = 3;
		using snapshot_ptr = SnapshotPtr;
		using array_type = std::array<snapshot_ptr, array_size>;

	public:
		sequence_list()
			: snapshot_blocks_(1)
			, log_index_begin_(0)
		{
		}

		snapshot_ptr get_snapshot(int64_t log_index) const
		{
			if (log_index < log_index_begin_)
				return nullptr;

			log_index -= log_index_begin_;
			auto advance = log_index / array_size;

			if (advance >= snapshot_blocks_.size())
				return nullptr;

			auto itr = snapshot_blocks_.begin();
			if (advance > 1)
			{
				std::advance(itr, advance);
				log_index -= advance * array_size;
			}

			return (*itr)[log_index];
		}

		void put_snapshot(int64_t log_index, snapshot_ptr snapshot)
		{
			if (log_index < log_index_begin_ || nullptr == snapshot)
				return;

			log_index -= log_index_begin_;
			auto advance = log_index / array_size;
			log_index -= advance * array_size;

			auto itr = snapshot_blocks_.begin();
			if (advance > 0)
			{
				std::list<array_type> temp;
				if (advance >= snapshot_blocks_.size())
				{
					temp.resize(advance - snapshot_blocks_.size() + 1);
					std::move(temp.begin(), temp.end(), std::back_inserter(snapshot_blocks_));
				}
				std::advance(itr, advance);
			}

			(*itr)[log_index] = snapshot;

			while (advance > max_array_count)
			{
				snapshot_blocks_.pop_front();
				log_index_begin_ += array_size;
				--advance;
			}
		}

	private:
		std::list<array_type>		snapshot_blocks_;
		int64_t					log_index_begin_;
	};

	template <typename StoragePolicy>
	class raft_consensus
	{
	public:
		using storage_policy = StoragePolicy;
		using snapshot_ptr = typename storage_policy::snapshot_ptr;
		using sequence_list_type = sequence_list<snapshot_ptr>;

	public:
		raft_consensus(storage_policy& storage)
			: storage_(storage)
		{
		}

		void put(std::string const& key, std::string const& value)
		{
			check_leader();
			// serialize 'put' 'key' 'value' to a buffer
			std::string serialized_log;
			log_serializer::pack_write(serialized_log, key, value);
			// replicate the log
			auto log_index = replicate(std::move(serialized_log));
			put(log_index, key, value);
		}

		std::string get(std::string const& key)
		{
			check_leader();
			return storage_.get(key);
		}

		void del(std::string const& key)
		{
			check_leader();
			std::string serialized_log;
			log_serializer::pack_delete(serialized_log, key);
			auto log_index = replicate(std::move(serialized_log));
			del(log_index, key);
			// no need for del to write snapshot?
		}

	private:
		int64_t replicate(std::string&& data)
		{
			semaphore s;
			bool result;
			int64_t log_index;

			raft_.replicate(std::move(data), [&] (bool r, int64_t index)
			{
				result = r;
				log_index = index;
				s.signal();
			});

			s.wait();
			if (!result)
				throw std::runtime_error{ "Failed to replicate log." };

			return log_index;
		}

		void check_leader()
		{
			if (!raft_.check_leader())
				throw std::runtime_error{ "Not a leader" };
		}

		bool write_snapshot(std::function<bool(const std::string &)> const& writer, int64_t log_index)
		{
			return storage_.write_snapshort(snapshot_blocks_.get_snapshot(), writer);
		}

		void commit_entry(std::string&& buffer, int64_t log_index)
		{
			auto db_op = log_serializer::unpack(buffer);
			if (db_op.op_type = static_cast<int>(log_op::Write))
			{
				put(log_index, db_op.key, db_op.value);
			}
			else
			{
				del(log_index, db_op.key);
			}
		}

		void put(int64_t log_index, std::string const& key, std::string const& value)
		{
			// we actually commit the key-value
			storage_.put(key, value);

			// get snapshot
			auto snapshot = storage_.get_snapshot();

			// cache this snapshot with log_index
			snapshot_blocks_.put_snapshot(log_index, snapshot);
		}

		void del(int64_t log_index, std::string const& key)
		{
			storage_.del(key);
		}

	private:
		storage_policy&		storage_;
		sequence_list_type	snapshot_blocks_;
		xraft::raft			raft_;
	};
} }
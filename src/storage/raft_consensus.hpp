#pragma once

#include <iterator>

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

		snapshot_ptr get_snapshot(uint64_t log_index) const
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

		bool put_snapshot(uint64_t log_index, snapshot_ptr snapshot)
		{
			if (log_index < log_index_begin_)
				return false;

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

			return true;
		}

	private:
		std::list<array_type>		snapshot_blocks_;
		uint64_t					log_index_begin_;
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

		}

		std::string get(std::string const& key)
		{

		}

	private:
		storage_policy&		storage_;
		sequence_list_type	snapshot_blocks_;
	};
} }
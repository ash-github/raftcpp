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
		template <typename F>
		explicit sequence_list(F&& func)
			: snapshot_blocks_(1)
			, log_index_begin_(1)
			, release_func_(std::forward<F>(func))
		{
		}

		~sequence_list()
		{
			for (auto& block : snapshot_blocks_)
			{
				release(block);
			}
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

			auto log_idx = log_index - log_index_begin_;
			auto advance = log_idx / array_size;
			log_idx -= advance * array_size;

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

			(*itr)[log_idx] = snapshot;

			while (advance >= max_array_count)
			{
				release(snapshot_blocks_.front());
				snapshot_blocks_.pop_front();
				log_index_begin_ += array_size;
				--advance;
			}
		}

		size_t block_size() const noexcept
		{
			return snapshot_blocks_.size();
		}

		int64_t get_begin_log_index() const noexcept
		{
			return log_index_begin_;
		}

	private:
		void release(array_type& snapshots)
		{
			for (auto snapshot : snapshots)
			{
				if (nullptr != snapshot)
				{
					release_func_(snapshot);
				}
			}
		}

	private:
		std::list<array_type>				snapshot_blocks_;
		int64_t							log_index_begin_;
		std::function<void(snapshot_ptr)>	release_func_;
	};

	struct raft_consensus_config
	{
		struct raft_node
		{
			std::string	addr;
			uint16_t		port;
			std::string	id;

			META(addr, port, id);
		};

		uint32_t					append_log_timeout;		// append log timeout
		uint32_t					election_timeout;			// election timeout
		uint32_t					heartbeat_duration;			// heart beat duration
		std::string				log_path;
		std::string				snapshot_path;
		std::string				meta_path;
		raft_node				this_node;
		std::vector<raft_node>	peer_nodes;

		META(append_log_timeout, election_timeout, heartbeat_duration, 
			log_path, snapshot_path, meta_path, this_node, peer_nodes);
	};

	template <typename StoragePolicy>
	class raft_consensus
	{
	public:
		using storage_policy = StoragePolicy;
		using snapshot_ptr = typename storage_policy::snapshot_ptr;
		using sequence_list_type = sequence_list<snapshot_ptr>;

	public:
		raft_consensus(storage_policy& storage, std::string const& consensus_config_path)
			: storage_(storage)
			, snapshot_blocks_([this](snapshot_ptr snapshot) { storage_.release_snapshot(snapshot); })
		{
			init(consensus_config_path);
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
		void init(std::string const& consensus_config_path)
		{
			raft_.regist_build_snapshot_callback(
				[this](auto const& writer, auto log_index)
			{
				return write_snapshot(writer, log_index);
			});

			raft_.regist_install_snapshot_handle(
				[this](auto& in_stream)
			{
				storage_.install_from_file(in_stream);
			});

			raft_.regist_commit_entry_callback(
				[this](auto&& buffer, auto log_index)
			{
				commit_entry(std::move(buffer), log_index);
			});

			init_raft_config(consensus_config_path);
		}

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
			return storage_.write_snapshot(snapshot_blocks_.get_snapshot(log_index), writer);
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

		void init_raft_config(std::string const& consensus_config_path)
		{
			std::ifstream in_stream;
			in_stream.open(consensus_config_path);
			std::vector<char> buffer;
			in_stream.seekg(0, std::ios::end);
			buffer.resize(in_stream.tellg());
			in_stream.seekg(std::ios::beg);
			in_stream.read(buffer.data(), buffer.size());

			timax::rpc::kapok_codec codec{};
			auto consensus_config = codec.template unpack<raft_consensus_config>(buffer.data(), buffer.size());
			xraft::raft::raft_config config_internal;
			config_internal.append_log_timeout_ = static_cast<size_t>(consensus_config.append_log_timeout);
			config_internal.election_timeout_ = static_cast<size_t>(consensus_config.election_timeout);
			config_internal.heartbeat_interval_ = static_cast<size_t>(consensus_config.heartbeat_duration);
			config_internal.raftlog_base_path_ = consensus_config.log_path;
			config_internal.snapshot_base_path_ = consensus_config.snapshot_path;
			config_internal.metadata_base_path_ = consensus_config.meta_path;
			config_internal.myself_.ip_ = consensus_config.this_node.addr;
			config_internal.myself_.port_ = static_cast<int>(consensus_config.this_node.port);
			config_internal.myself_.raft_id_ = consensus_config.this_node.id;

			std::transform(consensus_config.peer_nodes.begin(), consensus_config.peer_nodes.end(),
				std::back_inserter(config_internal.peers_), [](auto const& elem)
			{
				return xraft::raft::raft_config::raft_node{ elem.addr, static_cast<int>(elem.port), elem.id };
			});

			raft_.init(config_internal);
		}

	private:
		storage_policy&		storage_;
		sequence_list_type	snapshot_blocks_;
		xraft::raft			raft_;
	};
} }
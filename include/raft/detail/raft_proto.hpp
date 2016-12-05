#pragma once
namespace xraft
{
	namespace detail
	{
		struct vote_request
		{
			int64_t term_ = 0;
			std::string candidate_;
			int64_t last_log_index_ = 0;
			int64_t last_log_term_ = 0;

			std::size_t bytes() const
			{
				return
					endec::get_sizeof(term_) +
					endec::get_sizeof(candidate_) +
					endec::get_sizeof(last_log_index_) +
					endec::get_sizeof(last_log_term_);
			}
			std::string to_string() const
			{
				std::string buffer;
				buffer.resize(bytes());
				uint8_t *ptr = (uint8_t*)buffer.data();
				endec::put_uint64(ptr, term_);
				endec::put_string(ptr, candidate_);
				endec::put_uint64(ptr, last_log_index_);
				endec::put_uint64(ptr, last_log_term_);
				return buffer;
			}
			bool from_string(const std::string &buffer)
			{
				uint8_t *ptr = (uint8_t*)buffer.data();
				candidate_.clear();
				if (buffer.size() < bytes())
					return false;
				term_ = (int64_t)endec::get_uint64(ptr);
				candidate_ = endec::get_string(ptr);
				last_log_index_ = (int64_t)endec::get_uint64(ptr);
				last_log_term_ = (int64_t)endec::get_uint64(ptr);
				return true;
			}
		};

		struct vote_response 
		{
			int64_t term_ = 0;
			bool vote_granted_ = false;
			bool log_ok_ = false;

			std::size_t bytes() const
			{
				return
					endec::get_sizeof(term_) +
					endec::get_sizeof(vote_granted_) +
					endec::get_sizeof(log_ok_);
			}
			std::string to_string() const
			{
				std::string buffer;
				buffer.resize(bytes());
				uint8_t *ptr = (uint8_t*)buffer.data();
				endec::put_uint64(ptr, term_);
				endec::put_uint8(ptr, (uint8_t)vote_granted_);
				endec::put_uint8(ptr, (uint8_t)log_ok_);
				return buffer;
			}
			bool from_string(const std::string &buffer)
			{
				uint8_t *ptr = (uint8_t*)buffer.data();
				if (buffer.size() != bytes())
					return false;
				term_ = (int64_t)endec::get_uint64(ptr);
				vote_granted_ = endec::get_uint8(ptr)>0;
				log_ok_= endec::get_uint8(ptr)>0;
				return true;
			}
		};

		struct log_entry
		{
			enum class type:char
			{
				e_append_log,
				e_configuration
			};
			int64_t index_ = 0;
			int64_t term_ = 0;
			type type_ = type::e_append_log;
			std::string log_data_;

			std::size_t bytes() const
			{
				return
					endec::get_sizeof(index_) +
					endec::get_sizeof(term_) +
					endec::get_sizeof(std::underlying_type<type>::type()) +
					endec::get_sizeof(log_data_);
			}
			std::string to_string() const
			{
				std::string buffer;
				buffer.resize(bytes());
				uint8_t *ptr = (uint8_t*)buffer.data();
				endec::put_uint64(ptr, index_);
				endec::put_uint64(ptr, term_);
				endec::put_uint8(ptr, static_cast<uint8_t>(type_));
				endec::put_string(ptr, log_data_);
				return buffer;
			}
			bool from_string(const std::string &buffer)
			{
				uint8_t *ptr = (uint8_t*)buffer.data();
				log_data_.clear();
				if (buffer.size() < bytes())
					return false;
				index_ = (int64_t)endec::get_uint64(ptr);
				term_ = (int64_t)endec::get_uint64(ptr);
				type_ = (type)endec::get_uint8(ptr);
				log_data_ = endec::get_string(ptr);
				return true;
			}
			bool from_string(uint8_t *&ptr)
			{
				log_data_.clear();
				index_ = (int64_t)endec::get_uint64(ptr);
				term_ = (int64_t)endec::get_uint64(ptr);
				type_ = (type)endec::get_uint8(ptr);
				log_data_ = endec::get_string(ptr);
				return true;
			}
		};
		struct raft_config
		{
			struct raft_node
			{
				std::string ip_;
				int port_ = 0;
				std::string raft_id_;
			};
			using nodes = std::vector<raft_node>;
			nodes nodes_;
			raft_node myself_;
			std::size_t append_log_timeout_;
			std::size_t election_timeout_;
			std::string raftlog_base_path_;
			std::string snapshot_base_path_;
			std::string metadata_base_path_;
		};
		struct append_entries_request
		{
			int64_t term_ = 0;
			std::string leader_id_ = 0;
			int64_t prev_log_index_ = 0;
			int64_t prev_log_term_ = 0;
			int64_t leader_commit_ = 0;
			std::list<log_entry>entries_;

			std::size_t bytes() const
			{
				std::size_t len = 
					endec::get_sizeof(term_) +
					endec::get_sizeof(leader_id_) +
					endec::get_sizeof(prev_log_index_) +
					endec::get_sizeof(prev_log_term_) +
					endec::get_sizeof(leader_commit_) +
					endec::get_sizeof(int32_t());
				for (auto &itr : entries_)
					len += itr.bytes();
			}
			std::string to_string() const
			{
				std::string buffer;
				buffer.resize(bytes());
				uint8_t *ptr = (uint8_t*)buffer.data();
				endec::put_uint64(ptr, term_);
				endec::put_string(ptr, leader_id_);
				endec::put_uint64(ptr, prev_log_index_);
				endec::put_uint64(ptr, prev_log_term_);
				endec::put_uint64(ptr, leader_commit_);
				endec::put_uint32(ptr, (uint32_t)entries_.size());
				for (auto &itr : entries_)
					buffer.append(itr.to_string());
				return buffer;
			}
			bool from_string(const std::string &buffer)
			{
				uint8_t *ptr = (uint8_t*)buffer.data();
				leader_id_.clear();
				entries_.clear();
				if (buffer.size() < bytes())
					return false;
				term_ = (int64_t)endec::get_uint64(ptr);
				leader_id_ = endec::get_string(ptr);
				prev_log_index_ = (int64_t)endec::get_uint64(ptr);
				prev_log_term_ = (int64_t)endec::get_uint64(ptr);
				leader_commit_ = (int64_t)endec::get_uint64(ptr);
				uint32_t entries_size = endec::get_uint32(ptr);
				for (size_t i = 0; i < entries_size; i++)
				{
					log_entry entry;
					entry.from_string(ptr);
					entries_.emplace_back(std::move(entry));
				}
				return true;
			}
		};

		struct append_entries_response
		{
			int64_t term_ = 0;
			int64_t last_log_index_ = 0;
			bool success_ = false;

			std::size_t bytes() const
			{
				return
					endec::get_sizeof(term_) +
					endec::get_sizeof(last_log_index_) +
					endec::get_sizeof(success_);
			}
			std::string to_string() const
			{
				std::string buffer;
				buffer.resize(bytes());
				uint8_t *ptr = (uint8_t*)buffer.data();
				endec::put_uint64(ptr, term_);
				endec::put_uint64(ptr, last_log_index_);
				endec::put_bool(ptr, success_);
				return buffer;
			}
			bool from_string(const std::string &buffer)
			{
				uint8_t *ptr = (uint8_t*)buffer.data();
				if (buffer.size() < bytes())
					return false;
				term_ = (int64_t)endec::get_uint64(ptr);
				last_log_index_ = (int64_t)endec::get_uint64(ptr);
				success_ = endec::get_bool(ptr);
				return true;
			}
		};

		struct install_snapshot_request
		{
			int64_t term_ = 0;
			int64_t last_snapshot_index_ = 0;
			int64_t last_included_term_ = 0;
			int64_t offset_ = 0;
			bool done_ = false;
			std::string leader_id_;
			std::string data_;

			std::size_t bytes() const
			{
				return
					endec::get_sizeof(term_) +
					endec::get_sizeof(last_snapshot_index_) +
					endec::get_sizeof(last_included_term_) +
					endec::get_sizeof(offset_) +
					endec::get_sizeof(done_) +
					endec::get_sizeof(data_);
			}

			std::string to_string() const
			{
				std::string buffer;
				buffer.resize(bytes());
				uint8_t *ptr = (uint8_t*)buffer.data();
				endec::put_uint64(ptr, term_);
				endec::put_uint64(ptr, last_snapshot_index_);
				endec::put_uint64(ptr, last_included_term_);
				endec::put_uint64(ptr, offset_);
				endec::put_bool(ptr, done_);
				endec::put_string(ptr, data_);
				return buffer;
			}

			bool from_string(const std::string &buffer)
			{
				uint8_t *ptr = (uint8_t*)buffer.data();
				if (buffer.size() < bytes())
					return false;
				term_ = (int64_t)endec::get_uint64(ptr);
				last_snapshot_index_ = (int64_t)endec::get_uint64(ptr);
				last_included_term_ = (int64_t)endec::get_uint64(ptr);
				offset_ = (int64_t)endec::get_uint64(ptr);
				done_ = endec::get_bool(ptr);
				data_ = endec::get_string(ptr);
				return true;
			}
		};

		struct install_snapshot_response
		{
			int64_t term_ = 0;
			int64_t bytes_stored_ = 0;

			std::size_t bytes() const
			{
				return
					endec::get_sizeof(term_) +
					endec::get_sizeof(bytes_stored_);
			}

			std::string to_string() const
			{
				std::string buffer;
				buffer.resize(bytes());
				uint8_t *ptr = (uint8_t*)buffer.data();
				endec::put_uint64(ptr, term_);
				endec::put_uint64(ptr, bytes_stored_);
				return buffer;
			}

			bool from_string(const std::string &buffer)
			{
				uint8_t *ptr = (uint8_t*)buffer.data();
				if (buffer.size() < bytes())
					return false;
				term_ = (int64_t)endec::get_uint64(ptr);
				bytes_stored_ = (int64_t)endec::get_uint64(ptr);
				return true;
			}
		};
	}
}
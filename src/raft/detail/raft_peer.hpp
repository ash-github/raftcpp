#pragma once
#define TRACE std::cout <<__FUNCTION__ <<std::endl;
namespace xraft
{
namespace detail
{
	using namespace std::chrono;

	namespace RPC
	{
		TIMAX_DEFINE_PROTOCOL(append_entries_request, detail::append_entries_response(detail::append_entries_request));
		TIMAX_DEFINE_PROTOCOL(vote_request, detail::vote_response(detail::vote_request));
		TIMAX_DEFINE_PROTOCOL(install_snapshot, detail::install_snapshot_response(detail::install_snapshot_request));
	}

	class raft_peer
	{
		
	public:
		enum class cmd_t
		{
			e_connect,
			e_election,
			e_append_entries,
			e_interrupt_vote,
			e_sleep,
			e_exit,

		};
		raft_peer(detail::raft_config::raft_node node)
			:myself_(node)
		{

		}
		~raft_peer()
		{
			stop();
		}
		void send_cmd(cmd_t cmd)
		{
			cmd_queue_.push(std::move(cmd));
			notify();
		}
		void notify()
		{
			utils::lock_guard locker(mtx_);
			cv_.notify_one();
		}
		void start()
		{
			peer_thread_ = std::thread([this] {  run(); });
			peer_thread_.detach();
		}
		void stop()
		{
			stop_ = true;
			notify();
		}
		std::function<void(raft_peer&, bool)> connect_callback_;
		std::function<int64_t(void)> get_current_term_;
		std::function<int64_t(void)> get_last_log_index_;
		std::function<append_entries_request(int64_t)> build_append_entries_request_;
		std::function<vote_request()> build_vote_request_;
		std::function<void(const vote_response &)> vote_response_callback_;
		std::function<void(int64_t)> new_term_callback_;
		std::function<void(const std::vector<int64_t>&)> append_entries_success_callback_;
		std::function<std::string()> get_snapshot_path_;
		std::string raft_id_;
		raft_config::raft_node myself_;
	private:
		void run()
		{
			do
			{
				if (!try_execute_cmd())
				{
					do_sleep(1000);
				}
			} while (!stop_);
		}
		void do_append_entries()
		{
			next_index_ = 0;
			match_index_ = 0;
			send_heartbeat_ = false;
			do
			{
				try
				{
					if (try_execute_cmd())
						break;
					int64_t index = get_last_log_index_();
					if (!next_index_ || next_index_ > index)
						next_index_ = index;
					if (index == match_index_ && send_heartbeat_)
					{
						send_heartbeat_ = false;
						do_sleep(next_heartbeat_delay());
						continue;
					}
					auto request = build_append_entries_request_(next_index_);
					if (request.entries_.empty() && next_index_ < index)
					{
						send_install_snapshot_req();
						continue;
					}
					auto response = send_append_entries_request(request);
					update_heartbeat_time();
					if (!response.success_)
					{
						match_index_ = 0;
						if (get_current_term_() < response.term_)
						{
							new_term_callback_(response.term_);
							return;
						}
						next_index_ = response.last_log_index_ + 1;
						if (next_index_ == 0)
							next_index_ = 1;
						continue;
					}
					match_index_ = response.last_log_index_;
					next_index_ = match_index_ + 1;

					if (request.entries_.empty())
						continue;

					std::vector<int64_t> indexs;
					indexs.reserve(request.entries_.size());
					for (auto &itr : request.entries_)
						indexs.push_back(itr.index_);
					append_entries_success_callback_(indexs);
				}
				catch (std::exception &e)
				{
					std::cout << e.what() << std::endl;
				}
			} while (true);
		}

		append_entries_response 
			send_append_entries_request(const append_entries_request &req)
		{
			return rpc_client_.call(endpoint_, RPC::append_entries_request, req);
		}

		void send_install_snapshot_req()
		{
			snapshot_reader reader;
			snapshot_head head;

			auto filepath = get_snapshot_path_();
			if (!reader.open(filepath))
			{
				std::cout << "open file :" + filepath << " error" << std::endl;
				return;
			}
			if (!reader.read_sanpshot_head(head))
				throw std::runtime_error("read_sanpshot_head failed");

			std::ifstream &file = reader.get_snapshot_stream();
			file.seekg(0, std::ios::beg);
			do
			{
				if (try_execute_cmd())
					break;
				install_snapshot_request request;
				request.term_ = get_current_term_();
				request.leader_id_ = raft_id_;
				request.last_included_term_ = head.last_included_term_;
				request.last_snapshot_index_ = head.last_included_index_;
				request.offset_ = file.tellg();
				std::cout << "request.offset_: " << request.offset_ << std::endl;
				request.data_.resize(1024*1024);
				file.read((char*)request.data_.data(), request.data_.size());
				request.data_.resize(file.gcount());
				request.done_ = file.eof();
				try
				{

					auto resp = rpc_client_.call(endpoint_, RPC::install_snapshot,  request);
					if (resp.term_ > request.term_)
					{
						new_term_callback_(resp.term_);
						return;
					}
					else if (resp.bytes_stored_ != 
							request.offset_ + request.data_.size())
					{
						file.clear(file.goodbit);
						file.seekg(resp.bytes_stored_, std::ios::beg);
					}
					else if (request.done_)
					{
						std::cout << "send snapshot done " << std::endl;
						match_index_ = head.last_included_index_;
						next_index_ = match_index_ + 1;
						break;
					}
				}
				catch (const std::exception& e)
				{
					throw e;
				}

			} while (!stop_);
		}
		int64_t next_heartbeat_delay()
		{
			auto delay = high_resolution_clock::now() - last_heart_beat_;
			return std::abs(heatbeat_inteval_ - duration_cast<milliseconds>(delay).count());
		}
		bool try_execute_cmd()
		{
			if (!cmd_queue_.pop(cmd_))
				return false;
			switch (cmd_)
			{
			case cmd_t::e_connect:
				do_connect();
				break;
			case cmd_t::e_sleep:
				do_sleep();
				break;
			case cmd_t::e_election:
				do_election();
				break;
			case cmd_t::e_append_entries:
				do_append_entries();
				break;
			case cmd_t::e_exit:
				do_exist();
			default:
				//todo log error
				break;
			}
			return true;
		}
		void do_sleep(int64_t milliseconds = 0)
		{
			std::unique_lock<std::mutex> lock(mtx_);
			if(!milliseconds)
				cv_.wait(lock, [this] { 
				return get_last_log_index_() != match_index_; });
			else {
				cv_.wait_for(lock, std::chrono::milliseconds(milliseconds), 
					[this] { return get_last_log_index_() != match_index_; });
			}
		}
		
		void update_heartbeat_time()
		{
			last_heart_beat_ = high_resolution_clock::now();
			send_heartbeat_ = true;
		}
		void do_connect()
		{
			endpoint_ = timax::rpc::get_tcp_endpoint(myself_.ip_,
				boost::lexical_cast<uint16_t>(myself_.port_));
		}
		void do_election()
		{
			int faileds = 3;
			if (faileds == 0)
				return;
			auto req = build_vote_request_();
			try
			{
				auto resp = rpc_client_.call(endpoint_, RPC::vote_request,req);
				vote_response_callback_(resp);
			}
			catch (const std::exception& e)
			{
				std::cout << e.what() << std::endl;
			}
		}

		void do_exist()
		{
			stop_ = true;
		}
		std::int64_t heatbeat_inteval_ = 1000;
		using sync_client = timax::rpc::sync_client<timax::rpc::msgpack_codec>;

		boost::asio::ip::tcp::endpoint endpoint_;
		sync_client rpc_client_;
		high_resolution_clock::time_point last_heart_beat_;
		bool stop_ = false;
		std::mutex mtx_;
		std::condition_variable cv_;

		utils::lock_queue<cmd_t> cmd_queue_;

		//ratf info
		int64_t match_index_ = 0;
		int64_t next_index_ = 0;

		bool send_heartbeat_ = false;
		cmd_t cmd_;
		std::thread peer_thread_;
	};
}
}
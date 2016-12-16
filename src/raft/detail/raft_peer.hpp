#pragma once	
#include <rest_rpc/client.hpp>
namespace xraft
{
namespace detail
{
	using namespace std::chrono;
	namespace rpc_client
	{
		TIMAX_DEFINE_PROTOCOL(rpc_append_entries_request, append_entries_response(append_entries_request));
		TIMAX_DEFINE_PROTOCOL(rpc_vote_request, vote_response (vote_request));
		TIMAX_DEFINE_PROTOCOL(rpc_install_snapshot, install_snapshot_response(install_snapshot_request));
	}
	class raft_peer
	{
		
	public:
		using async_client_t = timax::rpc::async_client<timax::rpc::msgpack_codec>;

		enum class cmd_t
		{
			e_connect,
			e_election,
			e_append_entries,
			e_sleep,
			e_exit,

		};
		raft_peer()
			:peer_thread_([this] { run(); })
		{
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
		void interrupt()
		{
			std::lock_guard<std::mutex> loker(cannel_rpc_mtx_);
			if(cannel_rpc_)
				cannel_rpc_();
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
		raft_config::raft_node myself_;
		std::int64_t heatbeat_inteval_;
		std::string leader_id_;
	private:
		
		void run()
		{
			do
			{
				if (!try_execute_cmd())
				{
					do_sleep();
				}
			} while (stop_);
		}
		bool do_install_snapshot()
		{
			std::string filepath = get_snapshot_path_();
			if (filepath.empty())
				return false;
			snapshot_header header;
			snapshot_reader reader;
			reader.open(filepath);
			check_apply(reader.read_sanpshot_head(header));
			std::ifstream &file = reader.get_snapshot_stream();
			file.seekg(0, std::ios::beg);

			const int buffer_length = 102400;
			std::unique_ptr<char[]> buffer;
			buffer.reset(new char[buffer_length]);
			install_snapshot_request request;
			do
			{
				request.term_ = get_current_term_();
				request.offset_ = file.tellg();
				request.last_included_term_ = header.last_included_term_;
				request.last_snapshot_index_ = header.last_included_index_;
				request.leader_id_ = leader_id_;

				file.read(buffer.get(), buffer_length);
				request.data_.clear();
				request.data_.append(buffer.get(), file.gcount());
				request.done_ = file.eof();
				try
				{
					auto rpc_task = rpc_client_.call(endpoint_, rpc_client::rpc_install_snapshot, request);
					save_rpc_task(rpc_task);
					auto response = rpc_task.get();

					if (response.term_ > request.term_)
					{
						new_term_callback_(response.term_);
						return false;
					}
					else if (response.bytes_stored_ == 0)
					{
						std::cout << "response.bytes_stored_  == 0" << std::endl;
						return false;
					}
				}
				catch (const std::exception& e)
				{
					std::cout << e.what() << std::endl;
					return false;
				}
			} while (!request.done_);
			return true;
		}
		void do_append_entries()
		{
			next_index_ = 0;
			match_index_ = 0;
			bool send_heartbeat = false;
			do
			{
				try
				{
					if (try_execute_cmd())
						break;
					int64_t index = get_last_log_index_();
					if (index == match_index_ )
					{
						send_heartbeat = true;
						do_sleep(next_heartbeat_delay());
					}
					if (!next_index_)
						next_index_ = index;
					auto request = build_append_entries_request_(next_index_);
					if (request.entries_.empty() && send_heartbeat == false)
					{
						do_install_snapshot();
						continue;
					}
					auto response = send_append_entries_request(request);
					update_heartbeat_time();
					if (!response.success_)
					{
						if (get_current_term_() < response.term_)
						{
							new_term_callback_(response.term_);
							return;
						}
						--next_index_;
						continue;
					}
					std::vector<int64_t> indexs;
					indexs.reserve(request.entries_.size());
					for (auto &itr : request.entries_)
						indexs.push_back(itr.index_);
					append_entries_success_callback_(indexs);
					match_index_ = response.last_log_index_;
					next_index_ = match_index_ + 1;
				}
				catch (std::exception &e)
				{
					std::cout << e.what() << std::endl;
					break;
				}
			} while (true);
		}
		append_entries_response 
			send_append_entries_request(const append_entries_request &req ,int timeout = 10000)
		{
			auto result = rpc_client_.call(endpoint_, rpc_client::rpc_append_entries_request, req)
				.timeout(std::chrono::milliseconds(timeout));
			save_rpc_task(result);
			return std::move(result.get());
		}
		int64_t next_heartbeat_delay()
		{
			auto delay = duration_cast<milliseconds>(high_resolution_clock::now() - last_heart_beat_).count();
			if (heatbeat_inteval_ > delay)
				return heatbeat_inteval_ - delay;
			return 0;
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
		void do_sleep(int64_t milliseconds = INT64_MAX)
		{
			std::unique_lock<std::mutex> lock(mtx_);
			if(milliseconds > 0)
				cv_.wait_for(lock, std::chrono::milliseconds(milliseconds));
		}
		
		void update_heartbeat_time()
		{
			last_heart_beat_ = high_resolution_clock::now();
		}
		void do_connect()
		{
			endpoint_ = timax::rpc::get_tcp_endpoint(myself_.ip_, boost::lexical_cast<uint16_t>(myself_.port_));
			connect_callback_(*this, true);
		}
		void do_election()
		{
			auto request = build_vote_request_();
			try
			{
				auto result = rpc_client_.call(endpoint_, rpc_client::rpc_vote_request, request);
				save_rpc_task(result);
				vote_response_callback_(result.get());
			}
			catch (const std::exception& e)
			{
				std::cout << e.what() << std::endl;
			}
		}
		template<typename T>
		void save_rpc_task(T const& result)
		{
			std::lock_guard<std::mutex> loker(cannel_rpc_mtx_);
			cannel_rpc_ = [r = result]() mutable { r.cancel(); };
		}
		
		void do_exist()
		{
			stop_ = true;
		}

		high_resolution_clock::time_point last_heart_beat_;
		bool stop_ = false;
		std::thread peer_thread_;
		std::mutex mtx_;
		std::condition_variable cv_;

		utils::lock_queue<cmd_t> cmd_queue_;

		//ratf info
		int64_t match_index_ = 0;
		int64_t next_index_ = 0;

		cmd_t cmd_;

		boost::asio::ip::tcp::endpoint endpoint_;
		std::mutex cannel_rpc_mtx_;
		async_client_t rpc_client_;
		std::function<void()> cannel_rpc_;
	};
}
}
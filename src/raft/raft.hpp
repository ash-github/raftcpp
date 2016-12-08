#pragma once
#include <rest_rpc/server.hpp>
#include "detail/detail.hpp"
namespace xraft
{
	using namespace detail;
	class raft
	{
	public:
		using append_log_callback = std::function<void(bool, int64_t)>;
		using commit_entry_callback = std::function<void(std::string &&, int64_t)>;
		using install_snapshot_callback = std::function<void(std::ifstream &)>;
		using build_snapshot_callback = std::function<bool(const std::function<bool(const std::string &)>&, int64_t)>;
		using rpc_server_t = timax::rpc::server<timax::rpc::msgpack_codec>;
		enum state
		{
			e_follower,
			e_candidate,
			e_leader
		};
		raft()
		{

		}
		bool check_leader()
		{
			return state_ == state::e_leader;
		}
		void replicate(std::string &&data, append_log_callback&&callback)
		{
			do_relicate(std::move(data), std::move(callback));
		}
		void regist_commit_entry_callback(const commit_entry_callback &callback)
		{
			commit_entry_callback_ = callback;
		}
		void regist_build_snapshot_callback(const  build_snapshot_callback& callback)
		{
			build_snapshot_callback_ = callback;
		}
		void regist_install_snapshot_handle(const install_snapshot_callback &callback)
		{
			install_snapshot_callback_ = callback;
		}
		void init(raft_config config)
		{
			port_ = config.myself_.port_;
			raft_id_ = config.myself_.raft_id_;
			raft_config_mgr_.set(config.nodes_);
			filelog_base_path_ = config.raftlog_base_path_;
			metadata_base_path_ = config.metadata_base_path_;
			snapshot_base_path_ = config.snapshot_base_path_;
			append_log_timeout_ = config.append_log_timeout_;
			election_timeout_ = config.election_timeout_;
			state_ = e_follower;
			snapshot_builder_.regist_build_snapshot_callback(
							std::bind(&raft::make_snapshot_callback, this, 
								std::placeholders::_1, std::placeholders::_2));
			snapshot_builder_.regist_get_last_commit_index(
							std::bind(&raft::get_last_log_entry_index,this));
			snapshot_builder_.regist_get_log_entry_term_handle(
							std::bind(&raft::get_last_log_entry_term, this));
			snapshot_builder_.regist_get_log_start_index(
							std::bind(&raft::get_log_start_index, this));
			snapshot_builder_.set_snapshot_distance(1024);//for test

			init_pees();
			init_rpc_server();
		}
	private:
		void init_rpc_server()
		{
			rpc_server_.reset(new rpc_server_t(port_, 1));
			rpc_server_->register_handler("rpc_append_entries_request", timax::bind(&raft::handle_append_entries_request,this));
			rpc_server_->register_handler("rpc_vote_request", timax::bind(&raft::handle_vote_request, this));
			rpc_server_->register_handler("rpc_install_snapshot", timax::bind(&raft::handle_install_snapshot, this));
			rpc_server_->start();
		}
		void init_pees()
		{
			//using namespace std::placeholders;
			for (auto &itr: raft_config_mgr_.get_nodes())
			{
				pees_.emplace_back(new raft_peer);
				raft_peer &peer = *(pees_.back());
				peer.append_entries_success_callback_ = std::bind(&raft::append_entries_callback, this, std::placeholders::_1);
				peer.build_append_entries_request_ = std::bind(&raft::build_append_entries_request, this, std::placeholders::_1);
				peer.build_vote_request_ = std::bind(&raft::build_vote_request, this);
				peer.vote_response_callback_ = std::bind(&raft::handle_vote_response, this, std::placeholders::_1);
				peer.new_term_callback_ = std::bind(&raft::handle_new_term, this, std::placeholders::_1);
				peer.get_current_term_ = [this] { return current_term_.load(); };
				peer.get_last_log_index_ = std::bind(&raft::get_last_log_entry_index, this);
				peer.connect_callback_ = std::bind(&raft::peer_connect_callback, this, std::placeholders::_1, std::placeholders::_2);
				peer.myself_ = itr;
				peer.send_cmd(raft_peer::cmd_t::e_connect);
			}
		}
		void peer_connect_callback(raft_peer &peer, bool result)
		{
			auto result_str = result ? "connect success" : "connect failed";
			std::cout <<
				"IP:"<< 
				peer.myself_.ip_ << 
				" PORT:" << 
				peer.myself_.port_ << 
				"ID:" << 
				peer.myself_.raft_id_<< 
				result_str << 
				std::endl;
		}
		void do_relicate(std::string &&data, append_log_callback&&callback)
		{
			int64_t index;
			if (!log_.write(build_log_entry(std::move(data)), index))
			{
				append_log_callback handle;
				commiter_.push([handle = std::move(callback)] {
					handle(false, 0);
				});
			}
			insert_callback(index, set_timeout(index), std::move(callback));
			notify_peers();
		}
		append_entries_response  handle_append_entries_request(append_entries_request && request)
		{
			std::lock_guard<std::mutex> locker(mtx_);
			append_entries_response response;
			response.success_ = false;
			response.term_ = current_term_;
			if (request.term_ < current_term_)
				//todo log Warm
				return response;
			
			if (current_term_ < request.term_)
			{
				step_down(request.term_);
				response.term_ = current_term_;
			}

			if (leader_id_.empty())
				leader_id_ = request.leader_id_;
			else if (leader_id_ != request.leader_id_)
			{
				leader_id_ = request.leader_id_;
				//todo log Warm
			}
			set_election_timer();
			if (request.prev_log_term_ == get_last_log_entry_term() &&
				request.prev_log_index_ > get_last_log_entry_index())
				return response;

			if (request.prev_log_index_ > get_log_start_index() &&
				get_log_entry(request.prev_log_index_).term_ != request.prev_log_term_)
			{
				//todo log Warm
				return response;
			}
			response.success_ = true;
			auto check_log = true;
			for (auto &itr : request.entries_)
			{
				if (check_log)
				{
					if (itr.index_ < get_log_start_index())
						continue;
					if (itr.index_ <= get_last_log_entry_index())
					{
						if (get_log_entry(itr.index_).term_ == itr.term_)
							continue;
						assert(committed_index_ < itr.index_);
						log_.truncate_suffix(itr.index_);
						check_log = false;
					}
				}
				int64_t index;
				log_.write(std::move(itr), index);
			}
			response.last_log_index_ = get_last_log_entry_index();
			if (committed_index_ < request.leader_commit_)
			{
				auto leader_commit_ = request.leader_commit_;
				commiter_.push([leader_commit_,this] {
					auto entries = log_.get_log_entries(committed_index_ + 1, 
						leader_commit_ - committed_index_);
					for (auto &itr : entries)
					{
						assert(itr.index_ == committed_index_ + 1);
						commit_entry_callback_(std::move(itr.log_data_),itr.index_);
						++committed_index_;
					}
				});
			}
			set_election_timer();
			return response;
		}
		vote_response handle_vote_request(const vote_request &request)
		{
			vote_response response;
			auto is_ok = false;
			if (request.last_log_term_ > get_last_log_entry_term() ||
				(request.last_log_term_ == get_last_log_entry_term() &&
					request.last_log_index_ >= get_last_log_entry_index()))
				is_ok = true;
			//todo hold votes check

			if (request.term_ > current_term_)
			{
				step_down(request.term_);
			}

			if (request.term_ == current_term_) {
				if (is_ok && voted_for_.empty()) {
					step_down(current_term_);
					voted_for_ = request.candidate_;
				}
			}
			response.term_ = current_term_;
			response.vote_granted_ = (request.term_ == current_term_ && 
					voted_for_ == request.candidate_);
			response.log_ok_ = is_ok;
			return response;
		}
		install_snapshot_response handle_install_snapshot (install_snapshot_request &request)
		{
			install_snapshot_response response;
			response.term_ = current_term_;
			if (request.term_ < current_term_)
			{
				//todo LOG WARM
				return response;
			}

			if (request.term_ > current_term_)
			{
				//todo log Info
				response.term_ = request.term_;
			}
			step_down(request.term_);
			set_election_timer();
			if (leader_id_.empty())
			{
				leader_id_ = request.leader_id_;
				//todo log info
			}
			else if (leader_id_ != request.leader_id_)
			{
				//logo error;
			}

			if (!snapshot_writer_)
				open_snapshot_writer(request.last_snapshot_index_);
			response.bytes_stored_ = snapshot_writer_.get_bytes_writted();
			if (request.offset_ != snapshot_writer_.get_bytes_writted())
			{
				//todo LOG WRAM
				return response;
			}
			if (!snapshot_writer_.write(request.data_))
			{
				//todo LOG ERROR;
				return response;
			}
			response.bytes_stored_ = snapshot_writer_.get_bytes_writted();
			if (request.done_)
			{
				snapshot_writer_.close();
				if (request.last_snapshot_index_ < last_snapshot_index_)
				{
					//todo Warm
					snapshot_writer_.discard();
					return response;
				}
				load_snapshot();
			}
			return response;
		}
		void load_snapshot()
		{
			
			if (!snapshot_reader_.open(snapshot_writer_.get_snapshot_filepath()))
			{
				//todo process error;
			}
			commiter_.push([&] {
				snapshot_header head;
				if (!snapshot_reader_.read_sanpshot_head(head))
				{
					//todo process error;
				}
				install_snapshot_callback_(snapshot_reader_.get_snapshot_stream());;
				log_.truncate_suffix(head.last_included_index_);

				if (head.last_included_index_ > committed_index_)
					committed_index_ = head.last_included_index_;
			});
			
		}
		void open_snapshot_writer(int64_t index)
		{
			if(functors::fs::mkdir()(snapshot_path_))
				snapshot_writer_.open(snapshot_path_+std::to_string(index)+".SS");
			else
			{
				//todo process Error;
			}
		}

		void handle_new_term(int64_t new_term)
		{
			step_down(new_term);
		}
		void step_down(int64_t new_term)
		{
			if (current_term_ < new_term)
			{
				current_term_ = new_term;
				leader_id_.clear();
				voted_for_.clear();
				update_Log_metadata();
			}
			if (state_ == state::e_candidate)
			{
				vote_responses_.clear();
				interrupt_vote();
			}
			if (state_ == state::e_leader)
			{
				sleep_peer_threads();
			}
			state_ = state::e_follower;
			set_election_timer();
		}
		void set_election_timer()
		{
			cancel_election_timer();
			election_timer_id_ = timer_.set_timer(election_timeout_ ,[this] {
				std::lock_guard<std::mutex> lock(mtx_);
				for (auto &itr : pees_)
					itr->send_cmd(raft_peer::cmd_t::e_election);
			});
		}
		void interrupt_vote()
		{
			for (auto &itr : pees_)
				itr->interrupt();
		}
		void sleep_peer_threads()
		{
			for (auto &itr : pees_)
				itr->send_cmd(raft_peer::cmd_t::e_sleep);
		}
		void update_Log_metadata()
		{
			metadata_.set("leader_id", leader_id_);
		}
		struct append_log_callback_info
		{
			append_log_callback_info(int waits, int64_t timer_id, 
				append_log_callback && callback)
					:waits_(waits),
					timer_id_(timer_id),
					callback_(callback){}
			int64_t index_;
			int waits_;
			int64_t timer_id_;
			append_log_callback callback_;
		};
		void notify_peers()
		{
			for (auto &itr : pees_)
				itr->notify();
		}
		void insert_callback(int64_t index, int64_t timer_id, append_log_callback &&callback)
		{
			utils::lock_guard lock(mtx_);
			append_log_callbacks_.emplace(
				std::piecewise_construct, std::forward_as_tuple(index),
				std::forward_as_tuple(raft_config_mgr_.get_majority() - 1, timer_id, std::move(callback)));
		}
		int64_t set_timeout(int64_t index)
		{
			return timer_.set_timer(append_log_timeout_, [this, index] {
				utils::lock_guard lock(mtx_);
				auto itr = append_log_callbacks_.find(index);
				if (itr == append_log_callbacks_.end())
					return;
				append_log_callback func;
				commiter_.push([func = std::move(itr->second.callback_)]{ func(false, 0); });
				append_log_callbacks_.erase(itr);
			});
		}
		log_entry build_log_entry(std::string &&log, 
			log_entry::type type = log_entry::type::e_append_log)
		{
			log_entry entry;
			entry.term_ = current_term_;
			entry.log_data_ = std::move(log);
			entry.type_ = type;
			return std::move(entry);
		}
		append_entries_request build_append_entries_request(int64_t index)
		{
			append_entries_request request;
			request.entries_ = log_.get_log_entries(index - 1);
			request.leader_commit_ = committed_index_;
			request.leader_id_ = raft_id_;
			request.prev_log_index_ = request.entries_.size() ? (request.entries_.front().index_) : 0;
			request.prev_log_term_ = request.entries_.size() ? (request.entries_.front().term_) : 0;
			request.entries_.pop_front();
			return std::move(request);
		}
		void handle_vote_response(const vote_response &response)
		{
			if (state_ != e_candidate)
			{
				return;
			}
			if (response.term_ < current_term_)
			{
				return;
			}
			if (response.term_ > current_term_)
				step_down(response.term_);
			vote_responses_.push_back(response);
			int votes = 1;//mysql 
			for (auto &itr : vote_responses_)
				if (itr.vote_granted_) votes++;
			if (votes >= raft_config_mgr_.get_majority())
			{
				vote_responses_.clear();
				become_leader();
			}
		}
		void become_leader()
		{
			state_ = e_leader;
			cancel_election_timer();
			for (auto &itr : pees_)
				itr->send_cmd(raft_peer::cmd_t::e_append_entries);
		}
		void cancel_election_timer()
		{
			timer_.cancel(election_timer_id_);
		}
		vote_request build_vote_request()
		{
			vote_request request;
			request.candidate_ = raft_id_;
			request.term_ = current_term_;
			request.last_log_index_ = get_last_log_entry_index();
			request.last_log_term_ = get_last_log_entry_term();
			return request;
		}
		std::string get_snapshot_filepath()
		{
			auto files = functors::fs::ls_files()(snapshot_path_);
			if (files.empty())
				return	{};
			std::sort(files.begin(), files.end(),std::greater<std::string>());
			return files[0];
		}
		void append_entries_callback(const std::vector<int64_t> &indexs)
		{
			utils::lock_guard lock(mtx_);
			for (auto &itr : indexs)
			{
				auto item = append_log_callbacks_.find(itr);
				if (item == append_log_callbacks_.end())
					continue;
				item->second.waits_--;
				if (item->second.waits_ == 0)
				{
					timer_.cancel(item->second.timer_id_);
					committed_index_ = item->first;
					append_log_callback func;
					int64_t index_ = committed_index_;
					commiter_.push([func = std::move(item->second.callback_),this, index_]{ func(true, index_); });
					append_log_callbacks_.erase(item);
				}
			}
		}
		bool make_snapshot_callback(const std::function<bool(const std::string &)> &writer, int64_t index)
		{
			return build_snapshot_callback_(writer, index);
		}
		void make_snapshot_done_callback(int64_t index)
		{
			log_.truncate_prefix(index);
		}
		int64_t get_last_log_entry_term()
		{
			return log_.get_last_log_entry_term();
		}
		int64_t get_last_log_entry_index()
		{
			return log_.get_last_index();
		}
		int64_t get_log_start_index()
		{
			return log_.get_log_start_index();
		}
		log_entry get_log_entry(int64_t index)
		{
			log_entry entry;
			if (!log_.get_log_entry(index, entry))
			{
				//todo log error;
			}
			return std::move(entry);
		}
		std::atomic_int64_t last_snapshot_index_;
		std::atomic_int64_t last_snapshot_term_;
		std::atomic_int64_t current_term_;
		std::atomic_int64_t committed_index_;
		std::string raft_id_;
		std::string voted_for_;
		std::string leader_id_;
		int64_t election_timeout_ = 10000;
		int64_t election_timer_id_;

		commit_entry_callback commit_entry_callback_;

		snapshot_builder snapshot_builder_;
		snapshot_writer snapshot_writer_;
		snapshot_reader snapshot_reader_;
		build_snapshot_callback build_snapshot_callback_;
		install_snapshot_callback install_snapshot_callback_;

		raft_config_mgr raft_config_mgr_;
		int64_t append_log_timeout_ = 10000;//10 seconds;
		std::vector<std::unique_ptr<raft_peer>> pees_;
		state state_;
		std::mutex mtx_;
		std::map<int64_t, append_log_callback_info> append_log_callbacks_;
		detail::timer timer_;
		committer<> commiter_;

		metadata<> metadata_;
		std::string metadata_base_path_;
		std::string metadata_path_ = "data/metadata";

		detail::filelog log_;
		std::string filelog_path_ = "data/log";
		std::string filelog_base_path_;

		std::string snapshot_base_path_;
		std::string snapshot_path_ = "data/snapshot/";

		std::vector<vote_response>  vote_responses_;

		//rpc
		int port_;
		std::unique_ptr<rpc_server_t> rpc_server_;
	};
}
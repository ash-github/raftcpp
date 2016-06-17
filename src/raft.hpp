#pragma once

namespace timax { namespace consensus 
{ 
	// raft 是本地的raft状态
	class raft : copyassignless
	{
		using thread_ptr_t = std::unique_ptr<std::thread>;
		using peer_ptr_t = std::shared_ptr<peer>;

	public:
		raft()
			: time_out_bound_(milliseconds_t{ 150 })
			, role_(server_status::follower)
			, current_term_(0)
			, leader_id_(0)
			, vote_for_(0)
			, server_id_(0)
			, withhold_votes_until_(time_point_t::min())
			, election_timeout_(time_point_t::min())
			, exiting_()
			, election_timeout_thread_()
			, status_changed_()
			, mutex_()
			, gen_(time_out_bound_, time_out_bound_ * 2)
		{

		}

		// handler for remote protocol
		auto on_request_vote(uint64_t term, uint64_t candidate_id)
			-> TIMAX_MULTI_TYPE(uint64_t, uint64_t)
		{
			uint64_t vote_term = current_term_, vote_granted = static_cast<uint64_t>(false);

			if (withhold_votes_until_ > clock_t::now())
			{
				TIMAX_MULTI_RETURN(vote_term, vote_granted);
			}

			if (term > current_term_)
			{
				step_down(term);
			}

			if (term == current_term_)
			{
				step_down(current_term_);
				set_election_timer();
				vote_for_ = candidate_id;
			}

			vote_term = current_term_;
			vote_granted = static_cast<uint64_t>(
				term == current_term_ && vote_for_ == candidate_id);
			TIMAX_MULTI_RETURN(vote_term, vote_granted);
		}

		void election_timeout_main()
		{
			lock_t lock{ mutex_ };
			while (!exiting_.load())
			{
				lock_t lock{ mutex_ };
				if (clock_t::now() > election_timeout_)
				{
					start_new_election();
				}
				status_changed_.wait_until(lock, election_timeout_);
			}
		}

		void peer_thread_main(peer_ptr_t peer)
		{
			lock_t lock{ mutex_ };
			while (!exiting_.load())
			{
				auto now = clock_t::now();
				auto wait_until = peer->backoff_until();

				switch (role_)
				{
				case server_status::follower:
					wait_until = time_point_t::max();
					break;
				case server_status::candidate:
					if (!peer->request_vote_done())
						request_vote(lock, *peer);
					else
						wait_until = time_point_t::max();
					break;
				case server_status::leader:
					break;
				}
				status_changed_.wait_until(lock, wait_until);
			}
			status_changed_.notify_all();
		}

	protected:
		void start_new_election()
		{
			// Don`t have a configuration : go back to sleep
			// Print 

			++current_term_;
			role_ = server_status::candidate;
			vote_for_ = server_id_;
			set_election_timer();

			for (auto& remote_server : remote_servers_)
			{
				remote_server.second->begin_request_vote();
			}
			// interrupt all

			// if we're the only server, this election is already done
			if (remote_servers_.empty())
				become_leader();
		}

		void request_vote(lock_t& lock, peer& peer)
		{
			auto term = current_term_;
			auto server_id = server_id_;
			
			{
				unlock_guard<std::mutex> unock{ lock };
				std::tie(term, server_id_) =
					peer.request_vote(term, server_id);
			}
				
			if(term)
		}

		void step_down(uint64_t new_term)
		{
			assert(current_term_ <= new_term);
			if (current_term_ < new_term)
			{
				current_term_ = new_term;
				leader_id_ = 0;
				vote_for_ = 0;
				role_ = server_status::follower;
			}
			else
			{
				if (server_status::follower != role_)
				{
					role_ = server_status::follower;
				}
			}
			if (time_point_t::max() == election_timeout_)
			{
				set_election_timer();
			}
			if (time_point_t::max() == withhold_votes_until_)
			{
				withhold_votes_until_ = time_point_t::min();
			}

			// interapt all

			// If the leader disk thread is currently writing to disk, wait for it to
			// finish. We poll here because we don't want to release the lock (this
			// server would then believe its writes have been flushed when they
			// haven't).

			// If a recent append has been queued, empty it here. Do this after waiting
			// for leaderDiskThread to preserve FIFO ordering of Log::Sync objects.
			// Don't bother updating the localServer's lastSyncedIndex, since it
			// doesn't matter for non-leaders.
		}

		void become_leader()
		{
			assert(server_status::candidate == role_);
			role_ = server_status::leader;
			leader_id_ = server_id_;
			election_timeout_ = time_point_t::max();
			withhold_votes_until_ = time_point_t::max();
		}

		void set_election_timer(bool wait_forever = false)
		{
			auto timeout_until = wait_forever ?
				gen_.get<milliseconds_t>() + clock_t::now() :
				time_point_t::max();
			{
				lock_t lock{ mutex_ };
				election_timeout_ = timeout_until;
			}
			status_changed_.notify_all();
		}

	private:
		// constant
		const milliseconds_t				time_out_bound_;

		// data status
		server_status						role_;
		uint64_t							current_term_;
		uint64_t							leader_id_;
		uint64_t							vote_for_;
		uint64_t							server_id_;
		time_point_t						withhold_votes_until_;
		time_point_t						election_timeout_;

		// thread models
		std::atomic<bool>					exiting_;
		thread_ptr_t						election_timeout_thread_;

		mutable std::condition_variable		status_changed_;
		mutable std::mutex					mutex_;

		// functional
		uniform_duration_gen				gen_;

		// servers
		std::unordered_map<uint64_t, peer_ptr_t> remote_servers_;
	};

} }
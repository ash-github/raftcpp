#ifndef _PEER_H_
#define _PEER_H_

//#ifdef _MSC_VER
//#include "forward.hpp"
//#endif

namespace timax { namespace consensus
{
	// peer is the local status for remote servers
	class peer : std::enable_shared_from_this<peer>
			   , copyassignless
	{
		using thread_ptr_t = std::unique_ptr<std::thread>;
		using io_service_t = boost::asio::io_service;
		using client_ptr_t = std::unique_ptr<client_proxy>;
	public:
		peer(uint64_t server_id, raft& raft, io_service_t& io);
		void begin_request_vote() noexcept;

		bool request_vote_done() const noexcept;
		bool& request_vote_done() noexcept;

		bool have_vote() const noexcept;
		bool& have_vote() noexcept;
		
		time_point_t backoff_until() const;
		auto request_vote(uint64_t term, uint64_t candidate_id)
			->TIMAX_MULTI_TYPE(uint64_t, uint64_t);
		void begin_leadership();
		bool exiting();
		void stop();

	private:
		uint64_t const					server_id_;
		uint64_t						message_id_;
		std::string						addresses_;
		std::string						port_;
		bool							request_vote_done_;
		bool							have_vote_;
		std::atomic<bool>				exiting_;
		time_point_t					backoff_until_;
		thread_ptr_t					thread_;
		io_service_t&					io_;
		client_ptr_t					client_;
	};

	namespace protocol
	{
		TIMAX_DEFINE_PROTOCOL(request_vote, TIMAX_MULTI_TYPE(uint64_t, uint64_t)(uint64_t, uint64_t));
	}
} }

#endif
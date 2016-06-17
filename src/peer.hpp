#pragma once

namespace timax { namespace consensus 
{
	peer::peer(uint64_t server_id, raft& raft, io_service_t& io)
		: server_id_(server_id)
		, message_id_(0)
		, addresses_()
		, port_()
		, request_vote_done_(false)
		, accept_vote_(false)
		, exiting_(false)
		, backoff_until_()
		, thread_(std::make_unique<std::thread>(
			&raft::peer_thread_main, &raft, shared_from_this()))
		, io_(io)
		, client_()
	{
	}

	void peer::begin_request_vote()
	{
		request_vote_done_ = false;
		accept_vote_ = false;
	}

	bool peer::request_vote_done() const
	{
		return request_vote_done_;
	}

	time_point_t peer::backoff_until() const
	{
		return backoff_until_ > clock_t::now() ? 
			backoff_until_ : time_point_t::min();
	}

	auto peer::request_vote(uint64_t term, uint64_t candidate_id)
	{
		if (!client_)
		{
			auto client = std::make_unique<client_proxy>(io_);
			client->connect(addresses_, port_);
			client_.swap(client);
		}

		return client_->call(with_tag(protocol::request_vote, ++message_id_), term, candidate_id);
	}

	void peer::begin_leadership()
	{
		// currently dose nothing
	}

	bool peer::exiting()
	{
		return exiting_.load();
	}

	void peer::stop()
	{
		exiting_.store(true);
	}
} }
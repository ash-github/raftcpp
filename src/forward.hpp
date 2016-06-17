#pragma once
#include <cstdint>
#include <cstddef>
#include <string>
#include <atomic>
#include <thread>
#include <memory>
#include <chrono>
#include <mutex>
#include <condition_variable>
#include <unordered_map>

// rest_rpc
#include <server.hpp>
#include <client_proxy/client_base.hpp>

#include "copyassignless.hpp"
#include "random.hpp"

namespace timax
{
	using uint64_t = std::uint64_t;
	using int64_t = std::int64_t;
	using uint32_t = std::uint32_t;
	using int3_t2 = std::int32_t;

	template<typename Mutex>
	class unlock_guard : copyassignless
	{
	public:
		explicit unlock_guard(std::unique_lock<Mutex>& lock)
			: lock_(lock)
		{
			assert(lock_.owns_lock());
			lock_.unlock();
		}
		~unlock_guard()
		{
			lock_.lock();
		}
	private:
		std::unique_lock<Mutex>& lock_;
	};

}

namespace timax { namespace consensus 
{
	using clock_t = std::chrono::steady_clock;
	using time_point_t = clock_t::time_point;
	using duration_t = clock_t::duration;
	using lock_t = std::unique_lock<std::mutex>;

	enum class server_status
	{
		follower,
		candidate,
		leader,
	};

	struct raft_status
	{
		server_status	role;
		uint64_t		term;
		uint64_t		server_id;
		uint64_t		vote_for;
	};

	class server;
	class peer;
	class raft;
	class remote_servers;
} }

#define TIMAX_MULTI_TYPE(...) std::tuple<__VA_ARGS__>
#define TIMAX_MULTI_RETURN(...) return std::make_tuple(__VA_ARGS__)
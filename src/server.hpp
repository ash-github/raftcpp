#pragma  once

#include "forward.hpp"
#include "exception.hpp"

#include "peer.h"
#include "raft.hpp"
#include "peer.hpp"

namespace timax { namespace consensus
{
	class server
	{
		using raft_ptr_t = std::unique_ptr<raft>;

	public:

		void init()
		{
			// read from config
			short port = 0;
			size_t size = 0;
			size_t timeout = 0;
			
		}

	protected:

	private:
		raft_ptr_t					raft_;
	};
} }
#pragma  once

namespace timax
{
	class copyassignless
	{
	public:
		copyassignless() = default;
		~copyassignless() = default;
		copyassignless(copyassignless&&) = default;
		copyassignless& operator= (copyassignless&&) = default;
	protected:
		copyassignless(copyassignless const&) = delete;
		copyassignless& operator= (copyassignless const&) = delete;
	};
}
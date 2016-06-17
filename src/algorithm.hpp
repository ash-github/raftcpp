#pragma once

#include <algorithm>

namespace timax
{
	template <typename Iterator, typename F>
	void for_each(Iterator begin, Iterator end, F unary_func)
	{
		std::for_each(begin, end, unary_func);
	}

	struct pair_second
	{
		template <typename Pair>
		auto operator(Pair& pair) const noexcept
			-> typename Pair::second_type&
		{
			return pair.second;
		}
	};

	static const pair_second get_pair_second{};

	struct pair_const_second
	{
		template <typename Pair>
		auto operator(Pair const& pair) const noexcept
			-> typename Pair::second_type const&
		{
			return pair.second;
		}
	};

	static const pair_const_second get_pair_const_second{};

	template <typename Iterator, typename Func, typename Filter>
	void for_each(Iterator begin, Iterator end, Func unary_func, Filter filter)
	{
		//std::for_each(begin, end, [unary_func, filter](auto ) {  })
	}
}
#pragma once
#include <random>

using seq_list_type = timax::db::sequence_list<int const*>;

void test_put_in_order()
{
	int a = 0;
	int const* ptr = &a;
	seq_list_type list{ [](int const* ptr) {} };
	for (size_t loop = 1ul; loop < 2000ul; ++loop)
	{
		list.put_snapshot(loop, ptr);
		if (loop > seq_list_type::array_size * seq_list_type::max_array_count)
		{
			auto r = list.get_begin_log_index();
			if (r != seq_list_type::array_size  + 1)
			{
				std::cout << "test_put_in_order failed!" << std::endl;
			}
		}
	}

	std::cout << "test_put_in_order success." << std::endl;
}

void test_sequence_list()
{
	test_put_in_order();
}
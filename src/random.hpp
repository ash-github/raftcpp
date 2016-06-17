#pragma once

#include <random>
namespace timax
{
	using seconds_t = std::chrono::seconds;
	using nanoseconds_t = std::chrono::nanoseconds;
	using hours_t = std::chrono::hours;
	using minutes_t = std::chrono::minutes;
	using microseconds_t = std::chrono::microseconds;
	using milliseconds_t = std::chrono::milliseconds;

	class uniform_duration_gen
	{
	public:
		uniform_duration_gen(nanoseconds_t const& inf, nanoseconds_t const& sup)
			: device_()
			, generator_(device_())
			, distribution_(
				static_cast<uint64_t>(inf.count()), 
				static_cast<uint64_t>(sup.count()))
		{
		
		}

		uniform_duration_gen(microseconds_t const& inf, microseconds_t const& sup)
			: uniform_duration_gen(
				std::chrono::duration_cast<nanoseconds_t>(inf),
				std::chrono::duration_cast<nanoseconds_t>(sup))
		{
		}

		uniform_duration_gen(milliseconds_t const& inf, milliseconds_t const& sup)
			: uniform_duration_gen(
				std::chrono::duration_cast<nanoseconds_t>(inf),
				std::chrono::duration_cast<nanoseconds_t>(sup))
		{
		}

		uniform_duration_gen(minutes_t const& inf, minutes_t const& sup)
			: uniform_duration_gen(
				std::chrono::duration_cast<nanoseconds_t>(inf),
				std::chrono::duration_cast<nanoseconds_t>(sup))
		{
		}

		uniform_duration_gen(seconds_t const& inf, seconds_t const& sup)
			: uniform_duration_gen(
				std::chrono::duration_cast<nanoseconds_t>(inf),
				std::chrono::duration_cast<nanoseconds_t>(sup))
		{
		}

		uniform_duration_gen(hours_t const& inf, hours_t const& sup)
			: uniform_duration_gen(
				std::chrono::duration_cast<nanoseconds_t>(inf),
				std::chrono::duration_cast<nanoseconds_t>(sup))
		{
		}

		template <typename Duration>
		Duration get()
		{
			using rep_t = typename Duration::rep;
			auto count = static_cast<rep_t>(distribution_(generator_));
			return Duration{ count };
		}

	private:
		std::random_device							device_;
		std::mt19937_64								generator_;
		std::uniform_int_distribution<uint64_t>		distribution_;
	};
}
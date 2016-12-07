#pragma once

#include <string>

namespace timax { namespace db
{
	class log_serializer
	{
	public:

	};

	class snapshot_serializer
	{
	public:
		static void pack(std::string const& key, std::string const& value, std::string& buffer)
		{
			auto size_key = static_cast<uint32_t>(key.size());
			auto size_value = static_cast<uint32_t>(value.size());

			auto size = size_key + size_value + 2 * sizeof(uint32_t);
			buffer.resize(size);

			auto work_ptr = &buffer[0];

			std::memcpy(work_ptr, &size_key, sizeof(uint32_t));
			work_ptr += sizeof(uint32_t);

			std::memcpy(work_ptr, key.data(), size_key);
			work_ptr += size_key;

			std::memcpy(work_ptr, &size_value, sizeof(uint32_t));
			work_ptr += sizeof(uint32_t);

			std::memcpy(work_ptr, value.data(), size_value);
		}

		static void unpack(std::string const buffer, std::string& key, std::string& value)
		{
			uint32_t size_key, size_value;

			auto work_ptr = buffer.data();

			std::memcpy(&size_key, work_ptr, sizeof(uint32_t));
			work_ptr += sizeof(uint32_t);

			key.resize(size_key);
			std::memcpy(&key[0], work_ptr, size_key);
			work_ptr += size_key;

			std::memcpy(&size_value, work_ptr, sizeof(uint32_t));
			work_ptr += sizeof(uint32_t);

			value.resize(size_value);
			std::memcpy(&value[0], work_ptr, size_value);
		}
	};
} }
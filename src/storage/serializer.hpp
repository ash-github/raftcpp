#pragma once

#include <string>

namespace timax { namespace db
{
	enum class log_op : int32_t
	{
		Write,
		Delete
	};

	struct db_operation
	{
		int op_type;
		std::string key;
		std::string value;

		META(op_type, key, value)
	};

	struct log_serializer
	{
		class string_buffer
		{
		public:
			explicit string_buffer(std::string& buffer)
				: buffer_(buffer)
				, offset_(0)
			{
				buffer_.clear();
			}

			string_buffer(string_buffer const&) = default;
			string_buffer(string_buffer &&) = default;
			string_buffer& operator= (string_buffer const&) = default;
			string_buffer& operator= (string_buffer &&) = default;

			void write(char const* data, size_t length)
			{
				if (buffer_.size() - offset_ < length)
					buffer_.resize(length + offset_);

				std::memcpy(&buffer_[0] + offset_, data, length);
				offset_ += length;
			}

			std::string release() const noexcept
			{
				return std::move(buffer_);
			}

		private:
			std::string&			buffer_;
			size_t				offset_;
		};

		static void pack_write(std::string& buffer, std::string const& key, std::string const& value)
		{
			auto tuple = std::make_tuple(static_cast<int>(log_op::Write), key, value);
			string_buffer sb{ buffer };
			msgpack::pack(sb, tuple);
		}

		static void pack_delete(std::string& buffer, std::string const& key)
		{
			using namespace std::string_literals;
			auto tuple = std::make_tuple(static_cast<int>(log_op::Delete), key, ""s);
			string_buffer sb{ buffer };
			msgpack::pack(sb, tuple);
		}

		static auto unpack(std::string const& buffer)
		{
			try
			{
				msgpack::unpacked msg;
				msgpack::unpack(&msg, buffer.data(), buffer.size());
				return msg.get().as<db_operation>();
			}
			catch (...)
			{
				throw std::runtime_error{ "Serialization error." };
			}
		}

	};

	struct snapshot_serializer
	{
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

		static void unpack(std::string const& buffer, std::string& key, std::string& value)
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

		static bool unpack(std::ifstream& in_stream, std::string& key, std::string& value)
		{
			if (in_stream.eof())
				return false;

			uint32_t size;
			in_stream >> size;
			if (0 == size)
				return false;

			key.resize(size);
			in_stream.read(&key[0], size);

			in_stream >> size;
			if (0 == size)
				return false;

			value.resize(size);
			in_stream.read(&value[0], size);
			return true;
		}
	};
} }
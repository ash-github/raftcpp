#pragma once
namespace xraft
{
namespace detail
{
	struct lock_free 
	{
		void lock() {};
		void unlock() {};
	};

	template<typename mutex = std::mutex>
	class metadata
	{
		enum type:char
		{
			e_string,
			e_integral
		};
		enum op:char
		{
			e_set,
			e_del,
		};
	public:
		metadata()
		{

		}
		~metadata()
		{
			max_log_file_ = 0;
			make_snapshot();
		}
		void clear()
		{
			std::lock_guard<mutex> lock(mtx_);
			string_map_.clear();
			integral_map_.clear();
			log_.close();
			std::vector<std::string> files = functors::fs::ls_files()(path_);
			if (files.empty())
				return;
			for (auto &itr: files)
			{
				functors::fs::rm()(itr);
			}
		}
		bool init(const std::string &path)
		{
			if (!functors::fs::mkdir()(path))
				return false;
			path_ = path;
			return load();
		}
		bool set(const std::string &key, const std::string &value)
		{
			std::lock_guard<mutex> lock(mtx_);
			if (!write_log(build_log(key, value, op::e_set)))
				return false;
			string_map_[key] = value;
			return true;
		}
		bool set(const std::string &key, int64_t value)
		{
			std::lock_guard<mutex> lock(mtx_);
			if (!write_log(build_log(key, value, op::e_set)))
				return false;
			integral_map_[key] = value;
			return true;
		}
		bool del(const std::string &key)
		{
			std::lock_guard<mutex> lock(mtx_);
			if (!write_log(build_log(key, op::e_del, type::e_string)))
				return false;
			if (!write_log(build_log(key, op::e_del, type::e_integral)))
				return false;
			auto itr = string_map_.find(key);
			if (itr != string_map_.end())
				string_map_.erase(itr);
			auto itr2 = integral_map_.find(key);
			if (itr2 != integral_map_.end())
				integral_map_.erase(itr2);
			return true;
		}
		bool get(const std::string &key, std::string &value)
		{
			std::lock_guard<mutex> lock(mtx_);
			auto itr = string_map_.find(key);
			if (itr == string_map_.end())
				return false;
			value = itr->second;
			return true;
		}
		bool get(const std::string &key, int64_t &value)
		{
			std::lock_guard<mutex> lock(mtx_);
			auto itr = integral_map_.find(key);
			if (itr == integral_map_.end())
				return false;
			value = itr->second;
			return true;
		}
		bool write_snapshot(const std::function<bool(const std::string &)>& writer)
		{
			std::lock_guard<mutex> lock(mtx_);
			for (auto &itr : string_map_)
			{
				std::string log = build_log(itr.first, itr.second, op::e_set);
				uint8_t buf[sizeof(uint32_t)];
				uint8_t *ptr = buf;
				endec::put_uint32(ptr, (uint32_t)log.size());
				if (!writer(std::string((char*)buf, sizeof(buf))))
					return false;
				if (!writer(log))
					return false;
			}
			return true;
		}
		void load_snapshot(std::ifstream &file)
		{
			load_fstream(file);
		}
	private:
		std::string build_log(const std::string &key, const std::string &value,op _op)
		{
			std::string buffer;
			buffer.resize(endec::get_sizeof(key) +
				endec::get_sizeof(value) +
				endec::get_sizeof(typename std::underlying_type<type>::type()) +
				endec::get_sizeof(typename std::underlying_type<op>::type()));
			unsigned char* ptr = (unsigned char*)buffer.data();
			endec::put_uint8(ptr, static_cast<uint8_t>(_op));
			endec::put_uint8(ptr, static_cast<uint8_t>(type::e_string));
			endec::put_string(ptr, key);
			endec::put_string(ptr, value);
			return std::move(buffer);
		}
		std::string build_log(const std::string &key, int64_t &value, op _op)
		{
			std::string buffer;
			buffer.resize(endec::get_sizeof(key) +
				endec::get_sizeof(value) +
				endec::get_sizeof(typename std::underlying_type<type>::type()) +
				endec::get_sizeof(typename std::underlying_type<op>::type()));

			unsigned char* ptr = (unsigned char*)buffer.data();
			endec::put_uint8(ptr, static_cast<uint8_t>(_op));
			endec::put_uint8(ptr, static_cast<uint8_t>(type::e_integral));
			endec::put_string(ptr, key);
			endec::put_uint64(ptr, (uint64_t)value);
			return std::move(buffer);
		}
		std::string build_log(const std::string &key, op _op, type _type)
		{
			std::string buffer;
			buffer.resize(endec::get_sizeof(key) +
				endec::get_sizeof(typename std::underlying_type<type>::type()) +
				endec::get_sizeof(typename std::underlying_type<op>::type()));

			unsigned char* ptr = (unsigned char*)buffer.data();
			endec::put_uint8(ptr, static_cast<uint8_t>(_op));
			endec::put_uint8(ptr, static_cast<uint8_t>(_type));
			endec::put_string(ptr, key);
			return std::move(buffer);
		}
		bool write_log(const std::string &data)
		{
			char buffer[sizeof(uint32_t)];
			uint8_t *ptr = (uint8_t *)buffer;
			endec::put_uint32(ptr, (uint32_t)data.size());
			log_.write((char*)(buffer), sizeof (buffer));
			log_.write(data.data(), data.size());
			log_.flush();
			return log_.good() && try_make_snapshot();

		}
		bool load()
		{
			std::vector<std::string> files = functors::fs::ls_files()(path_);
			if (files.empty())
			{
				return reopen_log();
			}
			std::sort(files.begin(), files.end(),std::greater<std::string>());
			for (auto &file: files)
			{
				auto end = file.find(".metadata");
				if (end == std::string::npos)
					continue;
				std::size_t beg = file.find_last_of('/') + 1;
				if (beg == std::string::npos)
					beg = file.find_last_of('\\') + 1;
				std::string index = file.substr(beg, end - beg);
				index_ = std::strtoull(index.c_str(), 0, 10);
				if (errno == ERANGE || index_ == 0)
				{
					//todo log error
					assert(false);
				}
				if (!load_file(get_snapshot_file()) || !load_file(get_log_file()))
					return false;
				return reopen_log(false);
			}
			return false;
		}
		template<typename T>
		bool write(std::ofstream &file, T &map)
		{
			for (auto &itr : map)
			{
				std::string log = build_log(itr.first, itr.second, op::e_set);
				uint8_t buf[sizeof(uint32_t)];
				uint8_t *ptr = buf;
				endec::put_uint32(ptr, (uint32_t)log.size());
				file.write((char*)buf,(int)sizeof(buf));
				file.write(log.data(), log.size());
				if (!file.good())
					return false;
			}
			return true;
		}
		bool load_fstream(std::istream &file)
		{
			std::string buffer;
			uint8_t len_buf[sizeof(uint32_t)];
			do
			{
				file.read((char*)len_buf, sizeof(len_buf));
				if (file.eof())
					return true;
				auto *ptr = len_buf;
				auto len = endec::get_uint32(ptr);
				buffer.resize(len);
				file.read((char*)buffer.data(), len);
				if (!file.good())
				{
					//log error
					return false;
				}
				ptr = (uint8_t*)buffer.data();
				char _op = (char)endec::get_uint8(ptr);
				char _t = (char)endec::get_uint8(ptr);
				if (type::e_string == static_cast<type>(_t))
				{
					if (static_cast<op>(_op) == op::e_set)
					{
						auto key = endec::get_string(ptr);
						auto value = endec::get_string(ptr);
						string_map_[key] = value;
					}
					else if (static_cast<op>(_op) == op::e_del)
					{
						auto key = endec::get_string(ptr);
						auto itr = string_map_.find(key);
						if (itr != string_map_.end())
							string_map_.erase(itr);
					}
				}
				else if (type::e_integral == static_cast<type>(_t))
				{
					if (static_cast<op>(_op) == op::e_set)
					{
						auto key = endec::get_string(ptr);
						auto value = endec::get_uint64(ptr);
						integral_map_[key] = value;
					}
					else if (static_cast<op>(_op) == op::e_set)
					{
						auto key = endec::get_string(ptr);
						auto itr = integral_map_.find(key);
						if (itr != integral_map_.end())
							integral_map_.erase(itr);
					}
				}
			} while (true);
		}
		bool load_file(const std::string &filepath)
		{
			std::ifstream file;
			int mode = std::ios::binary | std::ios::in;
			file.open(filepath.c_str(), mode);
			if (!file.good())
			{
				//process error
				return true;
			}
			auto ret = load_fstream(file);
			file.close();
			return ret;
		}
		bool try_make_snapshot()
		{
			if ((int)max_log_file_ > log_.tellp())
				return true;
			return make_snapshot();
		}
		bool make_snapshot()
		{
			std::ofstream file;
			int mode = std::ios::binary |
				std::ios::trunc |
				std::ios::out;
			++index_;
			file.open(get_snapshot_file().c_str(), mode);
			if (!file.good())
			{
				//process error
				return false;
			}
			if (!write(file, string_map_))
			{
				//process error
				return false;
			}
			if (!write(file, integral_map_))
			{
				//process error
				return false;
			}
			file.flush();
			file.close();
			if (!reopen_log())
			{
				return false;
			}
			if (!touch_metadata_file())
			{
				return false;
			}
			if (!rm_old_files())
			{
				return false;
			}
			return true;
		}
		bool reopen_log(bool trunc = true)
		{
			log_.close();
			int mode = std::ios::binary | std::ios::out ;
			if (trunc)
				mode |= std::ios::trunc;
			else
				mode |= std::ios::app;

			log_.open(get_log_file().c_str(), mode);
			touch_metadata_file();
			return log_.good();
		}
		bool touch_metadata_file()
		{
			std::ofstream file;
			file.open(get_metadata_file().c_str());
			auto is_ok = file.good();
			file.close();
			return is_ok;
		}
		bool rm_old_files()
		{
			if (!functors::fs::rm()(get_old_log_file()))
			{
				//todo log error
				return false;
			}
			if (!functors::fs::rm()(get_old_snapshot_file()))
			{
				//todo log error
				return false;
			}
			if (!functors::fs::rm()(get_old_metadata_file()))
			{
				//todo log error
				return false;
			}
			return true;
		}
		std::string get_snapshot_file()
		{
			return path_ + std::to_string(index_)+ ".data";
		}
		std::string get_log_file()
		{
			return path_ + std::to_string(index_) + ".Log";
		}
		std::string get_metadata_file()
		{
			return path_ + std::to_string(index_) + ".metadata";
		}
		std::string get_old_snapshot_file()
		{
			return path_ + std::to_string(index_ - 1) + ".data";
		}
		std::string get_old_log_file()
		{
			return path_ + std::to_string(index_ - 1) + ".Log";
		}
		std::string get_old_metadata_file()
		{
			return path_ + std::to_string(index_ - 1) + ".metadata";
		}
		uint64_t index_ = 1;
		std::size_t max_log_file_ = 10 * 1024 * 1024;
		std::ofstream log_;
		std::string path_;
		mutex mtx_;
		std::map<std::string, std::string> string_map_;
		std::map<std::string, int64_t> integral_map_;
	};
}
}
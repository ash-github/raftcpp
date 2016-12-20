#pragma once
namespace xraft
{
namespace detail
{
	class file
	{
	public:
		file()
		{

		}
		~file()
		{
			//stop filelog remove this file immediately .
			//when someone is  writing or reading.
			std::lock_guard<std::mutex> lock(mtx_);
		}
		void operator = (file &&f)
		{
			std::lock_guard<std::mutex> lock(mtx_);
			std::lock_guard<std::mutex> lock2(f.mtx_);
			if (&f == this)
				return;
			move_reset(std::move(f));
		}
		file(file &&self)
		{
			std::lock_guard<std::mutex> lock2(self.mtx_);
			move_reset(std::move(self));
		}
		bool open(const std::string &filepath)
		{
			std::lock_guard<std::mutex> lock(mtx_);
			filepath_ = filepath;
			last_log_index_ = -1;
			log_index_start_ = -1;
			return open_no_lock();
		}

		bool write(int64_t index, std::string &&data)
		{
			std::lock_guard<std::mutex> lock(mtx_);

			check_apply(data_file_.good());
			check_apply(index_file_.good());
			int64_t file_pos = data_file_.tellp();
			uint32_t len = (uint32_t)data.size();
			data_file_.write(reinterpret_cast<char*>(&len), sizeof len);
			data_file_.write(data.data(), data.size());
			data_file_.sync();
			check_apply(data_file_.good());
			index_file_.write(reinterpret_cast<char*>(&index), sizeof(index));
			index_file_.write(reinterpret_cast<char*>(&file_pos), sizeof(file_pos));
			index_file_.sync();
			last_log_index_ = index;
			check_apply(index_file_.good());
			return true;
		}

		bool get_log_entries(int64_t &index,
			std::size_t &count,
			std::list<log_entry> &log_entries,
			std::unique_lock<std::mutex> &lock)
		{
			std::lock_guard<std::mutex> lock_guard(mtx_);
			lock.unlock();
			int64_t data_file_offset = 0;
			check_apply(get_data_file_offset(index, data_file_offset));
			data_file_.seekg(data_file_offset, std::ios::beg);
			check_apply(data_file_.good());
			do
			{
				uint32_t len;
				data_file_.read((char*)&len, sizeof(uint32_t));
				if (!data_file_.good())
				{
					bool ret = data_file_.eof();
					data_file_.clear(data_file_.goodbit);
					data_file_.seekp(0, std::ios::end);
					return ret;
				}
				std::string buffer;
				buffer.resize(len);
				data_file_.read((char*)buffer.data(), len);
				check_apply(data_file_.good());
				log_entry entry;
				entry.from_string(buffer);
				log_entries.emplace_back(std::move(entry));
				++index;
				--count;
			} while (count > 0);
			data_file_.seekp(0, std::ios::end);
			return true;
		}

		bool get_entry(int64_t index, log_entry &entry)
		{
			std::lock_guard<std::mutex> lock_guard(mtx_);
			int64_t data_file_offset = 0;
			check_apply(get_data_file_offset(index, data_file_offset));
			data_file_.seekg(data_file_offset, std::ios::beg);
			check_apply(data_file_.good());
			uint32_t len;
			data_file_.read((char*)&len, sizeof(uint32_t));
			check_apply(data_file_.good());
			std::string buffer;
			buffer.resize(len);
			data_file_.read((char*)buffer.data(), len);
			check_apply(data_file_.good());
			entry.from_string(buffer);
			data_file_.seekp(0, std::ios::end);
			return true;
		}
		std::size_t size()
		{
			data_file_.seekp(0, std::ios::end);
			return data_file_.tellp();
		}
		//rm file from disk.
		bool rm()
		{
			std::lock_guard<std::mutex> lock_guard(mtx_);
			return rm_on_lock();
		}
		bool truncate_suffix(int64_t index)
		{
			std::lock_guard<std::mutex> lock_guard(mtx_);
			int64_t offset = 0;
			check_apply(get_data_file_offset(index, offset));
			data_file_.close();
			index_file_.close();
			if (!functors::fs::truncate_suffix()(get_data_file_path(), offset))
			{
				//todo log error ;
				open_no_lock();
				return false;
			}
			offset = get_index_file_offset(index);
			last_log_index_ = -1;
			if (!functors::fs::truncate_suffix()(get_index_file_path(), offset))
			{
				//todo log error ;
				open_no_lock();
				return false;
			}
			return open_no_lock();
		}
		bool truncate_prefix(int64_t index)
		{
			std::lock_guard<std::mutex> lock_guard(mtx_);
			int64_t offset = 0;
			if (!get_data_file_offset(index + 1, offset))
				return false;
			data_file_.close();
			index_file_.close();
			if (!functors::fs::truncate_prefix()(get_data_file_path(), offset))
			{
				return false;
			}
			offset = get_index_file_offset(index + 1);
			auto tmp = log_index_start_;
			log_index_start_ = -1;
			if (!functors::fs::truncate_prefix()(get_index_file_path(), offset))
			{
				//todo log error ;
				return false;
			}
			auto old_data_file = get_data_file_path();
			auto old_index_file = get_index_file_path();
			auto beg = filepath_.find_last_of('/') + 1;
			filepath_ = filepath_.substr(0, beg);
			filepath_ += std::to_string(index + 1);
			filepath_ += ".log";
			functors::fs::rename()(old_data_file, get_data_file_path());
			functors::fs::rename()(old_index_file, get_index_file_path());
			return open_no_lock();
		}
		int64_t get_last_log_index()
		{
			if (last_log_index_ == -1)
			{
				std::lock_guard<std::mutex> lock(mtx_);
				index_file_.seekg(-(int32_t)(sizeof(int64_t) * 2), std::ios::end);
				index_file_.read((char*)&last_log_index_, sizeof(last_log_index_));
				index_file_.seekp(0, std::ios::end);
				if (index_file_.gcount() != sizeof(last_log_index_) || !index_file_.good())
				{
					index_file_.clear(index_file_.goodbit);
					last_log_index_ = -1;
					return 0;
				}
			}
			return last_log_index_;
		}
		int64_t get_log_start()
		{
			std::lock_guard<std::mutex> lock(mtx_);
			return get_log_start_no_lock();
		}
		bool is_open()
		{
			std::lock_guard<std::mutex> lock(mtx_);
			return data_file_.is_open() && index_file_.is_open();
		}
	private:
		void move_reset(file &&self)
		{
			data_file_ = std::move(self.data_file_);
			index_file_ = std::move(self.index_file_);
			filepath_ = std::move(self.filepath_);
			last_log_index_ = self.last_log_index_;
			log_index_start_ = self.log_index_start_;
			self.last_log_index_ = -1;
			self.log_index_start_ = -1;
		}
		std::string get_data_file_path()
		{
			return filepath_;
		}
		std::string get_index_file_path()
		{
			auto filepath = filepath_;
			filepath.pop_back();
			filepath.pop_back();
			filepath.pop_back();
			filepath.pop_back();
			return filepath + ".index";
		}
		int64_t get_index_file_offset(int64_t index)
		{
			auto diff = index - get_log_start_no_lock();
			return diff * sizeof(int64_t) * 2;;
		}
		bool get_data_file_offset(int64_t index, int64_t &data_file_offset)
		{
			data_file_offset = 0;
			auto offset = get_index_file_offset(index);
			if (offset == 0)
				return true;
			index_file_.seekg(offset, std::ios::beg);
			int64_t index_buffer_;
			index_file_.read((char*)&index_buffer_, sizeof(int64_t));
			if (!index_file_.good())
			{
				index_file_.clear(index_file_.goodbit);
				return false;
			}
			if (index_buffer_ != index)
				//todo log error.
				return false;
			index_file_.read((char*)&data_file_offset, sizeof(int64_t));
			if (!index_file_.good())
				return false;
			index_file_.seekp(0, std::ios::end);
			return true;
		}
		bool rm_on_lock()
		{
			data_file_.close();
			index_file_.close();
			if (!functors::fs::rm()(get_data_file_path()) ||
				!functors::fs::rm()(get_index_file_path()))
				return false;
			return true;
		}
		int64_t get_log_start_no_lock()
		{
			if (log_index_start_ == -1)
			{
				index_file_.seekg(0, std::ios::beg);
				char buffer[sizeof(int64_t)] = { 0 };
				index_file_.read(buffer, sizeof(buffer));
				index_file_.seekp(0, std::ios::end);
				if (index_file_.gcount() != sizeof(buffer))
				{
					index_file_.clear(index_file_.goodbit);
					return 0;
				}
				log_index_start_ = *(int64_t*)(buffer);
			}
			return log_index_start_;
		}
		bool open_no_lock()
		{
			int mode = std::ios::out |
				std::ios::in |
				std::ios::binary |
				std::ios::app |
				std::ios::ate;
			if (data_file_.is_open())
				data_file_.close();
			if (index_file_.is_open())
				index_file_.close();
			data_file_.open(get_data_file_path().c_str(), mode);
			index_file_.open(get_index_file_path().c_str(), mode);
			if (!data_file_.good() || !index_file_.good())
				return false;
			return true;
		}

		std::mutex mtx_;
		int64_t last_log_index_ = -1;
		int64_t log_index_start_ = -1;
		std::fstream data_file_;
		std::fstream index_file_;
		std::string filepath_;
	};

	class filelog
	{
	public:
		filelog()
		{
		}
		bool init(const std::string &path)
		{
			path_ = path;
			check_apply(functors::fs::mkdir()(path_));
			auto files = functors::fs::ls_files()(path_);
			if (files.empty())
				return true;
			for (auto &itr : files)
			{
				if (itr.find(".log") != std::string::npos)
				{
					file f;
					check_apply(f.open(itr));
					logfiles_.emplace(f.get_log_start(), std::move(f));
				}
			}
			current_file_ = std::move(logfiles_.rbegin()->second);
			logfiles_.erase(logfiles_.find(current_file_.get_log_start()));
			last_index_ = current_file_.get_last_log_index();
			return true;
		}

		bool write(detail::log_entry &&entry, int64_t &index)
		{
			std::lock_guard<std::mutex> lock(mtx_);
			if (entry.index_)
			{
				last_index_ = entry.index_;
			}
			else
			{
				++last_index_;
				index = last_index_;
				entry.index_ = last_index_;
			}
			std::string buffer = entry.to_string();
			log_entries_cache_size_ += buffer.size();
			log_entries_cache_.emplace_back(std::move(entry));
			check_log_entries_size();
			if (!current_file_.is_open())
			{
				current_file_.open(path_ + std::to_string(entry.index_) + ".log");
			}
			check_apply(current_file_.write(last_index_, std::move(buffer)));
			check_current_file_size();
			return true;
		}
		bool get_log_entry(int64_t index, log_entry &entry)
		{
			std::lock_guard<std::mutex> lock(mtx_);
			if (get_entry_from_cache(entry, index))
				return true;
			if (current_file_.is_open() && 
				current_file_.get_log_start() <= index &&
				index <= current_file_.get_last_log_index())
			{
				return current_file_.get_entry(index, entry);
			}
			for (auto itr = logfiles_.begin(); itr != logfiles_.end(); ++itr)
			{
				auto &f = itr->second;
				if (f.get_log_start() <= index &&
					index <= f.get_last_log_index())
					return f.get_entry(index, entry);
			}
			return true;
		}
		std::list<log_entry> get_log_entries(int64_t index, std::size_t count = 10)
		{
			std::unique_lock<std::mutex> lock(mtx_);
			std::list<log_entry> log_entries;
			get_entries_from_cache(log_entries, index, count);
			if (count == 0)
				return std::move(log_entries);
			for (auto itr = logfiles_.begin(); itr != logfiles_.end(); itr++)
			{
				file &f = itr->second;
				get_entries_from_cache(log_entries, index, count);
				if (count == 0)
					return std::move(log_entries);
				if (f.get_log_start() <= index && index <= f.get_last_log_index())
				{
					if (!f.get_log_entries(index, count, log_entries, lock))
						return std::move(log_entries);
					if (count == 0)
						return std::move(log_entries);
					lock.lock();
				}
			}
			get_entries_from_cache(log_entries, index, count);
			if (count == 0)
				return std::move(log_entries);
			if (current_file_.is_open() && 
				index <= current_file_.get_last_log_index() &&
				current_file_.get_log_start() <= index)
			{
				if (!current_file_.get_log_entries(index, count, log_entries, lock))
					return std::move(log_entries);
				lock.lock();
			}
			return std::move(log_entries);
		}
		void truncate_prefix(int64_t index)
		{
			std::lock_guard<std::mutex> lock(mtx_);
			for (auto itr = log_entries_cache_.begin(); itr != log_entries_cache_.end();)
			{
				if (itr->index_ <= index)
					itr = log_entries_cache_.erase(itr);
				else
					++itr;
			}
			for (auto itr = logfiles_.begin(); itr != logfiles_.end(); )
			{
				if (itr->second.get_last_log_index() <= index)
				{
					itr->second.rm();
					itr = logfiles_.erase(itr);
					continue;
				}
				else if (itr->second.get_log_start() <= index &&
					index < itr->second.get_last_log_index())
				{
					itr->second.truncate_prefix(index);
					auto f = std::move(itr->second);
					logfiles_.erase(itr);
					logfiles_.emplace(f.get_log_start(), std::move(f));
					break;
				}
				++itr;
			}
			if (current_file_.is_open() && index == current_file_.get_last_log_index())
			{
				current_file_.rm();
			}
			else if ( current_file_.is_open () && 
						current_file_.get_log_start() <= index &&
						index < current_file_.get_last_log_index())
			{
				current_file_.truncate_prefix(index);
			}
		}
		void truncate_suffix(int64_t index)
		{
			std::lock_guard<std::mutex> lock(mtx_);
			for (auto itr = log_entries_cache_.begin(); itr != log_entries_cache_.end();)
			{
				if (index <= itr->index_)
					itr = log_entries_cache_.erase(itr);
				else
					++itr;
			}
			for (auto itr = logfiles_.begin(); itr != logfiles_.end(); ++itr)
			{
				if (index <= itr->second.get_last_log_index() &&
					itr->second.get_log_start() <= index)
				{
					itr->second.truncate_suffix(index);
					current_file_.rm();
					last_index_ = index - 1;
					if (last_index_ < 0)
						last_index_ = 0;
					if (itr->second.get_last_log_index() > 0)
					{
						current_file_ = std::move(itr->second);
						last_index_ = current_file_.get_last_log_index();
						logfiles_.erase(itr);
						break;
					}
					
				}
			}
			for (auto itr = logfiles_.upper_bound(index); itr != logfiles_.end();)
			{
				itr->second.rm();
				itr = logfiles_.erase(itr);
			}
		}
		int64_t get_last_log_entry_term()
		{
			std::lock_guard<std::mutex> lock(mtx_);
			if (log_entries_cache_.size())
				return log_entries_cache_.back().term_;
			return 0;
		}
		int64_t get_last_index()
		{
			std::lock_guard<std::mutex> lock(mtx_);
			return last_index_;
		}
		int64_t get_log_start_index()
		{
			std::lock_guard<std::mutex> lock(mtx_);
			if (logfiles_.size())
				return logfiles_.begin()->second.get_log_start();
			else if(current_file_.is_open())
				return current_file_.get_log_start();
			return 0;
		}
		void set_make_snapshot_trigger(std::function<void()> callback)
		{
			std::lock_guard<std::mutex> lock(mtx_);
			make_snapshot_trigger_ = callback;
		}
	private:
		bool get_entries_from_cache(std::list<log_entry> &log_entries,
			int64_t &index, std::size_t &count)
		{
			if (log_entries_cache_.size() && log_entries_cache_.front().index_ <= index)
			{
				for (auto &itr : log_entries_cache_)
				{
					if (index == itr.index_)
					{
						log_entries.push_back(itr);
						++index;
						count--;
						if (count == 0)
							return true;
					}
				}
				return true;
			}
			return false;
		}
		bool get_entry_from_cache(log_entry &entry, int64_t index)
		{
			if (log_entries_cache_.size() &&
				log_entries_cache_.front().index_ <= index)
			{
				for (auto &itr : log_entries_cache_)
				{
					if (index == itr.index_)
					{
						entry = itr;
						return true;
					}
				}
			}
			return false;
		}
		void check_log_entries_size()
		{
			while (log_entries_cache_.size() &&
				log_entries_cache_size_ > max_cache_size_)
			{
				log_entries_cache_size_ -= log_entries_cache_.front().bytes();
				log_entries_cache_.pop_front();
			}
		}
		bool check_current_file_size()
		{
			if (current_file_.size() > max_file_size_)
			{
				logfiles_.emplace(current_file_.get_log_start(),
					std::move(current_file_));
				std::string filepath = path_ + std::to_string(last_index_ + 1) + ".log";
				check_apply(current_file_.open(filepath));
				check_make_snapshot_trigger();
			}
			return true;
		}

		void check_make_snapshot_trigger()
		{
			if (logfiles_.size() > max_log_file_count_ 
				&& make_snapshot_trigger_)
			{
				make_snapshot_trigger_();
				make_snapshot_trigger_ = nullptr;
			}
		}
		std::mutex mtx_;
		std::list<log_entry> log_entries_cache_;
		std::size_t log_entries_cache_size_ = 0;
		std::size_t max_cache_size_ = 1;
		std::size_t max_file_size_ = 1024;
		int64_t last_index_ = 0;
		std::string path_;
		file current_file_;
		int64_t current_file_last_index_ = 0;
		std::map<int64_t, detail::file> logfiles_;
		std::size_t max_log_file_count_ = 5;
		std::function<void()> make_snapshot_trigger_;
	};
}

}
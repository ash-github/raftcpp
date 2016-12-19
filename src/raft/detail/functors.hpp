#pragma once
namespace xraft
{
namespace functors
{
namespace fs
{
#ifdef _MSC_VER
	struct mkdir
	{
		bool operator()(const std::string &dir)
		{
			std::size_t offset = 0;
			do {

				auto pos = dir.find_first_of('\\', offset);
				if (pos == std::string::npos)
					pos = dir.find_first_of('/',offset);
				if (pos == std::string::npos)
				{
					return !!CreateDirectory(dir.c_str(), NULL) || ERROR_ALREADY_EXISTS == GetLastError();
				}
				else {
					auto child = dir.substr(0, pos);
					offset = pos+1;
					if (!!CreateDirectory(child.c_str(), NULL) || ERROR_ALREADY_EXISTS == GetLastError())
						continue;
					return false;
				}
			} while (true);
			
		}
	};
	struct ls_files
	{
		std::vector<std::string> operator()(const std::string &dir)
		{
			std::vector<std::string> files;
			WIN32_FIND_DATA find_data;
			HANDLE handle = ::FindFirstFile((dir+"*.*").c_str(), &find_data);
			if (INVALID_HANDLE_VALUE == handle)
				return {};
			while (TRUE)
			{
				if (find_data.dwFileAttributes & ~FILE_ATTRIBUTE_DIRECTORY)
				{
					files.emplace_back(std::string(dir) + find_data.cFileName);
				}
				if (!FindNextFile(handle, &find_data)) 
					break;
			}
			FindClose(handle);
			return files;
		}
	};

	struct rm 
	{
		bool operator()(const std::string &filepath)
		{
			return !!DeleteFile(filepath.c_str());
		}
	};

	struct truncate_suffix
	{
		bool operator()(const std::string &filepath_, int64_t offset)
		{
			HANDLE pHandle = CreateFile(filepath_.c_str(), 
				GENERIC_READ | GENERIC_WRITE, 
				FILE_SHARE_READ | FILE_SHARE_WRITE, 
				NULL, 
				OPEN_EXISTING, 
				FILE_ATTRIBUTE_NORMAL | FILE_FLAG_RANDOM_ACCESS, 
				NULL);
			if (pHandle == INVALID_HANDLE_VALUE)
			{
				std::cout << GetLastError() << std::endl;
				return false;
			}
			LONG HighOfft;
			DWORD dwNew;
			BOOL rc;
			HighOfft = (LONG)(offset >> 32);
			dwNew = SetFilePointer(pHandle, (LONG)offset, &HighOfft, FILE_BEGIN);
			if (dwNew == INVALID_SET_FILE_POINTER) {
				return false;
			}
			rc = SetEndOfFile(pHandle);
			if (rc) 
			{
				return true;
			}
			std::cout << GetLastError() << std::endl;
			return false;
		}
	};
#endif
	struct rename
	{
		bool operator()(const std::string &old_file, const std::string &new_file)
		{
			return ::rename(old_file.c_str(), new_file.c_str()) == 0;
		}
	};
	struct truncate_prefix
	{
		bool operator()(const std::string &filepath, int64_t offset)
		{
			std::ofstream tmp_file;
			int mode = std::ios::out | 
				std::ios::trunc | 
				std::ios::binary;
			tmp_file.open((filepath + ".tmp").c_str(), mode);
			if (!tmp_file.good())
			{
				//todo log error;
				return false;
			}
			std::ifstream old_file;
			mode = std::ios::in;
			old_file.open(filepath.c_str(), mode);
			if (!old_file.good())
			{
				//todo log error;
				rm()((filepath + ".tmp"));
				return false;
			}
			old_file.seekg(offset, std::ios::beg);
			if (!tmp_file.good())
			{
				//todo log error;
				rm()((filepath + ".tmp"));
				return false;
			}
			const int buffer_len = 16 * 1024;
			char buffer[buffer_len];
			do
			{
				old_file.read(buffer, buffer_len);
				tmp_file.write(buffer, old_file.gcount());
				if (old_file.eof())
					break;
			} while (true);
			old_file.close();
			tmp_file.close();
			if (!rm()(filepath))
			{
				//todo log error;
				rm()((filepath + ".tmp"));
				return false;
			}
			return rename()((filepath + ".tmp"),filepath);
		}
	};
}
		
}
}
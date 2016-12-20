#pragma once

namespace timax { namespace db 
{
	template 
	< 
		typename StoragePolicy,
		template <typename> class ConsensusPolicy
	>
	class kv_storage
	{
	public:
		using storage_policy = StoragePolicy;
		using consensus_policy = ConsensusPolicy<storage_policy>;

		class const_operation
		{
			friend kv_storage;
		protected:
			const_operation(consensus_policy& consensus, std::string key)
				: consensus_(consensus)
				, key_(std::move(key))
			{}

		public:
			operator std::string() const
			{
				return consensus_.get(key_);
			}

		protected:
			consensus_policy&		consensus_;
			std::string			key_;
		};

		class mutable_operation : public const_operation
		{
			friend kv_storage;
		protected:
			mutable_operation(consensus_policy& consensus, std::string key)
				: const_operation(consensus, std::move(key))
			{}

		public:
			void operator= (std::string const& value)
			{
				this->consensus_.put(key_, value);
			}
		};

	public:
		explicit kv_storage(std::string const& db_path, std::string const& consensus_config_path)
			: storage_(db_path)
			, consensus_(storage_, consensus_config_path)
		{
		}

		mutable_operation operator[] (std::string key)
		{
			return { consensus_, std::move(key) };
		}

		const_operation operator[] (std::string key) const
		{
			return { consensus_, std::move(key) };
		}

		void del(std::string const& key)
		{
			consensus_.del(key);
		}

	private:
		storage_policy		storage_;
		consensus_policy		consensus_;
	};
} }
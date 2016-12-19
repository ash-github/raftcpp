#include <rest_rpc/client.hpp>

namespace kvclient
{
	TIMAX_DEFINE_PROTOCOL(put, void(std::string const&, std::string const&));
	TIMAX_DEFINE_PROTOCOL(get, std::string(std::string const&));
	TIMAX_DEFINE_PROTOCOL(del, void(std::string const&));
}

int process(int argc, char* argv[])
{
	using namespace std::chrono_literals;
	using client_type = timax::rpc::async_client<timax::rpc::msgpack_codec>;

	client_type client;
	std::string op = argv[1];

	if ("put" == op)
	{
		if (argc < 6)
			return -1;

		std::string key = argv[2];
		std::string value = argv[3];
		std::string address = argv[4];
		uint16_t port = boost::lexical_cast<uint16_t>(argv[5]);

		auto endpoint = timax::rpc::get_tcp_endpoint(address, port);
		auto task = client.call(endpoint, kvclient::put, key, value);
		task.wait(2s);
	}
	else if ("get" == op)
	{
		if (argc < 5)
			return -1;

		std::string key = argv[2];
		std::string address = argv[3];
		uint16_t port = boost::lexical_cast<uint16_t>(argv[4]);

		auto endpoint = timax::rpc::get_tcp_endpoint(address, port);
		auto task = client.call(endpoint, kvclient::get, key);
		auto value = task.get(2s);
		std::cout << "Get key(" << key << ") Value(" << value << ").\n";
	}
	else
	{
		if (argc < 5)
			return -1;

		std::string key = argv[2];
		std::string address = argv[3];
		uint16_t port = boost::lexical_cast<uint16_t>(argv[4]);

		auto endpoint = timax::rpc::get_tcp_endpoint(address, port);
		auto task = client.call(endpoint, kvclient::del, key);
		task.wait(2s);
	}

	return 0;
}

int main(int argc, char* argv[])
{
	if (argc < 4)
		return -1;

	try
	{
		return process(argc, argv);
	}
	catch (timax::rpc::exception const& e)
	{
		std::cout << e.get_error_message() << std::endl;
		return -2;
	}
}
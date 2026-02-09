#include <boost/beast.hpp>
#include <boost/beast/ssl.hpp>
#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <iostream>
#include <string>

auto streamFromWebsocket() -> void {
    auto host = std::string{"stream.binance.com"};
    
    auto path = std::string{"/ws/btcusdt@trade"};
    auto port = std::string{"9443"};

    auto ioContext = boost::asio::io_context{};
    auto sslContext = boost::asio::ssl::context{
        boost::asio::ssl::context::tlsv12_client
    };

    auto resolver = boost::asio::ip::tcp::resolver{ioContext};

    auto webSocket = boost::beast::websocket::stream<
        boost::asio::ssl::stream<
            boost::asio::ip::tcp::socket
        >
    >{ioContext, sslContext};

    auto const result = resolver.resolve(host, port);

    boost::asio::connect(
        boost::beast::get_lowest_layer(webSocket), 
        result
    );

    if (!SSL_set_tlsext_host_name(webSocket.next_layer().native_handle(), host.c_str())) {
        std::cout << "error in the hostname stuff?\n";
    }

    webSocket.next_layer().handshake(boost::asio::ssl::stream_base::client);
    webSocket.handshake(host, path);

    auto streamBuffer = boost::beast::flat_buffer{};

    auto count = int{0};
    while(webSocket.read(streamBuffer)) {
    
        std::cout << boost::beast::make_printable(streamBuffer.data()) << '\n';
        streamBuffer.consume(streamBuffer.size());
        count++;
        std::cout << count << '\n';
    }

    webSocket.close(boost::beast::websocket::close_code::normal);
}

int main() {
    streamFromWebsocket();
}
#pragma once

#include <string>
#include <thread>
#include <iostream>
#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/beast.hpp>
#include <boost/beast/ssl.hpp>

#include "core/eventQueue.hpp"
#include "external/simdjson/simdjson.h"

namespace MarketData {

template<typename Event>
class WebSocketConnection {
private:
    boost::asio::io_context ioContext_;
    EventQueue<Event>& eventQueue_;

    std::string host_;
    std::string path_;
    std::string port_;

    std::thread readThread_;
    bool stopThread_;

public:
    WebSocketConnection(
        EventQueue<Event>& container,
        std::string host, 
        std::string path, 
        std::string port
    ) 
        : eventQueue_ {container}
        , host_ {std::move(host)}
        , path_ {std::move(path)}
        , port_ {std::move(port)}
        , stopThread_ {false}
    {   
        readThread_ = std::thread{&WebSocketConnection::readFromWebsocket, this};
    }

    auto stopThread() -> void {
        stopThread_ = true;
        readThread_.join();
    }
private:
    auto connectWebsocket(
        boost::asio::io_context& ioContext,
        std::string host, 
        std::string path, 
        std::string port
    ) -> boost::beast::websocket::stream<boost::asio::ssl::stream<boost::asio::ip::tcp::socket>> {
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

        return webSocket;
    }

    auto readFromWebsocket() -> void {
        auto ioContext = boost::asio::io_context{};
        auto webSocket = connectWebsocket(ioContext, host_, path_, port_);

        auto jsonParser = simdjson::ondemand::parser{};
        auto streamBuffer = boost::beast::flat_buffer{};
        auto doc = simdjson::ondemand::document{};
        while (true) {
            webSocket.read(streamBuffer);

            if (streamBuffer.capacity() < streamBuffer.size() + simdjson::SIMDJSON_PADDING) {
                streamBuffer.reserve(streamBuffer.size() + simdjson::SIMDJSON_PADDING);
            }

            auto currEventDataPtr = static_cast<char const*>(streamBuffer.data().data());

            auto errors = jsonParser.iterate(
                simdjson::padded_string_view(currEventDataPtr, streamBuffer.size(), streamBuffer.capacity())
            ).get(doc);

            auto event = Event{doc};

            eventQueue_.pushAndNotify(event);

            streamBuffer.consume(streamBuffer.size());

            if (stopThread_ == true) {
                break;
            }
        }
        try {
            webSocket.close(boost::beast::websocket::close_code::normal);
        } catch (const boost::system::system_error& err) {
            if (err.code() != boost::asio::ssl::error::stream_truncated) {
                std::cerr << "unexpected error on closing the websocket" << std::endl;
            }
        }
    }

};

}
#pragma once

#include <boost/beast/core/flat_buffer.hpp>

namespace MarketData {

struct RawEvent {
    boost::beast::flat_buffer data_;

    RawEvent() = default;

    RawEvent(boost::beast::flat_buffer&& data)
        : data_ {std::move(data)}
    {}
};

}
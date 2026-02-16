#pragma once

#include <chrono>
#include <boost/beast/core/flat_buffer.hpp>

#include "core/timer.hpp"

namespace MarketData {

struct RawEvent {
    boost::beast::flat_buffer data_;
    Timer lifeTime_;

    RawEvent() = default;

    RawEvent(boost::beast::flat_buffer&& data)
        : data_ {std::move(data)}
        , lifeTime_ {}
    {}
};

}
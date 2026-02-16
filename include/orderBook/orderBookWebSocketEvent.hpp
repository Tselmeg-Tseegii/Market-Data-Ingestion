#pragma once

#include <memory>
#include "core/rawEvent.hpp"
#include "external/simdjson/simdjson.h"

namespace MarketData {

struct OrderBookWebSocketEvent: RawEvent {
    std::unique_ptr<simdjson::ondemand::parser> parser_;
    simdjson::ondemand::document doc_;
    bool parserIsActive_;

    long long int firstUpdateId_ {-1};
    long long int lastUpdateId_ {-1};

    OrderBookWebSocketEvent() = default;

    OrderBookWebSocketEvent(boost::beast::flat_buffer&& data);

    auto initialiseParser() -> void;

    auto getFirstUpdateId() -> long long int;

    auto getLastUpdateId() -> long long int;
};

}
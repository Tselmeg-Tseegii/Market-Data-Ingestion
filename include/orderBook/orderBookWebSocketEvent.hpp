#pragma once

#include <vector>
#include "external/simdjson/simdjson.h"
#include "shared/marketDataTypes.hpp"

namespace MarketData {

struct OrderBookWebSocketEvent {
    long long int firstUpdateId_ {-1};
    long long int lastUpdateId_ {-1};

    std::vector<IntPriceVolume> asks_;
    std::vector<IntPriceVolume> bids_;

    OrderBookWebSocketEvent() = default;

    OrderBookWebSocketEvent(simdjson::ondemand::document& doc);
};

}
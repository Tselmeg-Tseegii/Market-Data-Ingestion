#pragma once

#include <mutex>
#include <unordered_map>
#include <map>

#include "core/marketDataTypes.hpp"
#include "external/simdjson/simdjson.h"
#include "tradeVolume/tradeVolumeWebSocketEvent.hpp"

namespace MarketData {

class TradeVolumeContainer {
private:
    std::mutex mtx_;
    std::map<int, int> tradeVolume_;
public:
    auto updateFromEvent(TradeVolumeWebSocketEvent& event) -> void;

    auto print() -> void;
};

}
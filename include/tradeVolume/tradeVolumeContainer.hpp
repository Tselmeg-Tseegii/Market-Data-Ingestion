#pragma once

#include <mutex>
#include <unordered_map>
#include <map>

#include "core/marketDataTypes.hpp"
#include "external/simdjson/simdjson.h"
#include "core/rawEvent.hpp"
#include "tradeVolume/tradeVolumeWebSocketEvent.hpp"

namespace MarketData {

class TradeVolumeContainer {
private:
    std::mutex mtx_;
    std::map<int, double> tradeVolume_;
public:
    auto updateFromEvent(TradeVolumeWebSocketEvent& event) -> void;

    auto print() -> void;
};

}
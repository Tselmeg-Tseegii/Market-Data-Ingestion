#pragma once

#include <mutex>
#include <unordered_map>

#include "core/marketDataTypes.hpp"
#include "external/simdjson/simdjson.h"
#include "core/rawEvent.hpp"

namespace MarketData {

class TradeVolumeContainer {
private:
    std::mutex mtx_;
    std::unordered_map<int, double> tradeVolume_;
public:
    auto updateFromEvent(RawEvent& event, simdjson::ondemand::parser& parser) -> void;

    auto print() -> void;
};

}
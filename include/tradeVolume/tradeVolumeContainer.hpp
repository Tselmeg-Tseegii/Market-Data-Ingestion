#pragma once

#include <mutex>

#include "core/marketDataTypes.hpp"
#include "external/simdjson/simdjson.h"

namespace MarketData {

class TradeVolumeContainer {
private:
    std::mutex mtx_;
    FlatContainer tradeVolume_;
public:
    auto updateFromEvent(simdjson::ondemand::document& parsedData) -> void;

    auto print() -> void;
};

}
#pragma once

#include <mutex>

#include "core/marketDataTypes.hpp"
#include "external/simdjson/simdjson.h"
#include "core/rawEvent.hpp"

namespace MarketData {

class TradeVolumeContainer {
private:
    std::mutex mtx_;
    FlatContainer tradeVolume_;
public:
    auto updateFromEvent(RawEvent& event, simdjson::ondemand::parser& parser) -> void;

    auto print() -> void;
};

}
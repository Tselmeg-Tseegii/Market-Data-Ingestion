#pragma once

#include <mutex>
#include <unordered_map>
#include <map>
#include <deque>

#include "shared/marketDataTypes.hpp"
#include "external/simdjson/simdjson.h"
#include "tradeVolume/tradeVolumeWebSocketEvent.hpp"

namespace MarketData {

class TradeVolumeContainer {
private:
    std::map<long long int, long long int> tradeVolumeMap_;
    std::deque<IntPriceVolume> tradeVolumeSeq_;

    std::mutex mtx_;
    std::condition_variable newDataAddedCv_;
    bool willGetMoreData_ {true};
public:
    auto updateFromEvent(TradeVolumeWebSocketEvent& event) -> void;

    auto print() -> void;

    auto getCondVar() -> std::condition_variable&;

    auto getMutex() -> std::mutex&;

    auto setWillNotGetMoreData() -> void;

    auto willGetNewData() -> bool;
};

}
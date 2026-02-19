#pragma once

#include <thread>

#include "tradeVolume/tradeVolumeContainer.hpp"
#include "core/eventQueue.hpp"

namespace MarketData {

class TradeVolumeUpdater {
private:
    TradeVolumeContainer& container_;
    EventQueue<TradeVolumeWebSocketEvent>& queue_;

    std::thread updateThread_;
    bool stopThread_;

public:
    TradeVolumeUpdater(TradeVolumeContainer& container, EventQueue<TradeVolumeWebSocketEvent>& queue);

    auto stopThread() -> void;

private:
    auto updateFromEventQueue() -> void;
    
};

}
#include "tradeVolume/tradeVolumeContainer.hpp"
#include "external/simdjson/simdjson.h"
#include "tradeVolume/tradeVolumeWebSocketEvent.hpp"
#include "core/timer.hpp"

namespace MarketData {

auto TradeVolumeContainer::updateFromEvent(TradeVolumeWebSocketEvent& event) -> void {
    tradeVolume_[event.intPrice_] += event.intVolume_;


    // auto timer = Timer{};
    // timer.printNow();
    // // event.lifeTime_.stop();
}

auto TradeVolumeContainer::print() -> void {
    // tradeVolume_.print();
}

}
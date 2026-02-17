#include "tradeVolume/tradeVolumeContainer.hpp"
#include "core/rawEvent.hpp"
#include "external/simdjson/simdjson.h"
#include "tradeVolume/tradeVolumeWebSocketEvent.hpp"

namespace MarketData {

auto TradeVolumeContainer::updateFromEvent(TradeVolumeWebSocketEvent& event) -> void {
    tradeVolume_[event.price_] += event.volume_;

    // event.lifeTime_.stop();
}

auto TradeVolumeContainer::print() -> void {
    // tradeVolume_.print();
}

}
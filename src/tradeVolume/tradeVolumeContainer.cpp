#include "tradeVolume/tradeVolumeContainer.hpp"
#include "external/simdjson/simdjson.h"
#include "tradeVolume/tradeVolumeWebSocketEvent.hpp"
#include "shared/timer.hpp"

namespace MarketData {

auto TradeVolumeContainer::updateFromEvent(TradeVolumeWebSocketEvent& event) -> void {
    this->tradeVolume_[event.intPrice] += event.intVolume;
}

auto TradeVolumeContainer::print() -> void {
    for (auto& elem : this->tradeVolume_) {
        std::cout << elem.first << " - " << elem.second << '\n';
    }
}

}
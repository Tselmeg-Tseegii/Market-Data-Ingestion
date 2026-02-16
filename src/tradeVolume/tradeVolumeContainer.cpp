#include "tradeVolume/tradeVolumeContainer.hpp"

namespace MarketData {
 
auto TradeVolumeContainer::updateFromEvent(simdjson::ondemand::document& parsedData) -> void {
    auto price = static_cast<int>(parsedData["p"].get_double_in_string().value() * 100);
    auto volume = parsedData["q"].get_double_in_string().value();
    
    tradeVolume_.insertOrUpdate(price, volume);
}

auto TradeVolumeContainer::print() -> void {
    tradeVolume_.print();
}

}
#include "tradeVolume/tradeVolumeWebSocketEvent.hpp"

namespace MarketData {

TradeVolumeWebSocketEvent::TradeVolumeWebSocketEvent(simdjson::ondemand::document& doc) {
    auto price = static_cast<int>(doc["p"].get_double_in_string().value() * 100);
    auto volume = doc["q"].get_double_in_string().value();
}

}
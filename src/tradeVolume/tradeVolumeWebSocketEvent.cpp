#include "tradeVolume/tradeVolumeWebSocketEvent.hpp"

namespace MarketData {

TradeVolumeWebSocketEvent::TradeVolumeWebSocketEvent(simdjson::ondemand::document& doc) {
    this->intPrice_ = static_cast<long long int>(std::round(doc["p"].get_double_in_string().value() * 100000000));
    this->intVolume_ = static_cast<long long int>(std::round(doc["q"].get_double_in_string().value() * 100000000));
}

}
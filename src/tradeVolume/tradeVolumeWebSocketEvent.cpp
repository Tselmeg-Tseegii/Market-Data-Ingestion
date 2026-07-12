#include "tradeVolume/tradeVolumeWebSocketEvent.hpp"

namespace MarketData {

TradeVolumeWebSocketEvent::TradeVolumeWebSocketEvent(simdjson::ondemand::document& doc, const Timer& timer) 
    : timer_ {timer}
{

    this->intPrice = static_cast<long long int>(std::round(doc["p"].get_double_in_string().value() * 100000000));
    this->intVolume = static_cast<long long int>(std::round(doc["q"].get_double_in_string().value() * 100000000));
}

}
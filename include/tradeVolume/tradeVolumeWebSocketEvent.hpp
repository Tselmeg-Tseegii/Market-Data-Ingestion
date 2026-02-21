#pragma once

#include "shared/marketDataTypes.hpp"
#include "external/simdjson/simdjson.h"
#include "shared/timer.hpp"

namespace MarketData {

class TradeVolumeWebSocketEvent: public IntPriceVolume {

public:

    TradeVolumeWebSocketEvent(simdjson::ondemand::document& doc);

};


}
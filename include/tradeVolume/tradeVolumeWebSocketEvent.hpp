#pragma once

#include "core/marketDataTypes.hpp"
#include "external/simdjson/simdjson.h"
#include "core/timer.hpp"

namespace MarketData {

class TradeVolumeWebSocketEvent: public IntPriceVolume {

public:

    TradeVolumeWebSocketEvent(simdjson::ondemand::document& doc);

};


}
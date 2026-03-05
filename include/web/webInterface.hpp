#pragma once

#include "orderBook/orderBookContainer.hpp"
#include "tradeVolume/tradeVolumeContainer.hpp"
#include "shared/marketDataTypes.hpp"

#include <nlohmann/json.hpp>
#include <chrono>

namespace Web {

// convert the top levels of an order book into a JSON object suitable for
// consumption by a simple web interface. prices/volumes are converted back to
// floating point values using the original scaling factor used elsewhere in
// the code (1e8). the returned object has keys "asks" and "bids"; each is an
// array of { price, volume } objects.

nlohmann::json orderBookToJson(
    MarketData::OrderBookContainer& book,
    std::size_t depth = 20
);

// return the most recent trades (up to |count|). the behaviour mirrors the
// internal TradeVolumeContainer FIFO; callers may then render the returned
// vector in a chart or table on the web page.

nlohmann::json recentTradesToJson(
    MarketData::TradeVolumeContainer& tv,
    std::size_t count = 100
);

} // namespace Web

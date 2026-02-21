#pragma once

#include <map>
#include <vector>
#include <mutex>
#include <iostream>

#include "core/marketDataTypes.hpp"
#include "orderBook/orderBookWebSocketEvent.hpp"
#include "external/nlohmann/json.hpp"

namespace MarketData {

class OrderBookContainer {
private:
    long long int lastUpdateId_ {-1};
    std::map<long long int, long long int> asks_;
    std::map<long long int, long long int> bids_;
    std::mutex mtx_;

public:
    auto getLastUpdateId() -> long long int;

    auto getMtx() -> std::mutex&;

    auto getOrderBook() -> std::pair<std::vector<IntPriceVolume>, std::vector<IntPriceVolume>>;

    auto print() -> void;

    auto clearOrderBook() -> void;

    auto setOrderBrookFromJson(nlohmann::json& data) -> void;

    auto updateOrderBookFromEvent(OrderBookWebSocketEvent& currEvent) -> void;
};

}
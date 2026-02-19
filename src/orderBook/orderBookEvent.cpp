#include "orderBook/orderBookWebSocketEvent.hpp"

namespace MarketData {

OrderBookWebSocketEvent::OrderBookWebSocketEvent(simdjson::ondemand::document& doc) {
    firstUpdateId_ = doc["U"].get<long long int>().value();

    for (auto elem : doc["b"]) {
        auto it = elem.begin();
        auto priceDouble = double{};
        auto res = (*it).get_double_in_string().get(priceDouble);

        auto priceInt = static_cast<int>(priceDouble * 100);

        ++it;

        auto volume = double{};
        res = (*it).get_double_in_string().get(volume);

        bids_.push_back({priceInt, volume});
    }

    for (auto elem : doc["a"]) {
        auto it = elem.begin();
        auto priceDouble = double{};
        auto res = (*it).get_double_in_string().get(priceDouble);

        auto priceInt = static_cast<int>(priceDouble * 100);

        ++it;

        auto volume = double{};
        res = (*it).get_double_in_string().get(volume);

        asks_.push_back({priceInt, volume});

    }

    lastUpdateId_ = doc["u"].get<long long int>().value();
}

}
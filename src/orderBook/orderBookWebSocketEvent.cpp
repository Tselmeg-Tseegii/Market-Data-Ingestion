#include "orderBook/orderBookWebSocketEvent.hpp"
#include <cmath>

namespace MarketData {

OrderBookWebSocketEvent::OrderBookWebSocketEvent(simdjson::ondemand::document& doc) {
    this->firstUpdateId_ = doc["U"].get<long long int>().value();

    for (auto elem : doc["b"]) {
        auto it = elem.begin();
        auto priceDouble = double{};
        auto res = (*it).get_double_in_string().get(priceDouble);

        auto priceInt = static_cast<long long int>(std::round(priceDouble * 100000000));

        ++it;

        auto volumeDouble = double{};
        res = (*it).get_double_in_string().get(volumeDouble);
        auto volumeInt = static_cast<long long int>(std::round(volumeDouble * 100000000));

        this->bids_.push_back({priceInt, volumeInt});
    }

    for (auto elem : doc["a"]) {
        auto it = elem.begin();
        auto priceDouble = double{};
        auto res = (*it).get_double_in_string().get(priceDouble);

        auto priceInt = static_cast<long long int>(std::round(priceDouble * 100000000));

        ++it;

        auto volumeDouble = double{};
        res = (*it).get_double_in_string().get(volumeDouble);
        auto volumeInt = static_cast<long long int>(std::round(volumeDouble * 100000000));

        this->asks_.push_back({priceInt, volumeInt});
    }

    this->lastUpdateId_ = doc["u"].get<long long int>().value();
}

}
#include "orderBook/orderBookContainer.hpp"
#include "core/marketDataTypes.hpp"
#include <cmath>
#include <iostream>

namespace MarketData {

auto OrderBookContainer::getLastUpdateId() -> long long int {
    auto lock = std::lock_guard{mtx_};
    return lastUpdateId_;
}
auto OrderBookContainer::getMtx() -> std::mutex& {
    return mtx_;
}

auto OrderBookContainer::getOrderBook() -> std::pair<std::vector<IntPriceVolume>, std::vector<IntPriceVolume>> {
    auto lock = std::lock_guard{mtx_};

    auto askBook = std::vector<IntPriceVolume>{};
    askBook.reserve(asks_.size());
    auto bidBook = std::vector<IntPriceVolume>{};
    bidBook.reserve(bids_.size());
    for (auto it {asks_.begin()}; it != asks_.end(); it++) {
        askBook.push_back({it->first, it->second});
    }

    for (auto it {bids_.begin()}; it != bids_.end(); it++) {
        bidBook.push_back({it->first, it->second});
    }
    
    return {askBook, bidBook};
}

auto OrderBookContainer::print() -> void {
    auto [ask, bid] = getOrderBook();
    std::cout << "OrderBook\n";

    std::cout << "Asks\n";
    for (auto& [price, vol] : ask) {
        std::cout << price << " - " << vol << '\n';
    }

    std::cout << "Bids\n";
    for (auto& [price, vol] : bid) {
        std::cout << price << " - " << vol << '\n';
    }
}

auto OrderBookContainer::clearOrderBook() -> void {
    auto lock = std::lock_guard{mtx_};
    lastUpdateId_ = -1;
    asks_.clear();
    bids_.clear();
}

auto OrderBookContainer::setOrderBrookFromJson(nlohmann::json& data) -> void {
    auto lock = std::lock_guard{mtx_};

    lastUpdateId_ = data["lastUpdateId"].get<long long int>();

    for (auto& elem : data["bids"]) {
        auto priceInt = static_cast<long long int>(std::round(std::stod(elem[0].get<std::string>()) * 100000000));
        auto volume = static_cast<long long int>(std::round(std::stod(elem[1].get<std::string>()) * 100000000));

        bids_.emplace(priceInt, volume);
    }
    for (auto& elem : data["asks"]) {
        auto priceInt = static_cast<long long int>(std::round(std::stod(elem[0].get<std::string>()) * 100000000));
        auto volume = static_cast<long long int>(std::round(std::stod(elem[1].get<std::string>()) * 100000000));

        asks_.emplace(priceInt, volume);
    }
}

auto OrderBookContainer::updateOrderBookFromEvent(OrderBookWebSocketEvent& currEvent) -> void {
    auto lock = std::lock_guard{mtx_};

    // std::cout << "Updated Event--------------------------------------\n";
    // std::cout << "ask\n";
    // for (auto& elem : currEvent.asks_) {
    //     std::cout << elem << '\n';
    // }
    // std::cout << "bid\n";
    // for (auto& elem : currEvent.bids_) {
    //     std::cout << elem << '\n';
    // }

    for (auto& priceVolume : currEvent.asks_) {
        if (priceVolume.intVolume_ > 0) {
            this->asks_[priceVolume.intPrice_] = priceVolume.intVolume_;
        } else {
            this->asks_.erase(priceVolume.intPrice_);
        }
    }

    for (auto& priceVolume : currEvent.bids_) {
        if (priceVolume.intVolume_ > 0) {
            this->bids_[priceVolume.intPrice_] = priceVolume.intVolume_;
        } else {
    
            this->bids_.erase(priceVolume.intPrice_);
        }
    }

    this->lastUpdateId_ = currEvent.lastUpdateId_;
}

}
#include <vector>
#include <algorithm>
#include <iostream>
#include <limits>

#include "shared/timer.hpp"
#include "shared/marketDataTypes.hpp"

namespace MarketData {

IntPriceVolume::IntPriceVolume(long long int price, long long int volume)
    : intPrice {price}
    , intVolume {volume}
{}

auto IntPriceVolume::operator<(const IntPriceVolume& other) -> bool {
    return this->intPrice < other.intPrice;
}

auto IntPriceVolume::operator<(long long int priceKey) -> bool {
    return this->intPrice < priceKey;
}

auto operator<<(std::ostream& out, const IntPriceVolume& priceVol) -> std::ostream& {
    out << priceVol.intPrice << " - " << priceVol.intVolume;
    return out;
}

PriceCandle::PriceCandle(double open, double high, double low, double close, int time) 
    : candleOpen {open}
    , candleHigh {high}
    , candleLow {low}
    , candleClose {close}
    , timeStamp {time}
{}

PriceCandle::PriceCandle(std::vector<IntPriceVolume>& latestTrades) {
    long long int minPrice = LLONG_MAX;
    long long int maxPrice = LLONG_MIN;
    for (auto& curr : latestTrades) {
        if (curr.intPrice < minPrice) {
            minPrice = curr.intPrice;
        }

        if (curr.intPrice > maxPrice) {
            maxPrice = curr.intPrice;
        }
    }

    candleOpen = latestTrades.front().intPrice / (double) 100000000;
    candleHigh = maxPrice / (double) 100000000;
    candleLow = minPrice / (double) 100000000;
    candleClose = latestTrades.back().intPrice / (double) 100000000;

    auto now = Timer{}.now();
    
    auto duration = now.time_since_epoch();

    timeStamp = std::chrono::duration_cast<std::chrono::seconds>(duration).count();

    latestTrades.clear();
}


auto FlatContainer::insertOrUpdate(long long int price, long long int volume) -> void {
    auto it = std::lower_bound(data_.begin(), data_.end(), price, [](const IntPriceVolume& elem, long long int priceKey) {
        return elem.intPrice < priceKey;
    });

    if (it != data_.end() && it->intPrice == price) {
        (it->intVolume) += volume;
    } else {
        data_.insert(it, {price, volume});
    }
}

auto FlatContainer::print() -> void {
    for (auto& curr : data_) {
        std::cout << '(' << curr.intPrice << ", " << curr.intVolume << ")\n";
    }
}

}
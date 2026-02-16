#include <vector>
#include <algorithm>
#include <iostream>

#include "core/marketDataTypes.hpp"

namespace MarketData {

IntPriceVolume::IntPriceVolume(int price, double volume)
    : price_ {price}
    , volume_ {volume}
{}

auto IntPriceVolume::operator<(const IntPriceVolume& other) -> bool {
    return price_ < other.price_;
}

auto IntPriceVolume::operator<(int priceKey) -> bool {
    return price_ < priceKey;
}


PriceCandle::PriceCandle(double open, double high, double low, double close, int time) 
    : candleOpen {open}
    , candleHigh {high}
    , candleLow {low}
    , candleClose {close}
    , timeStamp {time}
{}


auto FlatContainer::insertOrUpdate(int price, double volume) -> void {
    auto it = std::lower_bound(data_.begin(), data_.end(), price, [](const IntPriceVolume& elem, int priceKey) {
        return elem.price_ < priceKey;
    });

    if (it != data_.end() && it->price_ == price) {
        (it->volume_) += volume;
    } else {
        data_.insert(it, {price, volume});
    }
}

auto FlatContainer::print() -> void {
    for (auto& curr : data_) {
        std::cout << '(' << curr.price_ << ", " << curr.volume_ << ")\n";
    }
}

}
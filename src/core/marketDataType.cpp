#include <vector>
#include <algorithm>
#include <iostream>

#include "core/marketDataTypes.hpp"

namespace MarketData {

IntPriceVolume::IntPriceVolume(long long int price, long long int volume)
    : intPrice_ {price}
    , intVolume_ {volume}
{}

auto IntPriceVolume::operator<(const IntPriceVolume& other) -> bool {
    return this->intPrice_ < other.intPrice_;
}

auto IntPriceVolume::operator<(long long int priceKey) -> bool {
    return this->intPrice_ < priceKey;
}

auto operator<<(std::ostream& out, const IntPriceVolume& priceVol) -> std::ostream& {
    out << priceVol.intPrice_ << " - " << priceVol.intVolume_;
    return out;
}

PriceCandle::PriceCandle(double open, double high, double low, double close, int time) 
    : candleOpen {open}
    , candleHigh {high}
    , candleLow {low}
    , candleClose {close}
    , timeStamp {time}
{}


auto FlatContainer::insertOrUpdate(long long int price, long long int volume) -> void {
    auto it = std::lower_bound(data_.begin(), data_.end(), price, [](const IntPriceVolume& elem, long long int priceKey) {
        return elem.intPrice_ < priceKey;
    });

    if (it != data_.end() && it->intPrice_ == price) {
        (it->intVolume_) += volume;
    } else {
        data_.insert(it, {price, volume});
    }
}

auto FlatContainer::print() -> void {
    for (auto& curr : data_) {
        std::cout << '(' << curr.intPrice_ << ", " << curr.intVolume_ << ")\n";
    }
}

}
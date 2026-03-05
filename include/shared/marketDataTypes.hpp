#pragma once

#include <vector>
#include <iostream>

namespace MarketData {

struct IntPriceVolume {
    long long int intPrice;
    long long int intVolume;

    IntPriceVolume() = default;
    IntPriceVolume(long long int price, long long int volume);

    auto operator<(const IntPriceVolume& other) -> bool;

    auto operator<(long long int priceKey) -> bool;
};

auto operator<<(std::ostream& out, const IntPriceVolume& priceVol) -> std::ostream&;

struct PriceCandle {
    double candleOpen {-1};
    double candleHigh {-1};
    double candleLow {-1};
    double candleClose {-1};
    int timeStamp {-1};

    PriceCandle(double open, double high, double low, double close, int time);

    PriceCandle(std::vector<IntPriceVolume>& latestTrades);
};

class FlatContainer {
private:
    std::vector<IntPriceVolume> data_;

public:
    auto insertOrUpdate(long long int price, long long int volume) -> void;

    auto print() -> void;
};

}
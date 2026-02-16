#pragma once

#include <vector>

namespace MarketData {

struct IntPriceVolume {
    int price_;
    double volume_;

    IntPriceVolume() = default;
    IntPriceVolume(int price, double volume);

    auto operator<(const IntPriceVolume& other) -> bool;

    auto operator<(int priceKey) -> bool;
};

struct PriceCandle {
    double candleOpen {-1};
    double candleHigh {-1};
    double candleLow {-1};
    double candleClose {-1};
    int timeStamp {-1};

    PriceCandle(double open, double high, double low, double close, int time);
};

class FlatContainer {
private:
    std::vector<IntPriceVolume> data_;

public:
    auto insertOrUpdate(int price, double volume) -> void;

    auto print() -> void;
};

}
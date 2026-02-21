#pragma once

#include <vector>
#include <string>
#include <mutex>
#include <condition_variable>

#include "shared/marketDataTypes.hpp"

namespace MarketData {

#define FILE_CANDLE_DATA "data/candleData.txt"

class PriceCandleContainer {
private:
    std::string symbolName;
    std::vector<PriceCandle> data;

    std::mutex dataMutex;

    std::condition_variable newDataAddedCv;
    bool willGetMoreData {true};

public:

    auto push(PriceCandle& candle) -> void;

    auto getCondVar() -> std::condition_variable&;

    auto getMutex() -> std::mutex&;

    auto setWillNotGetMoreData() -> void;

    auto willGetNewData() -> bool;

    auto getData() -> std::vector<PriceCandle>&;

};

auto operator<<(std::ostream& out, const PriceCandle& candle) -> std::ostream&;

auto storeCandleInFile(PriceCandle& candle) -> void;

}
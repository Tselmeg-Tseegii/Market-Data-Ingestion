#include <fstream>

#include "priceCandle/priceCandleContainer.hpp"
#include "core/marketDataTypes.hpp"

namespace MarketData {

auto PriceCandleContainer::push(PriceCandle& candle) -> void {
    {
        auto dataLock = std::lock_guard<std::mutex>{dataMutex};
        data.push_back(candle);
    }
    newDataAddedCv.notify_all();

    storeCandleInFile(candle);
}

auto PriceCandleContainer::getCondVar() -> std::condition_variable& {
    return newDataAddedCv;
}

auto PriceCandleContainer::getMutex() -> std::mutex& {
    return dataMutex;
}

auto PriceCandleContainer::setWillNotGetMoreData() -> void {
    willGetMoreData = false;
    newDataAddedCv.notify_all();
}

auto PriceCandleContainer::willGetNewData() -> bool {
    return willGetMoreData;
}

auto PriceCandleContainer::getData() -> std::vector<PriceCandle>& {
    return data;
}

auto operator<<(std::ostream& out, const PriceCandle& candle) -> std::ostream& {
    out << '(' << candle.candleOpen << ", " << candle.candleLow;
    out << ", " << candle.candleHigh << ", " << candle.candleClose;
    out << ", " << candle.timeStamp << ')';

    return out;
}

auto storeCandleInFile(PriceCandle& candle) -> void {
    auto file = std::ofstream{FILE_CANDLE_DATA, std::ios::app};
    file << candle << '\n';
}

}
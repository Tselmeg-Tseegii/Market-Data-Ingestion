#include "tradeVolume/tradeVolumeContainer.hpp"
#include "external/simdjson/simdjson.h"
#include "tradeVolume/tradeVolumeWebSocketEvent.hpp"
#include "shared/timer.hpp"

namespace MarketData {

auto TradeVolumeContainer::updateFromEvent(TradeVolumeWebSocketEvent& event) -> void {
    {
        auto lock = std::lock_guard<std::mutex>{mtx_};

        tradeVolumeMap_[event.intPrice] += event.intVolume;
        tradeVolumeSeq_.emplace_back(event.intPrice, event.intVolume);

        if (tradeVolumeSeq_.size() > 20) {
            auto& frontElem = tradeVolumeSeq_.front();
            auto foundIt = tradeVolumeMap_.find(frontElem.intPrice);
            foundIt->second -= frontElem.intVolume;
            if (foundIt->second == 0) {
                tradeVolumeMap_.erase(foundIt);
            }
            tradeVolumeSeq_.pop_front();
        }
    }

    newDataAddedCv_.notify_all();
}

auto TradeVolumeContainer::print() -> void {
    for (auto& elem : tradeVolumeMap_) {
        std::cout << elem.first << " - " << elem.second << '\n';
    }
}

auto TradeVolumeContainer::getCondVar() -> std::condition_variable& {
    return newDataAddedCv_;
}

auto TradeVolumeContainer::getMutex() -> std::mutex& {
    return mtx_;
}

auto TradeVolumeContainer::setWillNotGetMoreData() -> void {
    willGetMoreData_ = false;
    newDataAddedCv_.notify_all();
}

auto TradeVolumeContainer::willGetNewData() -> bool {
    return willGetMoreData_;
}


auto TradeVolumeContainer::getLastTrades() -> std::vector<IntPriceVolume> {
    auto lock = std::lock_guard<std::mutex>{mtx_};
    std::vector<IntPriceVolume> result;
    result.reserve(tradeVolumeSeq_.size());
    for (auto& elem : tradeVolumeSeq_) {
        result.push_back(elem);
    }
    return result;
}

}
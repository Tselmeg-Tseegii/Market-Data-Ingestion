#include "tradeVolume/tradeVolumeContainer.hpp"
#include "core/rawEvent.hpp"
#include "external/simdjson/simdjson.h"

namespace MarketData {
 
// auto TradeVolumeContainer::updateFromEvent(RawEvent& event, simdjson::ondemand::parser& jsonParser) -> void {
//     auto doc = simdjson::ondemand::document{};

//     auto& currEventBuffer = event.data_;

//     auto currEventDataPtr = static_cast<char const*>(currEventBuffer.data().data());

//     auto errors = jsonParser.iterate(
//         simdjson::padded_string_view(currEventDataPtr, currEventBuffer.size(), currEventBuffer.capacity())
//     ).get(doc);

//     auto price = static_cast<int>(doc["p"].get_double_in_string().value() * 100);
//     auto volume = doc["q"].get_double_in_string().value();
    
//     tradeVolume_.insertOrUpdate(price, volume);

//     event.lifeTime_.stop();
// }

auto TradeVolumeContainer::updateFromEvent(RawEvent& event, simdjson::ondemand::parser& jsonParser) -> void {
    auto doc = simdjson::ondemand::document{};

    auto& currEventBuffer = event.data_;

    auto currEventDataPtr = static_cast<char const*>(currEventBuffer.data().data());

    auto errors = jsonParser.iterate(
        simdjson::padded_string_view(currEventDataPtr, currEventBuffer.size(), currEventBuffer.capacity())
    ).get(doc);

    auto price = static_cast<int>(doc["p"].get_double_in_string().value() * 100);
    auto volume = doc["q"].get_double_in_string().value();
    
    tradeVolume_[price] += volume;

    event.lifeTime_.stop();
}

auto TradeVolumeContainer::print() -> void {
    // tradeVolume_.print();
}

}
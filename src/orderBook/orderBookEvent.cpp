#include "orderBook/orderBookWebSocketEvent.hpp"

namespace MarketData {

OrderBookWebSocketEvent::OrderBookWebSocketEvent(boost::beast::flat_buffer&& data)
    : RawEvent(std::move(data))
{}

auto OrderBookWebSocketEvent::initialiseParser() -> void {
    parser_ = std::make_unique<simdjson::ondemand::parser>();
    auto rawDataPtr = static_cast<char const*>(data_.data().data());

    auto errors = parser_->iterate(
        simdjson::padded_string_view(rawDataPtr, data_.size(), data_.capacity())
    ).get(doc_);

    parserIsActive_ = true;
}

auto OrderBookWebSocketEvent::getFirstUpdateId() -> long long int {
    if (firstUpdateId_ == -1) {
        if (!parserIsActive_) {
            this->initialiseParser();
        }
        firstUpdateId_ = doc_["U"].get<long long int>().value();
    }
    return firstUpdateId_;
}

auto OrderBookWebSocketEvent::getLastUpdateId() -> long long int {
    if (lastUpdateId_ == -1) {
        this->getFirstUpdateId();
        lastUpdateId_ = doc_["u"].get<long long int>().value();
    }
    return lastUpdateId_;
}

}
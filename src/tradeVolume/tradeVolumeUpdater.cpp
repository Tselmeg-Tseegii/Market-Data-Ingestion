#include "tradeVolume/tradeVolumeUpdater.hpp"

namespace MarketData {


TradeVolumeUpdater::TradeVolumeUpdater(TradeVolumeContainer& container, EventQueue<RawEvent>& queue)
    : container_ {container}
    , queue_ {queue}
    , stopThread_ {false}
{
    updateThread_ = std::thread{&TradeVolumeUpdater::updateFromEventQueue, this};
}

auto TradeVolumeUpdater::stopThread() -> void {
    stopThread_ = true;
    queue_.getCv().notify_all();
    updateThread_.join();
}

auto TradeVolumeUpdater::updateFromEventQueue() -> void {
    auto& eventQueueCv = queue_.getCv();
    auto eventQueueLock = std::unique_lock{queue_.getMtx()};
    eventQueueLock.unlock();

    auto jsonParser = simdjson::ondemand::parser{};
    while (true) {
        auto currEvents = EventQueue<RawEvent>{};

        eventQueueLock.lock();
        eventQueueCv.wait(eventQueueLock, [this] () {
            return queue_.IsNotEmptyFlag() || stopThread_;
        });

        if (stopThread_) {
            return;
        }
        
        eventQueueLock.unlock();

        queue_.spliceTo(currEvents);
        

        for (auto& currEvent : currEvents.getData()) {
            if (stopThread_) {
                return;
            }
            auto doc = simdjson::ondemand::document{};

            auto& currEventBuffer = currEvent.data_;

            auto currEventDataPtr = static_cast<char const*>(currEventBuffer.data().data());

            auto errors = jsonParser.iterate(
                simdjson::padded_string_view(currEventDataPtr, currEventBuffer.size(), currEventBuffer.capacity())
            ).get(doc);

            container_.updateFromEvent(doc);
        }
    }
}

}

#include "tradeVolume/tradeVolumeUpdater.hpp"
#include "shared/timer.hpp"

namespace MarketData {


TradeVolumeUpdater::TradeVolumeUpdater(TradeVolumeContainer& container, EventQueue<TradeVolumeWebSocketEvent>& queue)
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

    while (true) {
        auto currEvents = EventQueue<TradeVolumeWebSocketEvent>{};

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

            container_.updateFromEvent(currEvent);
        }
    }
}

}

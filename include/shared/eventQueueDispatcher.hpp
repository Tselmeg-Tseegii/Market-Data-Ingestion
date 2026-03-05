#pragma once

#include <vector>
#include <memory>
#include <thread>

#include "shared/eventQueue.hpp"

namespace MarketData {

template <typename Event>
class EventQueueDispatcher {
private:
    EventQueue<Event>& mainQueue_;
    std::vector<EventQueue<Event>*> consumerQueuePtrs_;
    std::thread dispatchLoopThread_;

public:
    EventQueueDispatcher(EventQueue<Event>& queue)
        : mainQueue_{queue}
    {
        dispatchLoopThread_ = std::thread{&EventQueueDispatcher::dispatchLoop, this};
    }

    auto addConsumerQueue(EventQueue<Event>& queue) -> void {
        consumerQueuePtrs_.emplace_back(&queue);
    }

private:
    auto dispatchLoop() -> void {
        auto& mainEventQueueCv = mainQueue_.getCv();
        auto mainEventQueueLock = std::unique_lock{mainQueue_.getMtx()};
        mainEventQueueLock.unlock();

        while (true) {
            auto currEvents = EventQueue<Event>{};

            mainEventQueueLock.lock();
            mainEventQueueCv.wait(mainEventQueueLock, [this] () {
                return mainQueue_.IsNotEmptyFlag();
            }); 
            
            mainEventQueueLock.unlock();

            mainQueue_.spliceTo(currEvents);

            for (auto& consumerQueuePtr : consumerQueuePtrs_) {
                for (auto& event : currEvents.getData()) {
                    consumerQueuePtr->pushAndNotify(event);
                }

            }
        }
    }
};

}
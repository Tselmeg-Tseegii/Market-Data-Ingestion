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
    bool stopDispatch_ {false};

public:
    EventQueueDispatcher(EventQueue<Event>& queue)
        : mainQueue_{queue}
    {
        dispatchLoopThread_ = std::thread{&EventQueueDispatcher::dispatchLoop, this};
    }

    auto addConsumerQueue(EventQueue<Event>& queue) -> void {
        consumerQueuePtrs_.emplace_back(&queue);
    }

    auto stopConsumerQueue(EventQueue<Event>& queue) -> void {
        for (auto it = consumerQueuePtrs_.begin(); it != consumerQueuePtrs_.end(); it++) {
            if (*it == &queue) {
                consumerQueuePtrs_.erase(it);
                break;
            }
        }
    }

    auto stop() -> void {
        stopDispatch_ = true;
        mainQueue_.getCv().notify_all();
        dispatchLoopThread_.join();
    }

private:
    auto dispatchLoop() -> void {
        auto& mainEventQueueCv = mainQueue_.getCv();
        auto mainEventQueueLock = std::unique_lock{mainQueue_.getMtx()};
        mainEventQueueLock.unlock();

        while (!stopDispatch_) {
            auto currEvents = EventQueue<Event>{};

            mainEventQueueLock.lock();
            mainEventQueueCv.wait(mainEventQueueLock, [this] () {
                return mainQueue_.IsNotEmptyFlag() || stopDispatch_;
            }); 
            if (stopDispatch_) {
                break;
            }
            
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
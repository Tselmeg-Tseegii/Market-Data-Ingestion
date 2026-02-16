#pragma once

#include <thread>
#include <atomic>

#include "orderBook/orderBookContainer.hpp"
#include "core/eventQueue.hpp"
#include "external/httplib.h"

namespace MarketData {

class OrderBookUpdater {
private:
    OrderBookContainer& container_;
    EventQueue<OrderBookWebSocketEvent>& eventsQueue_;

    std::atomic_bool stopUpdate_;

    std::thread webSocketThread_;
    std::thread updateOrderBookThread_;

public:
    OrderBookUpdater(OrderBookContainer& container, EventQueue<OrderBookWebSocketEvent>& events);

    auto stopUpdate() -> void;

private:
    auto continualUpdate() -> void;
};
}
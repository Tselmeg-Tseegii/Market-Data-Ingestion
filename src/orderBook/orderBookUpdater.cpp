#include "orderBook/orderBookUpdater.hpp"

namespace MarketData {


OrderBookUpdater::OrderBookUpdater(OrderBookContainer& container, EventQueue<OrderBookWebSocketEvent>& events)
    : container_ {container}
    , eventsQueue_ {events}
    , stopUpdate_ {false}
{
    updateOrderBookThread_ = std::thread{&OrderBookUpdater::continualUpdate, this};
}

auto OrderBookUpdater::stopUpdate() -> void {
    stopUpdate_ = true;
    eventsQueue_.getCv().notify_all();

    updateOrderBookThread_.join();
}

auto OrderBookUpdater::continualUpdate() -> void {
restart_update_orderbook_process:
    auto eventBufferLock = std::unique_lock<std::mutex>{eventsQueue_.getMtx()};
    auto& eventBufferCv = eventsQueue_.getCv();
    
    eventBufferCv.wait(eventBufferLock, [this]() {
        return eventsQueue_.IsNotEmptyFlag() || stopUpdate_;
    });
    eventBufferLock.unlock();

    if (stopUpdate_) {
        return;
    }

    auto& firstEvent = eventsQueue_.getFront();
    auto veryFirstUpdateId = firstEvent.getFirstUpdateId();
    
    auto client = httplib::Client{"https://api.binance.com"};
    auto path = std::string{"/api/v3/depth?symbol=BTCUSDT&limit=5000"};

    long long int snapshotLastUpdateId = -1;
    auto snapshotJson = nlohmann::json{};

    while (snapshotLastUpdateId == -1 || snapshotLastUpdateId < veryFirstUpdateId) {
        auto res = httplib::Result{client.Get(path)};
        if (res->status != 200) {
            std::cout << "error in result\n";
        }
        snapshotJson = nlohmann::json::parse(res->body);
        snapshotLastUpdateId = snapshotJson["lastUpdateId"].get<long long int>();
    }

    auto currList = EventQueue<OrderBookWebSocketEvent>{};
    eventsQueue_.spliceTo(currList);
    auto& currListData = currList.getData();
    for (auto it {currListData.begin()}; it != currListData.end(); ) {
        auto currLastUpdateId = it->getLastUpdateId();
        if (currLastUpdateId <= snapshotLastUpdateId) {
            it = currListData.erase(it);
        } else {
            it++;
        }
    }

    container_.setOrderBrookFromJson(snapshotJson);

    for (auto& currEvent : currListData) {
        auto currOrderBookLastUpdateId = container_.getLastUpdateId();

        if (currEvent.getFirstUpdateId() > (currOrderBookLastUpdateId + 1)) {
            container_.clearOrderBook();
            eventsQueue_.clear();

            goto restart_update_orderbook_process;
        }
        
        container_.updateOrderBookFromEvent(currEvent);
    }
    
    while (true) {
        auto currList = EventQueue<OrderBookWebSocketEvent>{};
        eventBufferLock.lock();
        eventBufferCv.wait(eventBufferLock, [this]() {
            return eventsQueue_.IsNotEmptyFlag() || stopUpdate_;
        });

        if (stopUpdate_) {
            return;
        }

        eventBufferLock.unlock();

        eventsQueue_.spliceTo(currList);
        
        for (auto& currEvent : currList.getData()) {
            auto currOrderBookLastUpdateId = container_.getLastUpdateId();

            if (currEvent.getLastUpdateId() < currOrderBookLastUpdateId) {
                continue;
            }

            if (currEvent.getFirstUpdateId() > (currOrderBookLastUpdateId + 1)) {
                container_.clearOrderBook();
                eventsQueue_.clear();

                goto restart_update_orderbook_process;
            }
            
            container_.updateOrderBookFromEvent(currEvent);
        }
    }

}
};
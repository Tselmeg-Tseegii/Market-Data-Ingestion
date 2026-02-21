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
    auto client = httplib::Client{"https://api.binance.com"};
    auto path = std::string{"/api/v3/depth?symbol=BTCUSDT&limit=5000"};

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

    auto veryFirstUpdateId = eventsQueue_.getFront().firstUpdateId_;
    
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

    
    eventBufferLock.lock();
    auto& currListData = eventsQueue_.getData();
    for (auto it {currListData.begin()}; it != currListData.end(); ) {
        if (it->lastUpdateId_ <= snapshotLastUpdateId) {
            it = currListData.erase(it);
        } else {
            it++;
        }
    }
    eventBufferLock.unlock();

    container_.setOrderBrookFromJson(snapshotJson);
    
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

            if (currEvent.firstUpdateId_ < currOrderBookLastUpdateId) {
                continue;
            }

            if (currEvent.firstUpdateId_ > (currOrderBookLastUpdateId + 1)) {
                container_.clearOrderBook();
                eventsQueue_.clear();

                goto restart_update_orderbook_process;
            }
            
            container_.updateOrderBookFromEvent(currEvent);
        }
    }

}
};
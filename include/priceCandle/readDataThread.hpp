#pragma once

#include <mutex>
#include <condition_variable>
#include <thread>

#include "priceCandle/priceCandleContainer.hpp"
#include "external/httplib.h"

namespace MarketData {

#define API_REQUEST_PER_MIN 7
#define API_REQUEST_INTERVAL_SEC 60

class ReadDataThread {
private:
    std::mutex stopSignalMutex_;
    std::condition_variable cvSignalManager_;
    bool stopReading_ ;
    std::thread thread_;

    PriceCandleContainer& container_;

    httplib::Client apiClient_;
    std::string apiRequestEndPoint_;
    std::string apiPastRequestEndPoint_;

public:
    ReadDataThread(PriceCandleContainer& container);

    auto stopThread() -> void;

private:
    auto getStartUpData() -> void;

    auto getOneCandleRequest() -> PriceCandle;

    auto onlineReadLoop() -> void;
};

}
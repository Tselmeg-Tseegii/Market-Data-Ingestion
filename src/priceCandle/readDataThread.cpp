#include <cstdlib>

#include "priceCandle/readDataThread.hpp"
#include "core/timer.hpp"
#include "external/nlohmann/json.hpp"

namespace MarketData {

ReadDataThread::ReadDataThread(PriceCandleContainer& container)
    : stopReading_ {false}
    , container_ {container}
    , apiClient_{"https://api.twelvedata.com"}
{
    auto MY_API_KEY = std::string{std::getenv("TWELVEDATA_MY_API_KEY")};
    auto request = std::string{"/quote"};
    auto symbol = std::string{"XAU/USD"};
    auto interval = std::string{"1min"};

    apiRequestEndPoint_ = {request + "?symbol=" + symbol + "&interval=" + interval + "&apikey=" + MY_API_KEY};
    request = "/time_series";
    auto outputSize = std::string{"55"};
    auto order = std::string{"asc"};
    apiPastRequestEndPoint_ = {request + "?symbol=" + symbol + "&interval=" + interval + "&outputsize=" + outputSize + "&order=" + order + "&apikey=" + MY_API_KEY};
    
    thread_ = std::thread{&ReadDataThread::onlineReadLoop, this};
}

auto ReadDataThread::stopThread() -> void {
    {
        auto stopFlagLock = std::lock_guard<std::mutex>{stopSignalMutex_};
        stopReading_ = true;
    }
    cvSignalManager_.notify_all();
    thread_.join();
}

auto ReadDataThread::getStartUpData() -> void {
    auto latestCandle = getOneCandleRequest();
    auto latestOneMinTimeStamp = latestCandle.timeStamp;
    auto currTimeStamp = latestOneMinTimeStamp - 55 * 60;

    auto res = httplib::Result{apiClient_.Get(apiPastRequestEndPoint_)};
    
    if (res->status != 200) {
        std::cout << "error in past";
    }

    auto body = nlohmann::json::parse(res->body);
    auto values = body["values"];
    for (auto& item : values) {
        auto open = std::stod(item["open"].get<std::string>());
        auto high = std::stod(item["high"].get<std::string>());
        auto low = std::stod(item["low"].get<std::string>());
        auto close = std::stod(item["close"].get<std::string>());
        auto time = currTimeStamp;

        auto currCandle = PriceCandle{open, high, low, close, time};
        container_.push(currCandle);
        currTimeStamp += 60;
    }
}

auto ReadDataThread::getOneCandleRequest() -> PriceCandle {
    auto res = httplib::Result{apiClient_.Get(apiRequestEndPoint_)};
    if (res->status != 200) {
        std::cout << "status not 200\n";
    }

    nlohmann::json data = nlohmann::json::parse(res->body);

    auto open = std::stod(data["open"].get<std::string>());
    auto high = std::stod(data["high"].get<std::string>());
    auto low = std::stod(data["low"].get<std::string>());
    auto close = std::stod(data["close"].get<std::string>());
    auto time = data["timestamp"].get<int>();
    
    return PriceCandle{open, high, low, close, time};
}

auto ReadDataThread::onlineReadLoop() -> void {
    getStartUpData();
    
    auto apiRequestInterval = std::chrono::duration<double>{
        std::chrono::seconds{API_REQUEST_INTERVAL_SEC}
    };

    auto currTime = Timer{}.now();
    auto nextTime = currTime + apiRequestInterval;

    auto stopSignalLock = std::unique_lock<std::mutex>{stopSignalMutex_};
    while(true) {
        cvSignalManager_.wait_until(
            stopSignalLock, 
            nextTime, 
            [this] () {
                return stopReading_;
            }
        );

        if (stopReading_) {
            container_.setWillNotGetMoreData();
            break;
        }

        auto latestCandle = PriceCandle{getOneCandleRequest()};

        container_.push(latestCandle);

        nextTime = nextTime + apiRequestInterval;
    }
}

}
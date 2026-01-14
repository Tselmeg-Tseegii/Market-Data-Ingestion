#include <iostream>
#include <cstdlib>
#include <string>
#include <thread>
#include <mutex>
#include <condition_variable>
#include "nlohmann/json.hpp"

#define CPPHTTPLIB_OPENSSL_SUPPORT
#include "httplib.h"

#include "timer.h"

#define API_REQUESTS_PER_MIN 7
#define API_REQUEST_INTERVAL_SEC 60 / API_REQUESTS_PER_MIN + 1

struct PriceCandle {
public:
    double candleOpen;
    double candleHigh;
    double candleLow;
    double candleClose;
    int timeStamp;

    PriceCandle(double open, double high, double low, double close, int time) 
        : candleOpen {open}
        , candleHigh {high}
        , candleLow {low}
        , candleClose {close}
        , timeStamp {time}
    {}

    friend auto operator<<(std::ostream& out, PriceCandle& candle) -> std::ostream&;
};

auto operator<<(std::ostream& out, PriceCandle& candle) -> std::ostream& {
    out << '(' << candle.candleOpen << ", " << candle.candleLow;
    out << ", " << candle.candleHigh << ", " << candle.candleClose;
    out << ", " << candle.timeStamp << ')';

    return out;
}

class ReadDataThread;

class PriceData {
    friend class ReadDataThread;
public:
    std::string symbolName;
    std::vector<PriceCandle> data;

private:
    std::mutex dataLock;

    auto getMutex() -> std::mutex& {
        return dataLock;
    }

    
};

class ReadDataThread {
private:
    std::mutex stopSignalLock_;
    std::condition_variable cvSignalManager_;
    bool stopReading_;
    std::thread thread_;

    std::vector<PriceCandle>& dataContainer_;
    std::mutex& dataContainerMutex_;

    httplib::Client apiClient_;
    std::string apiRequestEndPoint_;

public:
    ReadDataThread(PriceData& container)
        : stopReading_ {false}
        , dataContainer_ {container.data}
        , dataContainerMutex_ {container.getMutex()}
        , apiClient_{"https://api.twelvedata.com"}
    {
        auto MY_API_KEY = std::string{std::getenv("TWELVEDATA_MY_API_KEY")};
        auto dataTypeRequested = std::string{"/quote"};
        auto symbol = std::string{"XAU/USD"};
        auto interval = std::string{"1min"};

        apiRequestEndPoint_ = {dataTypeRequested + "?symbol=" + symbol + "&interval" + interval + "&apikey=" + MY_API_KEY};
    }

    auto startThread() -> void {
        thread_ = std::thread{&ReadDataThread::readLoop, this};
    }

    auto stopThread() -> void {
        {
            auto stopFlagLock = std::lock_guard<std::mutex>{stopSignalLock_};
            stopReading_ = true;
        }
        cvSignalManager_.notify_one();
        thread_.join();
    }

private:
    auto getCandleRequest() -> PriceCandle {
        auto res = httplib::Result{apiClient_.Get(apiRequestEndPoint_)};
        if (res->status != 200) {
            std::cout << "status not 200\n";
        }

        std::cout << res->body << '\n';

        nlohmann::json data = nlohmann::json::parse(res->body);

        auto open = std::stod(data["open"].get<std::string>());
        auto high = std::stod(data["high"].get<std::string>());
        auto low = std::stod(data["low"].get<std::string>());
        auto close = std::stod(data["close"].get<std::string>());
        auto time = data["timestamp"].get<int>();
       
        return PriceCandle{open, high, low, close, time};
    }

    auto readLoop() -> void {
        auto apiRequestInterval = std::chrono::duration<double>{
            std::chrono::seconds{API_REQUEST_INTERVAL_SEC}
        };

        auto stopSignalLock = std::unique_lock<std::mutex>{stopSignalLock_};
        
        while(true) {
            cvSignalManager_.wait_for(
                stopSignalLock, 
                apiRequestInterval, 
                [this] () {
                    return stopReading_;
                }
            );

            if (stopReading_) {
                break;
            }

            auto latestCandle = PriceCandle{getCandleRequest()};
            {
                auto dataLock = std::lock_guard<std::mutex>{dataContainerMutex_};
                dataContainer_.push_back(latestCandle);
            }
        
        }
    }
};

int main() {
    
    auto goldPrices = PriceData{};
    
    auto readThread = ReadDataThread{goldPrices};
    readThread.startThread();

    std::this_thread::sleep_for(std::chrono::seconds{20});

    readThread.stopThread();

    for (auto currCandle : goldPrices.data) {
        std::cout << currCandle << '\n';
    }
}
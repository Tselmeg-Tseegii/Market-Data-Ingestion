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

class ReadDataThread {
private:
    std::mutex stopSignalLock_;
    std::condition_variable cvSignalManager_;
    bool stopReading_;
    std::thread thread_;

    std::vector<double>& dataContainer_;
    std::mutex& dataContainerMutex_;

    httplib::Client apiClient_;
    std::string apiRequestEndPoint_;

public:
    ReadDataThread(std::vector<double>& data, std::mutex& dataLock)
        : stopReading_ {false}
        , dataContainer_ {data}
        , dataContainerMutex_ {dataLock}
        , apiClient_{"https://api.twelvedata.com"}
    {
        auto MY_API_KEY = std::string{std::getenv("TWELVEDATA_MY_API_KEY")};
        auto dataTypeRequested = std::string{"/price"};
        auto symbol = std::string{"EUR/USD"};

        apiRequestEndPoint_ = {dataTypeRequested + "?symbol=" + symbol + "&apikey=" + MY_API_KEY};
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
    auto getDataRequest() -> double {
        auto res = httplib::Result{apiClient_.Get(apiRequestEndPoint_)};
        if (res->status != 200) {
            std::cout << "status not 200\n";
        }

        nlohmann::json data = nlohmann::json::parse(res->body);

        auto iterToPrice = data.find("price");
        if (iterToPrice != data.end()) {
            double price = std::stod(static_cast<std::string>(iterToPrice.value()));
            return price;
        } else {
            std::cout << "no price found\n";
            return -1;
        }
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

            auto latestPrice = getDataRequest();
            {
                auto dataLock = std::lock_guard<std::mutex>{dataContainerMutex_};
                dataContainer_.push_back(latestPrice);
            }
        
        }
    }
};

int main() {
    
    auto dataPoints = std::vector<double>{};
    auto dataLock = std::mutex{};
    
    auto readThread = ReadDataThread{dataPoints, dataLock};
    readThread.startThread();

    std::this_thread::sleep_for(std::chrono::seconds{20});

    readThread.stopThread();

    for (double curr : dataPoints) {
        std::cout << curr << '\n';
    }


}
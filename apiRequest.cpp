#include <iostream>
#include <cstdlib>
#include <string>
#include <thread>
#include <mutex>
#include "nlohmann/json.hpp"

#define CPPHTTPLIB_OPENSSL_SUPPORT
#include "httplib.h"

#include "timer.h"

#define API_REQUESTS_PER_MIN 7
#define API_REQUEST_INTERVAL_SEC 60 / API_REQUESTS_PER_MIN + 1

void readData(std::vector<double>& data, std::mutex& dataLock, int& toStop) {
    auto client = httplib::Client{"https://api.twelvedata.com"};
    auto MY_API_KEY = std::string{std::getenv("TWELVEDATA_MY_API_KEY")};
    auto dataTypeRequested = std::string{"/price"};
    auto symbol = std::string{"EUR/USD"};

    auto endPoint {dataTypeRequested + "?symbol=" + symbol + "&apikey=" + MY_API_KEY};

    auto startTime = std::chrono::high_resolution_clock::now();
    auto apiRequestInterval = std::chrono::seconds(API_REQUEST_INTERVAL_SEC);

    while (toStop != 0) {
        auto res = httplib::Result{client.Get(endPoint)};
        if (res->status != 200) {
            std::cout << "status not 200\n";
        }
        // std::cout << res->body << '\n';

        nlohmann::json data = nlohmann::json::parse(res->body);

        auto iterToPrice = data.find("price");
        if (iterToPrice != data.end()) {
            double price = std::stod(static_cast<std::string>(iterToPrice.value()));
            std::cout << "got price: " << price << '\n';
        } else {
            std::cout << "no price found\n";
        }

        startTime += apiRequestInterval;
        std::this_thread::sleep_until(startTime);
    }
}

int main() {
    
    std::vector<double> dataPoints{};
    std::mutex dataLock{};
    int stop = 1;

    auto readThread = std::thread{
        readData, 
        std::ref(dataPoints), 
        std::ref(dataLock), 
        std::ref(stop)
    };

    std::cout << "hi from main\n";
    std::cin >> stop;

    readThread.join();
}
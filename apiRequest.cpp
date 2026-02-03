#include <iostream>
#include <cstdlib>
#include <string>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <boost/process.hpp>
#include <fstream>

#include "nlohmann/json.hpp"

#define CPPHTTPLIB_OPENSSL_SUPPORT
#include "httplib.h"

#include "timer.h"

#define API_REQUESTS_PER_MIN 7
#define API_REQUEST_INTERVAL_SEC 60
#define FILE_CANDLE_DATA "data/candleData.txt"
#define FILE_PREDICTION_DATA "data/predictionData.txt"

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

class PriceDataContainer {
    friend class ReadDataThread;
public:
    std::string symbolName;
    std::vector<PriceCandle> data;
    bool noMoreData {false};

private:
    std::mutex dataLock;
    std::condition_variable dataCv;

public:
    auto getMutex() -> std::mutex& {
        return dataLock;
    }

    auto newDataAdded() -> void {
        dataCv.notify_one();
    }

    auto getCondVar() -> std::condition_variable& {
        return dataCv;
    }
};

auto storeCandleInFile(PriceCandle& candle) -> void {
    auto file = std::ofstream{FILE_CANDLE_DATA, std::ios::app};
    file << candle << '\n';
}

class ReadDataThread {
private:
    std::mutex stopSignalLock_;
    std::condition_variable cvSignalManager_;
    bool stopReading_;
    std::thread thread_;

    PriceDataContainer& container_;

    httplib::Client apiClient_;
    std::string apiRequestEndPoint_;

public:
    ReadDataThread(PriceDataContainer& container)
        : stopReading_ {false}
        , container_ {container}
        , apiClient_{"https://api.twelvedata.com"}
    {
        auto MY_API_KEY = std::string{std::getenv("TWELVEDATA_MY_API_KEY")};
        auto dataTypeRequested = std::string{"/quote"};
        auto symbol = std::string{"XAU/USD"};
        auto interval = std::string{"1min"};

        apiRequestEndPoint_ = {dataTypeRequested + "?symbol=" + symbol + "&interval=" + interval + "&apikey=" + MY_API_KEY};
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

        nlohmann::json data = nlohmann::json::parse(res->body);

        auto open = std::stod(data["open"].get<std::string>());
        auto high = std::stod(data["high"].get<std::string>());
        auto low = std::stod(data["low"].get<std::string>());
        auto close = std::stod(data["close"].get<std::string>());
        auto time = data["timestamp"].get<int>();
       
        return PriceCandle{open, high, low, close, time};
    }

    auto shouldAddCandle(PriceDataContainer& container, PriceCandle& candle) -> bool {
        auto& data = container.data;
        if (data.empty() || (!data.empty() &&
            data.back().timeStamp != candle.timeStamp)) {
            return true;
        } else {
            return false;
        }
    }

    auto readLoop() -> void {
        auto apiRequestInterval = std::chrono::duration<double>{
            std::chrono::seconds{API_REQUEST_INTERVAL_SEC}
        };

        auto stopSignalLock = std::unique_lock<std::mutex>{stopSignalLock_};
        
        while(true) {
            auto latestCandle = PriceCandle{getCandleRequest()};
            bool useCandle = false;
            {
                auto dataLock = std::lock_guard<std::mutex>{container_.getMutex()};
                
                if (shouldAddCandle(container_, latestCandle)) {
                    useCandle = true;
                }
                if (useCandle == true) {;
                    container_.data.push_back(latestCandle);
                    container_.getCondVar().notify_one();
                }
            }
            if (useCandle == true) {
                storeCandleInFile(latestCandle);
            }
        
            cvSignalManager_.wait_for(
                stopSignalLock, 
                apiRequestInterval, 
                [this] () {
                    return stopReading_;
                }
            );

            if (stopReading_) {
                container_.noMoreData = true;
                break;
            }
        }
    }
};

auto managePythonProcess(
    PriceDataContainer& container,
    std::string pythonFile
) -> void {
    auto pipeToPython = boost::process::opstream{};
    auto pipeFromPython = boost::process::ipstream{};

    auto pythonProcess = boost::process::child{
        boost::process::search_path("py"),
        "-3.11",
        "-u",
        pythonFile,
        boost::process::std_in < pipeToPython,
        boost::process::std_out > pipeFromPython, 
        boost::process::std_err > stderr
    };

    auto prefictionSaveFile = std::fstream{FILE_PREDICTION_DATA, std::ios::app};

    auto& cvContainer = container.getCondVar();
    auto lockContainer = std::unique_lock<std::mutex>{container.getMutex()};
    
    while (true) {
        cvContainer.wait(lockContainer);

        if (container.noMoreData == true) {
            break;
        }

        std::cout << "HI send " << container.data.back() << '\n';
        pipeToPython << container.data.back() << std::endl;

        lockContainer.unlock();

        auto pythonMessage = std::string{};
        std::getline(pipeFromPython, pythonMessage);

        std::cout << "got python " << pythonMessage << '\n';
        prefictionSaveFile << pythonMessage << '\n';

        lockContainer.lock();
    }
    pipeToPython.close();
    pipeFromPython.close();
    pythonProcess.wait();
}

int main() {
    
    auto goldPrices = PriceDataContainer{};
    
    auto readThread = ReadDataThread{goldPrices};
    readThread.startThread();

    auto sendPythonThread = std::thread{
        managePythonProcess, 
        std::ref(goldPrices),
        "tradeDecision.py"
    };

    int stop{};
    std::cin >> stop;
    if (stop == -1) {
        readThread.stopThread();
    }

    for (auto currCandle : goldPrices.data) {
        std::cout << currCandle << '\n';
    }
}
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

    PriceCandle(
        double open = -1, 
        double high = -1, 
        double low = -1, 
        double close = -1, 
        int time = -1
    ) 
        : candleOpen {open}
        , candleHigh {high}
        , candleLow {low}
        , candleClose {close}
        , timeStamp {time}
    {}

    friend auto operator<<(std::ostream& out, const PriceCandle& candle) -> std::ostream&;
};

auto operator<<(std::ostream& out, const PriceCandle& candle) -> std::ostream& {
    out << '(' << candle.candleOpen << ", " << candle.candleLow;
    out << ", " << candle.candleHigh << ", " << candle.candleClose;
    out << ", " << candle.timeStamp << ')';

    return out;
}

auto storeCandleInFile(PriceCandle& candle) -> void {
    auto file = std::ofstream{FILE_CANDLE_DATA, std::ios::app};
    file << candle << '\n';
}

class PriceDataContainer {
private:
    std::string symbolName;
    std::vector<PriceCandle> data;

    std::mutex dataMutex;

    std::condition_variable newDataAddedCv;
    bool willGetMoreData {true};

public:

    auto push(PriceCandle& candle) -> void {
        {
            auto dataLock = std::lock_guard<std::mutex>{dataMutex};
            data.push_back(candle);
        }
        newDataAddedCv.notify_all();

        storeCandleInFile(candle);
    }

    auto getCondVar() -> std::condition_variable& {
        return newDataAddedCv;
    }

    auto getMutex() -> std::mutex& {
        return dataMutex;
    }

    auto setWillNotGetMoreData() -> void {
        willGetMoreData = false;
        newDataAddedCv.notify_all();
    }

    auto willGetNewData() -> bool {
        return willGetMoreData;
    }

    auto getData() -> std::vector<PriceCandle>& {
        return data;
    }
};

class ReadDataThread {
private:
    std::mutex stopSignalMutex_;
    std::condition_variable cvSignalManager_;
    bool stopReading_;
    std::jthread thread_;

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
    
        thread_ = std::jthread{&ReadDataThread::readLoop, this};
    }

    auto stopThread() -> void {
        {
            auto stopFlagLock = std::lock_guard<std::mutex>{stopSignalMutex_};
            stopReading_ = true;
        }
        cvSignalManager_.notify_all();
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

    auto readLoop() -> void {
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

            auto latestCandle = PriceCandle{getCandleRequest()};

            container_.push(latestCandle);

            nextTime = nextTime + apiRequestInterval;
        }
    }
};

class ManagePythonProcess {
private:
    PriceDataContainer& container_;

    boost::process::opstream pipeToPython_;
    boost::process::ipstream pipeFromPython_;

    std::jthread sendDataThread_;
    std::jthread getDataThread_;

    boost::process::child pythonProcess_;

public:
    ManagePythonProcess(
        PriceDataContainer& container,
        std::string pythonFile
    )
        : container_ {container}
        , pipeToPython_ {}
        , pipeFromPython_ {}

    {
        pythonProcess_ = boost::process::child{
            boost::process::search_path("py"),
            "-3.11",
            "-u",
            pythonFile,
            boost::process::std_in < pipeToPython_,
            boost::process::std_out > pipeFromPython_, 
            boost::process::std_err > stderr
        };

        sendDataThread_ = std::jthread{&ManagePythonProcess::sendDataToPython, this};
        getDataThread_ = std::jthread{&ManagePythonProcess::getDataFromPython, this};
    }

private:
    auto sendDataToPython() -> void {
        auto& cvContainer = container_.getCondVar();
        auto lockContainer = std::unique_lock<std::mutex>{container_.getMutex()};
        
        auto lastSentDataIndex = std::size_t{0};
        while (true) {
            cvContainer.wait(lockContainer, [this, &lastSentDataIndex]() {
                return (container_.getData().size() > lastSentDataIndex)
                        || !container_.willGetNewData();
            });

            auto& data = container_.getData();

            while (data.size() > lastSentDataIndex) {
                std::cout << "send " << data[lastSentDataIndex] << '\n';
                pipeToPython_ << data[lastSentDataIndex] << std::endl;
                lastSentDataIndex++;
            }

            if (!container_.willGetNewData()) {
                break;
            }
        }
    }

    auto getDataFromPython() -> void {
        auto prefictionSaveFile = std::fstream{FILE_PREDICTION_DATA, std::ios::app};

        auto pythonResponse = std::string{};
        while (std::getline(pipeFromPython_, pythonResponse)) {
            std::cout << "got " << pythonResponse << '\n';
            prefictionSaveFile << pythonResponse << std::endl;
        }
    }
};

int main() {
    
    auto goldPrices = PriceDataContainer{};
    
    auto readThread = ReadDataThread{goldPrices};

    auto manageDecisionPython = ManagePythonProcess{
        goldPrices, 
        "tradeDecision.py"
    };

    int stop{};
    std::cin >> stop;
    if (stop == -1) {
        readThread.stopThread();
    }
}
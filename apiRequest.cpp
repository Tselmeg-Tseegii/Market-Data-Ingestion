#include <iostream>
#include <cstdlib>
#include <string>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <boost/process.hpp>
#include <fstream>
#include <boost/beast.hpp>
#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>

#include "nlohmann/json.hpp"

#define CPPHTTPLIB_OPENSSL_SUPPORT
#include "httplib.h"

#include "timer.h"

#define API_REQUESTS_PER_MIN 7
#define API_REQUEST_INTERVAL_SEC 60
#define FILE_CANDLE_DATA "data/candleData.txt"
#define FILE_PREDICTION_DATA "data/predictionData.txt"

// class ReadDataWebSocket {
// private:
//     std::mutex stopSignalMutex_;
//     std::condition_variable cvSignalManager_;
//     bool stopReading_;
//     std::jthread thread_;

// public:
//     ReadDataWebSocket()
        
//     {
    
//     }

//     auto stopThread() -> void {
//         {
//             auto stopFlagLock = std::lock_guard<std::mutex>{stopSignalMutex_};
//             stopReading_ = true;
//         }
//         cvSignalManager_.notify_all();
//         thread_.join();
//     }
// private:

//     auto streamFromWebsocket() -> void {
//         auto host = std::string{"ws.twelvedata.com"};
//         auto MY_API_KEY = std::string{std::getenv("TWELVEDATA_MY_API_KEY")};
//         auto path = std::string{"/v1/quotes/price?apikey="} + MY_API_KEY;
//         auto port = std::string{"9443"};
//         auto subscription = std::string{R"(
//             {
//                 "action": "subscribe",
//                 "params": {
//                     "symbols": "AAPL,TRP,QQQ,EUR/USD,BTC/USD"
//                 }
//             }
//         )"};


//         auto ioContext = boost::asio::io_context{};
//         auto sslContext = boost::asio::ssl::context{
//             boost::asio::ssl::context::tlsv12_client
//         };

//         auto resolver = boost::asio::ip::tcp::resolver{ioContext};

//         auto webSocket = boost::beast::websocket::stream<
//             boost::asio::ssl::stream<
//                 boost::asio::ip::tcp::socket
//             >
//         >{ioContext, sslContext};

//         auto const result = resolver.resolve(host, port);

//         boost::asio::connect(
//             boost::beast::get_lowest_layer(webSocket), 
//             result
//         );

//         if (!SSL_set_tlsext_host_name(webSocket.next_layer().native_handle(), host.c_str())) {
//             std::cout << "error in the hostname stuff?\n";
//         }

//         webSocket.next_layer().handshake(boost::asio::ssl::stream_base::client);
//         webSocket.handshake(host, path);

//         webSocket.write(boost::asio::buffer(subscription));

//         auto streamBuffer = boost::beast::flat_buffer{};

//         while(webSocket.read(streamBuffer)) {
//             std::cout << boost::beast::make_printable(streamBuffer.data()) << std::endl;
//             streamBuffer.consume(streamBuffer.size());
//         }

//         webSocket.close(boost::beast::websocket::close_code::normal);
//     }

// };

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

class PriceCandleContainer {
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

    PriceCandleContainer& container_;

    httplib::Client apiClient_;
    std::string apiRequestEndPoint_;
    std::string apiPastRequestEndPoint_;

public:
    ReadDataThread(PriceCandleContainer& container)
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
        auto outputSize = std::string{"50"};
        auto order = std::string{"asc"};
        apiPastRequestEndPoint_ = {request + "?symbol=" + symbol + "&interval=" + interval + "&outputsize=" + outputSize + "&order=" + order + "&apikey=" + MY_API_KEY};
        
        thread_ = std::jthread{&ReadDataThread::onlineReadLoop, this};
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
    auto getStartUpData() -> void {
        auto latestCandle = getOneCandleRequest();
        auto latestOneMinTimeStamp = latestCandle.timeStamp;
        auto currTimeStamp = latestOneMinTimeStamp - 50 * 60;

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

    auto getOneCandleRequest() -> PriceCandle {
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

    auto onlineReadLoop() -> void {
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
};

class ManagePythonProcess {
private:
    PriceCandleContainer& container_;

    boost::process::opstream pipeToPython_;
    boost::process::ipstream pipeFromPython_;

    std::jthread sendDataThread_;
    std::jthread getDataThread_;

    boost::process::child pythonProcess_;

public:
    ManagePythonProcess(
        PriceCandleContainer& container,
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

    auto endProcess() -> void {
        sendDataThread_.join();
        getDataThread_.join();
        pythonProcess_.wait();
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
        pipeToPython_ << "STOP" << std::endl;
        pipeToPython_.close();
    }

    auto getDataFromPython() -> void {
        auto prefictionSaveFile = std::fstream{FILE_PREDICTION_DATA, std::ios::app};

        auto pythonResponse = std::string{};
        while (std::getline(pipeFromPython_, pythonResponse)) {
            std::cout << "got " << pythonResponse << '\n';
            prefictionSaveFile << pythonResponse << std::endl;
        }
        pipeFromPython_.close();
    }
};

int main() {
    
    auto goldPrices = PriceCandleContainer{};
    
    auto readThread = ReadDataThread{goldPrices};

    auto manageDecisionPython = ManagePythonProcess{
        goldPrices, 
        "tradeDecision.py"
    };

    int stop{};
    std::cin >> stop;
    if (stop == -1) {
        readThread.stopThread();
        manageDecisionPython.endProcess();
    }

}
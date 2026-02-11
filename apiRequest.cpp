#include <iostream>
#include <cstdlib>
#include <string>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <fstream>
#include <map>
#include <queue>

#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>

#include <boost/process.hpp>

#include <boost/beast.hpp>
#include <boost/beast/ssl.hpp>

#include "json.hpp"

#define CPPHTTPLIB_OPENSSL_SUPPORT
#include "httplib.h"

#include "timer.h"

#define API_REQUESTS_PER_MIN 7
#define API_REQUEST_INTERVAL_SEC 60
#define FILE_CANDLE_DATA "data/candleData.txt"
#define FILE_PREDICTION_DATA "data/predictionData.txt"

template<typename Event>
class WebSocketConnection {
private:
    boost::asio::io_context ioContext_;
    std::list<Event>& eventContainer_;

    std::string host_;
    std::string path_;
    std::string port_;

    std::thread readThread_;
    bool stopThread_;

public:
    WebSocketConnection(
        std::list<Event>& container,
        std::string host, 
        std::string path, 
        std::string port
    ) 
        : eventContainer_ {container}
        , host_ {std::move(host)}
        , path_ {std::move(path)}
        , port_ {std::move(port)}
    {   
        readThread_ = std::thread{&WebSocketConnection::readFromWebsocket, this};
    }

private:
    auto connectWebsocket(
        boost::asio::io_context& ioContext,
        std::string host, 
        std::string path, 
        std::string port
    ) -> boost::beast::websocket::stream<boost::asio::ssl::stream<boost::asio::ip::tcp::socket>> {
        auto sslContext = boost::asio::ssl::context{
            boost::asio::ssl::context::tlsv12_client
        };

        auto resolver = boost::asio::ip::tcp::resolver{ioContext};

        auto webSocket = boost::beast::websocket::stream<
            boost::asio::ssl::stream<
                boost::asio::ip::tcp::socket
            >
        >{ioContext, sslContext};

        auto const result = resolver.resolve(host, port);

        boost::asio::connect(
            boost::beast::get_lowest_layer(webSocket), 
            result
        );

        if (!SSL_set_tlsext_host_name(webSocket.next_layer().native_handle(), host.c_str())) {
            std::cout << "error in the hostname stuff?\n";
        }

        webSocket.next_layer().handshake(boost::asio::ssl::stream_base::client);
        webSocket.handshake(host, path);

        return webSocket;
    }

    auto readFromWebsocket() -> void {
        auto ioContext = boost::asio::io_context{};
        auto webSocket = connectWebsocket(ioContext, host_, path_, port_);

        auto streamBuffer = boost::beast::flat_buffer{};

        while (webSocket.read(streamBuffer)) {
            auto rawData = static_cast<char const*>(streamBuffer.data().data());
            auto length = streamBuffer.data().size();

            auto data = nlohmann::json::parse(rawData, rawData + length);

            auto event = createEventFromJson(data);
            
            events_.pushAndNotify(event);

            streamBuffer.consume(streamBuffer.size());
            if (stopThread_ == true) {
                break;
            }
        }
        try {
            webSocket.close(boost::beast::websocket::close_code::normal);
        } catch (const boost::system::system_error& err) {
            if (err.code() != boost::asio::ssl::error::stream_truncated) {
                std::cerr << "unexpected error on closing the websocket" << std::endl;
            }
        }
    }

};

struct IntPriceVolume {
    int price;
    double quantity;
};

class OrderBook {
    struct WebSocketEvent {
        long long int firstUpdateId;
        long long int lastUpdateId;
        std::vector<IntPriceVolume> bids;
        std::vector<IntPriceVolume> asks;

        WebSocketEvent() = default;
        
        WebSocketEvent(const WebSocketEvent&) = default;

        WebSocketEvent& operator=(const WebSocketEvent&) = default;

        WebSocketEvent& operator=(WebSocketEvent&&) = default;

        WebSocketEvent(const WebSocketEvent&& event) 
            : firstUpdateId {event.firstUpdateId}
            , lastUpdateId {event.lastUpdateId}
            , bids {std::move(event.bids)}
            , asks {std::move(event.asks)}
        {}
    };

    class EventQueue {
    private:
        std::mutex eventQueueMtx_;
        std::condition_variable eventQueueCv_;
        std::list<WebSocketEvent> eventQueue_;
        bool eventQueueIsNonEmpty_ {false};
    
    public:
        auto getCv() -> std::condition_variable& {
            return eventQueueCv_;
        }

        auto getMtx() -> std::mutex& {
            return eventQueueMtx_;
        }

        auto IsNotEmptyFlag() -> bool {
            return eventQueueIsNonEmpty_;
        }

        auto getFront() -> WebSocketEvent& {
            auto lock = std::lock_guard<std::mutex>{eventQueueMtx_};
            return eventQueue_.front();
        }

        auto pushAndNotify(WebSocketEvent& event) -> void {
            {
                auto lock = std::lock_guard<std::mutex>{eventQueueMtx_};
                eventQueue_.push_back(event);
                eventQueueIsNonEmpty_ = true;
            }
            eventQueueCv_.notify_all();
        }

        auto removeOldEvents(long long int orderBookLastUpdateId) {
            auto lock = std::lock_guard<std::mutex>{eventQueueMtx_};

            for (auto it {eventQueue_.begin()}; it != eventQueue_.end(); ) {
                if (it->lastUpdateId <= orderBookLastUpdateId) {
                    it = eventQueue_.erase(it);
                } else {
                    it++;
                }
            }
        }

        auto spliceTo(std::list<WebSocketEvent>& list) -> void {
            auto lock = std::lock_guard<std::mutex>{eventQueueMtx_};
            list.splice(list.begin(), eventQueue_);
        }

        auto clear() -> void {
            auto lock = std::lock_guard<std::mutex>{eventQueueMtx_};
            eventQueue_.clear();
            eventQueueIsNonEmpty_ = false;
        }

    };
private:
    long long int lastUpdateId_;
    std::map<int, double> asks_;
    std::map<int, double> bids_;
    std::mutex bookMtx_;

    EventQueue events_;

    std::atomic_bool stopReading_;

    std::thread webSocketThread_;
    std::thread updateOrderBookThread_;

public:
    OrderBook()
        : stopReading_ {false}
    {
        webSocketThread_ = std::thread{&OrderBook::readFromWebsocket, this};
        updateOrderBookThread_ = std::thread{&OrderBook::continualUpdate, this};
    }

    auto getOrderBook() -> std::pair<std::vector<IntPriceVolume>, std::vector<IntPriceVolume>> {
        auto lock = std::lock_guard{bookMtx_};

        auto askBook = std::vector<IntPriceVolume>{};
        askBook.reserve(asks_.size());
        auto bidBook = std::vector<IntPriceVolume>{};
        bidBook.reserve(bids_.size());
        for (auto it {asks_.begin()}; it != asks_.end(); it++) {
            askBook.push_back({it->first, it->second});
        }

        for (auto it {bids_.begin()}; it != bids_.end(); it++) {
            bidBook.push_back({it->first, it->second});
        }
        
        return {askBook, bidBook};
    }

    auto stopOrderBook() -> void {
        stopReading_ = true;
        events_.getCv().notify_all();

        webSocketThread_.join();
        updateOrderBookThread_.join();
    }

private:
    auto clearOrderBook() -> void {
        lastUpdateId_ = -1;
        asks_.clear();
        bids_.clear();
    }

    auto setOrderBrookFromJson(nlohmann::json& data) -> void {
        lastUpdateId_ = data["lastUpdateId"].get<long long int>();

        for (auto& elem : data["bids"]) {
            auto priceInt = static_cast<int>(std::stod(elem[0].get<std::string>()));
            auto volume = std::stod(elem[1].get<std::string>());

            bids_.emplace(priceInt, volume);
        }
        for (auto& elem : data["asks"]) {
            auto priceInt = static_cast<int>(std::stod(elem[0].get<std::string>()));
            auto volume = std::stod(elem[1].get<std::string>());

            asks_.emplace(priceInt, volume);
        }
    }

    auto createEventFromJson(nlohmann::json& data) -> WebSocketEvent {
        auto event = WebSocketEvent{};
        event.firstUpdateId = data["U"].get<long long int>();
        event.lastUpdateId = data["u"].get<long long int>();
        
        for (auto& elem : data["b"]) {
            auto priceInt = static_cast<int>(std::stod(elem[0].get<std::string>()));
            auto volume = std::stod(elem[1].get<std::string>());

            event.bids.push_back({priceInt, volume});
        }
        for (auto& elem : data["a"]) {
            auto priceInt = static_cast<int>(std::stod(elem[0].get<std::string>()) * 100);
            auto volume = std::stod(elem[1].get<std::string>());

            event.asks.push_back({priceInt, volume});
        }

        return event;
    }

    auto readFromWebsocket() -> void {
        auto ioContext = boost::asio::io_context{};
        auto webSocket = connectWebsocket(
            ioContext, 
            "stream.binance.com", 
            "/ws/btcusdt@depth@100ms", 
            "9443"
        );

        auto streamBuffer = boost::beast::flat_buffer{};

        while (webSocket.read(streamBuffer)) {
            auto rawData = static_cast<char const*>(streamBuffer.data().data());
            auto length = streamBuffer.data().size();

            auto data = nlohmann::json::parse(rawData, rawData + length);

            auto event = createEventFromJson(data);
            
            events_.pushAndNotify(event);

            streamBuffer.consume(streamBuffer.size());
            if (stopReading_ == true) {
                break;
            }
        }
        try {
            webSocket.close(boost::beast::websocket::close_code::normal);
        } catch (const boost::system::system_error& err) {
            if (err.code() != boost::asio::ssl::error::stream_truncated) {
                std::cerr << "unexpected error on closing the websocket" << std::endl;
            }
        }
    }

    auto updateOrderBookFromEvent(WebSocketEvent& currEvent) {
        auto lock = std::lock_guard{bookMtx_};
        // std::cout << "entered update" << std::endl;
        // std::cout << currEvent.asks.size() << std::endl;
        // std::cout << currEvent.bids.size() << std::endl;


        for (IntPriceVolume& elem : currEvent.asks) {
            if (elem.quantity != 0) {
                // std::cout << "updating asign ask" << std::endl;
                asks_.insert_or_assign(elem.price, elem.quantity);
            } else {
                // std::cout << "delete asign ask" << std::endl;
                asks_.erase(elem.price);
            }
        }

        for (IntPriceVolume& elem : currEvent.bids) {
            if (elem.quantity != 0) {
                // std::cout << "updating asign bid" << std::endl;

                bids_.insert_or_assign(elem.price, elem.quantity);
            } else {
                // std::cout << "delete asign bid" << std::endl;

                bids_.erase(elem.price);
            }
        }

        lastUpdateId_ = currEvent.lastUpdateId;
    }

    auto continualUpdate() -> void {
restart_update_orderbook_process:
        auto eventBufferLock = std::unique_lock<std::mutex>{events_.getMtx()};
        auto& eventBufferCv = events_.getCv();

        auto veryFirstUpdateId = int{};
        
        eventBufferCv.wait(eventBufferLock, [this]() {
            return events_.IsNotEmptyFlag() || stopReading_;
        });
        eventBufferLock.unlock();

        if (stopReading_) {
            return;
        }

        veryFirstUpdateId = events_.getFront().firstUpdateId;
        
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

        events_.removeOldEvents(snapshotLastUpdateId);

        {
            auto lock = std::lock_guard{bookMtx_};
            setOrderBrookFromJson(snapshotJson);
        }

        
        while (true) {
            auto currList = std::list<WebSocketEvent>{};
            eventBufferLock.lock();
            eventBufferCv.wait(eventBufferLock, [this]() {
                return events_.IsNotEmptyFlag() || stopReading_;
            });

            if (stopReading_) {
                return;
            }

            eventBufferLock.unlock();
            events_.spliceTo(currList);
            
            for (auto& currEvent : currList) {

                if (currEvent.lastUpdateId < lastUpdateId_) {
                    continue;
                }
                
                if (currEvent.firstUpdateId > (lastUpdateId_ + 1)) {
                    {
                        auto lock = std::lock_guard{bookMtx_};
                        clearOrderBook();
                    }
                    events_.clear();

                    goto restart_update_orderbook_process;
                }
                updateOrderBookFromEvent(currEvent);
            }
        }

    }
};

class TradeVolumeContainer {
private:
    std::mutex mtx_;
    std::map<int, double> tradeVolume_;

public:
    auto print() -> void {
        auto lock = std::lock_guard<std::mutex>{mtx_};
        for (auto& [price, vol] : tradeVolume_) {
            std::cout << '(' << price << ", " << vol << ")\n";
        }
    }

    auto push(double price, double volume) {
        auto lock = std::lock_guard<std::mutex>{mtx_};
        auto key = static_cast<int>(price * 100);

        auto foundIt = tradeVolume_.find(key);
        if (foundIt == tradeVolume_.end()) {
            tradeVolume_.emplace(key, volume);
        } else {
            foundIt->second += volume;
        }
    }

    auto getRange(double low, double high) -> std::vector<std::map<int, double>::iterator> {
        auto lock = std::lock_guard<std::mutex>{mtx_};
        auto volumeArr = std::vector<std::map<int, double>::iterator>{};
        auto firstElem = tradeVolume_.lower_bound(static_cast<int>(low));
        auto lastElem = tradeVolume_.upper_bound(static_cast<int>(high));
        for (auto curr {firstElem}; curr != lastElem; curr++) {
            volumeArr.emplace_back(curr);
        }
        volumeArr.emplace_back(lastElem);
        return volumeArr;
    }
};

class ReadDataWebSocket {
private:
    bool stopThread_ {false};
    TradeVolumeContainer& container_;
    std::thread streamThread_;

public:
    ReadDataWebSocket(TradeVolumeContainer& container)
        : container_ {container}   
    {
        streamThread_ = std::thread{
            &ReadDataWebSocket::streamFromWebsocket,
            this,
            std::ref(stopThread_),
            std::ref(container_)
        };
    }

    auto stopThread() -> void {
        stopThread_ = true;
        streamThread_.join();
    }
private:

    auto streamFromWebsocket(bool& stopThread, TradeVolumeContainer& container) -> void {
        

        
        auto host = std::string{"stream.binance.com"};
        
        auto path = std::string{"/ws/btcusdt@trade"};
        auto port = std::string{"9443"};

        auto ioContext = boost::asio::io_context{};
        auto sslContext = boost::asio::ssl::context{
            boost::asio::ssl::context::tlsv12_client
        };

        auto resolver = boost::asio::ip::tcp::resolver{ioContext};

        auto webSocket = boost::beast::websocket::stream<
            boost::asio::ssl::stream<
                boost::asio::ip::tcp::socket
            >
        >{ioContext, sslContext};

        auto const result = resolver.resolve(host, port);

        boost::asio::connect(
            boost::beast::get_lowest_layer(webSocket), 
            result
        );

        if (!SSL_set_tlsext_host_name(webSocket.next_layer().native_handle(), host.c_str())) {
            std::cout << "error in the hostname stuff?\n";
        }

        webSocket.next_layer().handshake(boost::asio::ssl::stream_base::client);
        webSocket.handshake(host, path);

        auto streamBuffer = boost::beast::flat_buffer{};

        while(webSocket.read(streamBuffer)) {
            auto rawData = static_cast<char const*>(streamBuffer.data().data());
            auto length = streamBuffer.data().size();

            auto data = nlohmann::json::parse(rawData, rawData + length);

            auto price = std::stod(data["p"].get<std::string>());
            auto volume = std::stod(data["q"].get<std::string>());
            container.push(price, volume);

            streamBuffer.consume(streamBuffer.size());
            
            if (stopThread == true) {
                break;
            }
        }

        try {
            webSocket.close(boost::beast::websocket::close_code::normal);
        } catch (const boost::system::system_error& err) {
            if (err.code() != boost::asio::ssl::error::stream_truncated) {
                std::cerr << "unexpected error on closing the websocket" << std::endl;
            }
        }
    }
};

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
    std::thread thread_;

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
        
        thread_ = std::thread{&ReadDataThread::onlineReadLoop, this};
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

    std::thread sendDataThread_;
    std::thread getDataThread_;

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
            boost::process::search_path("python3.13"),
            "-u",
            pythonFile,
            boost::process::std_in < pipeToPython_,
            boost::process::std_out > pipeFromPython_, 
            boost::process::std_err > stderr
        };

        sendDataThread_ = std::thread{&ManagePythonProcess::sendDataToPython, this};
        getDataThread_ = std::thread{&ManagePythonProcess::getDataFromPython, this};
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
    
    // auto goldPrices = PriceCandleContainer{};
    
    // auto readThread = ReadDataThread{goldPrices};

    // auto manageDecisionPython = ManagePythonProcess{
    //     goldPrices, 
    //     "tradeDecision.py"
    // };

    // auto btcVolume = TradeVolumeContainer{};
    // auto webSocketThread = ReadDataWebSocket{btcVolume};

    auto orderBook = OrderBook{};

    std::this_thread::sleep_for(std::chrono::milliseconds(2000));


    auto [ask, bid] = orderBook.getOrderBook();

    for (auto& [price, vol] : ask) {
        std::cout << price << " - " << vol << '\n';
    }

    for (auto& [price, vol] : bid) {
        std::cout << price << " - " << vol << '\n';
    }

    std::cout << "sleepign again" << std::endl;
    std::this_thread::sleep_for(std::chrono::milliseconds(2000));

    std::cout << "------------------" << std::endl;
    std::cout << "two second later" << std::endl;

    auto tempBook = orderBook.getOrderBook();
    ask = tempBook.first;
    bid = tempBook.second;

    for (auto& [price, vol] : ask) {
        std::cout << price << " - " << vol << '\n';
    }

    for (auto& [price, vol] : bid) {
        std::cout << price << " - " << vol << '\n';
    }

    int stop{};
    std::cin >> stop;
    if (stop == -1) {
        // readThread.stopThread();
        // manageDecisionPython.endProcess();

        // webSocketThread.stopThread();

        orderBook.stopOrderBook();

    }
    // orderBook.getOrderBook();
    // btcVolume.print();
}
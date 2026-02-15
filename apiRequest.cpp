#include <iostream>
#include <cstdlib>
#include <string>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <fstream>
#include <map>
#include <queue>
#include <charconv>
#include <memory>

#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>

#include <boost/process.hpp>

#include <boost/beast.hpp>
#include <boost/beast/ssl.hpp>

#include "json.hpp"

#include "simdjson/simdjson.h"

#define CPPHTTPLIB_OPENSSL_SUPPORT
#include "httplib.h"

#include "timer.h"

#define API_REQUESTS_PER_MIN 7
#define API_REQUEST_INTERVAL_SEC 60
#define FILE_CANDLE_DATA "data/candleData.txt"
#define FILE_PREDICTION_DATA "data/predictionData.txt"

struct RawEvent {
    boost::beast::flat_buffer data_;

    RawEvent() = default;

    RawEvent(boost::beast::flat_buffer&& data)
        : data_ {std::move(data)}
    {}
};

template<typename Event>
class EventQueue {
private:
    std::mutex eventQueueMtx_;
    std::condition_variable eventQueueCv_;
    std::vector<Event> buffer_;
 
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

    auto getFront() -> Event& {
        auto lock = std::lock_guard<std::mutex>{eventQueueMtx_};
        return buffer_.front();
    }

    auto pushAndNotify(boost::beast::flat_buffer&& data) -> void {
        {
            auto lock = std::lock_guard<std::mutex>{eventQueueMtx_};
            buffer_.emplace_back(std::move(data));
            eventQueueIsNonEmpty_ = true;
        }
        eventQueueCv_.notify_all();
    }

    auto getData() -> std::vector<Event>& {
        return buffer_;
    }

    auto spliceTo(EventQueue<Event>& queue) -> void {
        auto lock = std::lock_guard<std::mutex>{eventQueueMtx_};
        buffer_.swap(queue.getData());
        eventQueueIsNonEmpty_ = false;
    }

    auto clear() -> void {
        auto lock = std::lock_guard<std::mutex>{eventQueueMtx_};
        buffer_.clear();
        eventQueueIsNonEmpty_ = false;
    }
};

template<typename Event>
class WebSocketConnection {
private:
    boost::asio::io_context ioContext_;
    EventQueue<Event>& eventQueue_;

    std::string host_;
    std::string path_;
    std::string port_;

    std::thread readThread_;
    bool stopThread_;

public:
    WebSocketConnection(
        EventQueue<Event>& container,
        std::string host, 
        std::string path, 
        std::string port
    ) 
        : eventQueue_ {container}
        , host_ {std::move(host)}
        , path_ {std::move(path)}
        , port_ {std::move(port)}
        , stopThread_ {false}
    {   
        readThread_ = std::thread{&WebSocketConnection::readFromWebsocket, this};
    }

    auto stopThread() -> void {
        stopThread_ = true;
        readThread_.join();
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

        auto jsonParser = simdjson::ondemand::parser{};
        while (true) {
            auto streamBuffer = boost::beast::flat_buffer{};

            webSocket.read(streamBuffer);

            if (streamBuffer.capacity() < streamBuffer.size() + simdjson::SIMDJSON_PADDING) {
                streamBuffer.reserve(streamBuffer.size() + simdjson::SIMDJSON_PADDING);
            }

            eventQueue_.pushAndNotify(std::move(streamBuffer));

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
    int price_;
    double volume_;

    IntPriceVolume() = default;
    IntPriceVolume(int price, double volume)
        : price_ {price}
        , volume_ {volume}
    {}

    auto operator<(const IntPriceVolume& other) -> bool {
        return price_ < other.price_;
    }

    auto operator<(int priceKey) -> bool {
        return price_ < priceKey;
    }
};

struct OrderBookWebSocketEvent: RawEvent {
    std::unique_ptr<simdjson::ondemand::parser> parser_;
    simdjson::ondemand::document doc_;
    bool parserIsActive_;

    long long int firstUpdateId_ {-1};
    long long int lastUpdateId_ {-1};

    OrderBookWebSocketEvent() = default;

    OrderBookWebSocketEvent(boost::beast::flat_buffer&& data)
        : RawEvent(std::move(data))
    {}

    auto initialiseParser() -> void {
        parser_ = std::make_unique<simdjson::ondemand::parser>();
        auto rawDataPtr = static_cast<char const*>(data_.data().data());

        auto errors = parser_->iterate(
            simdjson::padded_string_view(rawDataPtr, data_.size(), data_.capacity())
        ).get(doc_);

        parserIsActive_ = true;
    }

    auto getFirstUpdateId() -> long long int {
        if (firstUpdateId_ == -1) {
            if (!parserIsActive_) {
                this->initialiseParser();
            }
            firstUpdateId_ = doc_["U"].get<long long int>().value();
        }
        return firstUpdateId_;
    }

    auto getLastUpdateId() -> long long int {
        if (lastUpdateId_ == -1) {
            this->getFirstUpdateId();
            lastUpdateId_ = doc_["u"].get<long long int>().value();
        }
        return lastUpdateId_;
    }
};

class OrderBookContainer {
private:
    long long int lastUpdateId_ {-1};
    std::map<int, double> asks_;
    std::map<int, double> bids_;
    std::mutex mtx_;

public:
    auto getLastUpdateId() -> long long int {
        auto lock = std::lock_guard{mtx_};
        return lastUpdateId_;
    }
    auto getMtx() -> std::mutex& {
        return mtx_;
    }

    auto getOrderBook() -> std::pair<std::vector<IntPriceVolume>, std::vector<IntPriceVolume>> {
        auto lock = std::lock_guard{mtx_};

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

    auto print() -> void {
        auto [ask, bid] = getOrderBook();
        std::cout << "OrderBook\n";

        std::cout << "Asks\n";
        for (auto& [price, vol] : ask) {
            std::cout << price << " - " << vol << '\n';
        }

        std::cout << "Bids\n";
        for (auto& [price, vol] : bid) {
            std::cout << price << " - " << vol << '\n';
        }
    }

    auto clearOrderBook() -> void {
        auto lock = std::lock_guard{mtx_};
        lastUpdateId_ = -1;
        asks_.clear();
        bids_.clear();
    }

    auto setOrderBrookFromJson(nlohmann::json& data) -> void {
        auto lock = std::lock_guard{mtx_};

        lastUpdateId_ = data["lastUpdateId"].get<long long int>();

        for (auto& elem : data["bids"]) {
            auto priceInt = static_cast<int>(std::stod(elem[0].get<std::string>()) * 100);
            auto volume = std::stod(elem[1].get<std::string>());

            bids_.emplace(priceInt, volume);
        }
        for (auto& elem : data["asks"]) {
            auto priceInt = static_cast<int>(std::stod(elem[0].get<std::string>()) * 100);
            auto volume = std::stod(elem[1].get<std::string>());

            asks_.emplace(priceInt, volume);
        }
    }

    auto updateOrderBookFromEvent(OrderBookWebSocketEvent& currEvent) {
        for (auto elem : currEvent.doc_["b"]) {
            auto it = elem.begin();
            auto priceDouble = double{};
            auto res = (*it).get_double_in_string().get(priceDouble);

            auto priceInt = static_cast<int>(priceDouble * 100);

            ++it;

            auto volume = double{};
            res = (*it).get_double_in_string().get(volume);

            if (volume != 0) {
                bids_.insert_or_assign(priceInt, volume);
            } else {
                bids_.erase(priceInt);
            }
        }

        for (auto elem : currEvent.doc_["a"]) {
            auto it = elem.begin();
            auto priceDouble = double{};
            auto res = (*it).get_double_in_string().get(priceDouble);

            auto priceInt = static_cast<int>(priceDouble * 100);

            ++it;

            auto volume = double{};
            res = (*it).get_double_in_string().get(volume);

            if (volume != 0) {
                bids_.insert_or_assign(priceInt, volume);
            } else {
                bids_.erase(priceInt);
            }
        }

        lastUpdateId_ = currEvent.getLastUpdateId();
    }
};

class OrderBookUpdater {
private:
    OrderBookContainer& container_;
    EventQueue<OrderBookWebSocketEvent>& eventsQueue_;

    std::atomic_bool stopUpdate_;

    std::thread webSocketThread_;
    std::thread updateOrderBookThread_;

public:
    OrderBookUpdater(OrderBookContainer& container, EventQueue<OrderBookWebSocketEvent>& events)
        : container_ {container}
        , eventsQueue_ {events}
        , stopUpdate_ {false}
    {
        updateOrderBookThread_ = std::thread{&OrderBookUpdater::continualUpdate, this};
    }

    auto stopUpdate() -> void {
        stopUpdate_ = true;
        eventsQueue_.getCv().notify_all();

        updateOrderBookThread_.join();
    }

private:

    auto continualUpdate() -> void {
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

        auto& firstEvent = eventsQueue_.getFront();
        auto veryFirstUpdateId = firstEvent.getFirstUpdateId();
        
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

        auto currList = EventQueue<OrderBookWebSocketEvent>{};
        eventsQueue_.spliceTo(currList);
        auto& currListData = currList.getData();
        for (auto it {currListData.begin()}; it != currListData.end(); ) {
            auto currLastUpdateId = it->getLastUpdateId();
            if (currLastUpdateId <= snapshotLastUpdateId) {
                it = currListData.erase(it);
            } else {
                it++;
            }
        }

        container_.setOrderBrookFromJson(snapshotJson);

        for (auto& currEvent : currListData) {
            auto currOrderBookLastUpdateId = container_.getLastUpdateId();

            if (currEvent.getFirstUpdateId() > (currOrderBookLastUpdateId + 1)) {
                container_.clearOrderBook();
                eventsQueue_.clear();

                goto restart_update_orderbook_process;
            }
            
            container_.updateOrderBookFromEvent(currEvent);
        }
      
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

                if (currEvent.getLastUpdateId() < currOrderBookLastUpdateId) {
                    continue;
                }

                if (currEvent.getFirstUpdateId() > (currOrderBookLastUpdateId + 1)) {
                    container_.clearOrderBook();
                    eventsQueue_.clear();

                    goto restart_update_orderbook_process;
                }
                
                container_.updateOrderBookFromEvent(currEvent);
            }
        }

    }
};

class FlatContainer {
private:
    std::vector<IntPriceVolume> data_;

public:
    auto insertOrUpdate(int price, double volume) {
        auto it = std::lower_bound(data_.begin(), data_.end(), price, [](const IntPriceVolume& elem, int priceKey) {
            return elem.price_ < priceKey;
        });

        if (it != data_.end() && it->price_ == price) {
            (it->volume_) += volume;
        } else {
            data_.insert(it, {price, volume});
        }
    }
};

class TradeVolumeContainer {
private:
    std::mutex mtx_;
    FlatContainer tradeVolume_;

public:
    // auto print() -> void {
    //     auto lock = std::lock_guard<std::mutex>{mtx_};
    //     for (auto& [price, vol] : tradeVolume_) {
    //         std::cout << '(' << price << ", " << vol << ")\n";
    //     }
    // }

    auto updateFromEvent(simdjson::ondemand::document& data) {
        auto price = static_cast<int>(data["p"].get_double_in_string().value() * 100);
        auto volume = data["q"].get_double_in_string().value();
        
        tradeVolume_.insertOrUpdate(price, volume);
    }
};

class TradeVolumeUpdater {
private:
    TradeVolumeContainer& container_;
    EventQueue<RawEvent>& queue_;

    std::thread updateThread_;
    bool stopThread_;

public:
    TradeVolumeUpdater(TradeVolumeContainer& container, EventQueue<RawEvent>& queue)
        : container_ {container}
        , queue_ {queue}
        , stopThread_ {false}
    {
        updateThread_ = std::thread{&TradeVolumeUpdater::updateFromEventQueue, this};
    }

    auto stopThread() -> void {
        stopThread_ = true;
        queue_.getCv().notify_all();
        updateThread_.join();
    }
private:
    auto updateFromEventQueue() -> void {
        auto& eventQueueCv = queue_.getCv();
        auto eventQueueLock = std::unique_lock{queue_.getMtx()};
        eventQueueLock.unlock();

        auto jsonParser = simdjson::ondemand::parser{};
        while (true) {
            auto currEvents = EventQueue<RawEvent>{};

            eventQueueLock.lock();
            eventQueueCv.wait(eventQueueLock, [this] () {
                return queue_.IsNotEmptyFlag() || stopThread_;
            });

            if (stopThread_) {
                return;
            }
            
            eventQueueLock.unlock();

            queue_.spliceTo(currEvents);
            

            for (auto& currEvent : currEvents.getData()) {
                if (stopThread_) {
                    return;
                }
                auto doc = simdjson::ondemand::document{};

                auto& currEventBuffer = currEvent.data_;

                auto currEventDataPtr = static_cast<char const*>(currEventBuffer.data().data());

                auto errors = jsonParser.iterate(
                    simdjson::padded_string_view(currEventDataPtr, currEventBuffer.size(), currEventBuffer.capacity())
                ).get(doc);

                container_.updateFromEvent(doc);
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

    auto btcVolume = TradeVolumeContainer{};
    auto btcVolumeEventQueue = EventQueue<RawEvent>{};
    auto btcVolumeEventWebScoket = WebSocketConnection<RawEvent>{
        btcVolumeEventQueue,
        "stream.binance.com",
        "/ws/btcusdt@trade",
        "9443"
    };
    auto btcVolumeUpdater = TradeVolumeUpdater{btcVolume, btcVolumeEventQueue};

    // auto orderBook = OrderBookContainer{};
    // auto orderBookEventQueue = EventQueue<OrderBookWebSocketEvent>{};
    // auto orderBookWebSocket = WebSocketConnection<OrderBookWebSocketEvent>{
    //     orderBookEventQueue,
    //     "stream.binance.com",
    //     "/ws/btcusdt@depth@100ms",
    //     "9443"
    // };
    // auto orderBookUpdater = OrderBookUpdater{orderBook, orderBookEventQueue};

    // std::this_thread::sleep_for(std::chrono::milliseconds(2000));

    // orderBook.print();

    // std::this_thread::sleep_for(std::chrono::milliseconds(2000));

    // orderBook.print();

    int stop{};
    std::cin >> stop;
    if (stop == -1) {
        // readThread.stopThread();
        // manageDecisionPython.endProcess();

        btcVolumeEventWebScoket.stopThread();
        btcVolumeUpdater.stopThread();

        // orderBookWebSocket.stopThread();
        // orderBookUpdater.stopUpdate();
    }
    // btcVolume.print();
    // orderBook.getOrderBook();
    // btcVolume.print();
}
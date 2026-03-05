#include <iostream>
#include <fstream>
#include <iterator>
#include <filesystem>
#include <atomic>
#include <thread>

#include "shared/marketDataTypes.hpp"
#include "shared/eventQueue.hpp"
#include "shared/eventQueueDispatcher.hpp"

#include "network/webSocketConnection.hpp"
#include "tradeVolume/tradeVolumeContainer.hpp"
#include "tradeVolume/tradeVolumeUpdater.hpp"
#include "tradeVolume/tradeVolumeWebSocketEvent.hpp"

#include "orderBook/orderBookWebSocketEvent.hpp"
#include "orderBook/orderBookContainer.hpp"
#include "orderBook/orderBookUpdater.hpp"

#include "priceCandle/priceCandleContainer.hpp"
#include "priceCandle/readDataThread.hpp"
#include "shared/managePythonProcess.hpp"

#include "external/httplib.h"          // simple embedded HTTP server/client
#include "web/webInterface.hpp"        // conversions for website

using namespace MarketData;

int main() {
    // wire up both order book and trade volume feeds; the web endpoints below
    // will query these containers for the "top 20" / "last 100" snapshots.

    auto orderBook = OrderBookContainer{};
    auto orderBookEventQueue = EventQueue<OrderBookWebSocketEvent>{};
    auto orderBookWebSocket = WebSocketConnection<OrderBookWebSocketEvent>{
        orderBookEventQueue,
        "stream.binance.com",
        "/ws/btcusdt@depth@100ms",
        "9443"
    };
    auto orderBookUpdater = OrderBookUpdater{orderBook, orderBookEventQueue};

    auto btcVolume = TradeVolumeContainer{};
    auto btcVolumeEventQueueContainer = EventQueue<TradeVolumeWebSocketEvent>{};

    auto btcVolumeMainEventQueue = EventQueue<TradeVolumeWebSocketEvent>{};
    auto btcVolumeEventQueueDispatch = EventQueueDispatcher<TradeVolumeWebSocketEvent>{btcVolumeMainEventQueue};
    btcVolumeEventQueueDispatch.addConsumerQueue(btcVolumeEventQueueContainer);

    auto btcVolumeEventWebScoket = WebSocketConnection<TradeVolumeWebSocketEvent>{
        btcVolumeMainEventQueue,
        "stream.binance.com",
        "/ws/btcusdt@trade",
        "9443"
    };
    auto btcVolumeUpdater = TradeVolumeUpdater{btcVolume, btcVolumeEventQueueContainer};

    auto btcVolumePythonQueue = EventQueue<TradeVolumeWebSocketEvent>{};
    btcVolumeEventQueueDispatch.addConsumerQueue(btcVolumePythonQueue);

    // hold the active python bridge so that handlers can restart it
    std::unique_ptr<ManagePythonProcess<TradeVolumeWebSocketEvent>> manageDecisionPython;

    // --- http server -------------------------------------------------------
    httplib::Server svr;
    // allow CORS so that the dashboard can be opened via file:// or another host
    svr.set_default_headers({
        {"Access-Control-Allow-Origin", "*"},
        {"Access-Control-Allow-Methods", "GET, POST, OPTIONS"},
        {"Access-Control-Allow-Headers", "Content-Type"}
    });

    svr.Get("/orderbook", [&](const httplib::Request&, httplib::Response& res) {
        std::cout << "HTTP GET /orderbook\n";
        res.set_content(Web::orderBookToJson(orderBook, 10).dump(), "application/json");
    });

    svr.Get("/trades", [&](const httplib::Request&, httplib::Response& res) {
        std::cout << "HTTP GET /trades\n";
        res.set_content(Web::recentTradesToJson(btcVolume, 20).dump(), "application/json");
    });

    svr.Post("/runscript", [&](const httplib::Request& req, httplib::Response& res) {
        std::cout << "HTTP POST /runscript\n";
        try {
            // ensure pythonScript directory exists
            std::filesystem::create_directories("pythonScript");
            
            std::string tmpFile = "pythonScript/user_script.py";
            {
                std::ofstream out(tmpFile);
                out << req.body;
            }
            if (manageDecisionPython) {
                manageDecisionPython->endProcess();
            }
            manageDecisionPython = std::make_unique<ManagePythonProcess<TradeVolumeWebSocketEvent>>(btcVolumePythonQueue, tmpFile, "data/predictionData.txt");
            res.set_content("started", "text/plain");
        } catch (const std::exception& e) {
            res.set_content(std::string("error: ") + e.what(), "text/plain");
            res.status = 500;
        }
    });

    svr.Post("/stop", [&](const httplib::Request&, httplib::Response& res) {
        std::cout << "HTTP POST /stop\n";
        if (manageDecisionPython) {
            manageDecisionPython->endProcess();
            manageDecisionPython.reset();
        }
        res.set_content("stopped", "text/plain");
    });

    svr.Get("/scriptOutput", [&](const httplib::Request&, httplib::Response& res) {
        std::cout << "HTTP GET /scriptOutput\n";
        std::ifstream in("data/predictionData.txt");
        std::string content((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
        res.set_content(content, "text/plain");
    });

    // serve the dashboard HTML file
    svr.Get("/", [](const httplib::Request&, httplib::Response& res) {
        std::ifstream in("index.html");
        if (in) {
            std::string content((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
            res.set_content(content, "text/html");
        } else {
            res.set_content("index.html not found", "text/plain");
            res.status = 404;
        }
    });

    // flag used by several handlers and threads
    std::atomic<bool> running{true};

    // allow external requests to shut down the server
    svr.Post("/shutdown", [&](const httplib::Request&, httplib::Response& res) {
        std::cout << "HTTP POST /shutdown\n";
        res.set_content("shutting down", "text/plain");
        running = false;
        svr.stop();
    });

    std::thread serverThread([&]{ svr.listen("0.0.0.0", 8080); });

    // also accept -1 on stdin, but do it in a separate thread so the main
    // event loop isn't blocked (some environments don't have a console).
    std::thread inputThread([&](){
        int stop;
        while (running && std::cin >> stop) {
            if (stop == -1) {
                running = false;
                svr.stop();
                break;
            }
        }
    });

    // wait for shutdown signal
    while (running) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    // cleanup same as before
    btcVolumeEventQueueDispatch.stopConsumerQueue(btcVolumePythonQueue);
    if (manageDecisionPython) {
        manageDecisionPython->endProcess();
    }

    btcVolumeUpdater.stopThread();
    btcVolumeEventWebScoket.stopThread();
    btcVolumeEventQueueDispatch.stop();

    orderBookWebSocket.stopThread();
    orderBookUpdater.stopUpdate();

    serverThread.join();
    inputThread.join();
}

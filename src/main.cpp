#include <iostream>
#include <fstream>
#include <iterator>

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

    svr.Get("/orderbook", [&](const httplib::Request&, httplib::Response& res) {
        res.set_content(Web::orderBookToJson(orderBook, 20).dump(), "application/json");
    });

    svr.Get("/trades", [&](const httplib::Request&, httplib::Response& res) {
        res.set_content(Web::recentTradesToJson(btcVolume, 100).dump(), "application/json");
    });

    svr.Post("/runscript", [&](const httplib::Request& req, httplib::Response& res) {
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
    });

    svr.Post("/stop", [&](const httplib::Request&, httplib::Response& res) {
        if (manageDecisionPython) {
            manageDecisionPython->endProcess();
            manageDecisionPython.reset();
        }
        res.set_content("stopped", "text/plain");
    });

    svr.Get("/scriptOutput", [&](const httplib::Request&, httplib::Response& res) {
        std::ifstream in("data/predictionData.txt");
        std::string content((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
        res.set_content(content, "text/plain");
    });

    std::thread serverThread([&]{ svr.listen("0.0.0.0", 8080); });

    // make shutdown identical to prior behaviour
    int stop{};
    std::cin >> stop;
    if (stop == -1) {
        btcVolumeEventQueueDispatch.stopConsumerQueue(btcVolumePythonQueue);
        if (manageDecisionPython) {
            manageDecisionPython->endProcess();
        }

        btcVolumeUpdater.stopThread();
        btcVolumeEventWebScoket.stopThread();
        btcVolumeEventQueueDispatch.stop();

        orderBookWebSocket.stopThread();
        orderBookUpdater.stopUpdate();
    }

    serverThread.join();
}

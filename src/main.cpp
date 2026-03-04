#include <iostream>

#include "shared/marketDataTypes.hpp"

#include "network/webSocketConnection.hpp"
#include "network/dashboardServer.hpp"

#include "tradeVolume/tradeVolumeContainer.hpp"
#include "tradeVolume/tradeVolumeUpdater.hpp"
#include "tradeVolume/tradeVolumeWebSocketEvent.hpp"

#include "orderBook/orderBookWebSocketEvent.hpp"
#include "orderBook/orderBookContainer.hpp"
#include "orderBook/orderBookUpdater.hpp"

#include "priceCandle/priceCandleContainer.hpp"
#include "priceCandle/readDataThread.hpp"
#include "shared/managePythonProcess.hpp"

using namespace MarketData;

int main() {
    
    auto goldPrices = PriceCandleContainer{};
    
    auto readThread = ReadDataThread{goldPrices};

    auto manageDecisionPython = ManagePythonProcess{
        goldPrices, 
        "pythonScript/tradeDecision.py",
        "data/predictionData.txt"
    };

    auto btcVolume = TradeVolumeContainer{};
    auto btcVolumeEventQueue = EventQueue<TradeVolumeWebSocketEvent>{};
    auto btcVolumeEventWebScoket = WebSocketConnection<TradeVolumeWebSocketEvent>{
        btcVolumeEventQueue,
        "stream.binance.com",
        "/ws/btcusdt@trade",
        "9443"
    };
    auto btcVolumeUpdater = TradeVolumeUpdater{btcVolume, btcVolumeEventQueue};


    auto orderBook = OrderBookContainer{};
    auto orderBookEventQueue = EventQueue<OrderBookWebSocketEvent>{};
    auto orderBookWebSocket = WebSocketConnection<OrderBookWebSocketEvent>{
        orderBookEventQueue,
        "stream.binance.com",
        "/ws/btcusdt@depth@100ms",
        "9443"
    };
    auto orderBookUpdater = OrderBookUpdater{orderBook, orderBookEventQueue};

    auto dashBoard = DashboardServer{&orderBook};
    dashBoard.start(8080);

    int stop{};
    std::cin >> stop;
    if (stop == -1) {
        // readThread.stopThread();
        // manageDecisionPython.endProcess();

        // btcVolumeEventWebScoket.stopThread();
        // btcVolumeUpdater.stopThread();

        orderBookWebSocket.stopThread();
        orderBookUpdater.stopUpdate();
    }
    // orderBook.print();

    // btcVolume.print();
}
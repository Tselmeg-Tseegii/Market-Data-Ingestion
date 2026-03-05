#include <iostream>

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

using namespace MarketData;

int main() {
    
    // auto goldPrices = PriceCandleContainer{};
    
    // auto readThread = ReadDataThread{goldPrices};

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

    auto manageDecisionPython = ManagePythonProcess{
        btcVolumePythonQueue, 
        "pythonScript/echoInput.py",
        "data/predictionData.txt"
    };


    // auto orderBook = OrderBookContainer{};
    // auto orderBookEventQueue = EventQueue<OrderBookWebSocketEvent>{};
    // auto orderBookWebSocket = WebSocketConnection<OrderBookWebSocketEvent>{
    //     orderBookEventQueue,
    //     "stream.binance.com",
    //     "/ws/btcusdt@depth@100ms",
    //     "9443"
    // };
    // auto orderBookUpdater = OrderBookUpdater{orderBook, orderBookEventQueue};

    int stop{};
    std::cin >> stop;
    if (stop == -1) {
        // readThread.stopThread();
        // manageDecisionPython.endProcess();

        btcVolumeEventWebScoket.stopThread();
        btcVolumeUpdater.stopThread();

    //     orderBookWebSocket.stopThread();
    //     orderBookUpdater.stopUpdate();
    }
    // orderBook.print();

    // btcVolume.print();
}
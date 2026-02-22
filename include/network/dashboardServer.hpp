#pragma once

#include <thread>
#include <fstream>
#include <sstream>
#include <vector>
#include <memory>
#include "external/httplib.h"
#include "external/nlohmann/json.hpp"
#include "orderBook/orderBookContainer.hpp"
#include "shared/marketDataTypes.hpp"

namespace MarketData {

class DashboardServer {
private:
    std::unique_ptr<httplib::Server> server_;
    std::thread serverThread_;
    OrderBookContainer* orderBook_;
    const std::string predictionFilePath_;
    bool running_{false};

    auto getOrderBookJson() -> nlohmann::json {
        auto [asks, bids] = orderBook_->getOrderBook();
        
        nlohmann::json response;
        
        // Top 50 asks
        response["asks"] = nlohmann::json::array();
        size_t askCount = std::min(size_t(50), asks.size());
        for (size_t i = 0; i < askCount; ++i) {
            response["asks"].push_back({
                {"price", asks[i].intPrice},
                {"volume", asks[i].intVolume}
            });
        }
        
        // Top 50 bids
        response["bids"] = nlohmann::json::array();
        size_t bidCount = std::min(size_t(50), bids.size());
        for (size_t i = 0; i < bidCount; ++i) {
            response["bids"].push_back({
                {"price", bids[i].intPrice},
                {"volume", bids[i].intVolume}
            });
        }
        
        response["timestamp"] = std::time(nullptr);
        response["lastUpdateId"] = orderBook_->getLastUpdateId();
        
        return response;
    }

    auto getPredictionsText() -> std::string {
        std::ifstream file(predictionFilePath_);
        if (!file.is_open()) {
            return "No prediction data available yet.";
        }
        
        std::vector<std::string> lines;
        std::string line;
        while (std::getline(file, line)) {
            lines.push_back(line);
        }
        file.close();
        
        // Return last 20 predictions
        std::stringstream ss;
        size_t start = lines.size() > 20 ? lines.size() - 20 : 0;
        for (size_t i = start; i < lines.size(); ++i) {
            ss << lines[i] << "\n";
        }
        
        return ss.str();
    }

    auto getPredictionsJson() -> nlohmann::json {
        std::ifstream file(predictionFilePath_);
        nlohmann::json predictions = nlohmann::json::array();
        
        if (!file.is_open()) {
            return predictions;
        }
        
        std::string line;
        while (std::getline(file, line)) {
            // Parse "prediction: (timestamp, direction, confidence)"
            predictions.push_back(line);
        }
        file.close();
        
        return predictions;
    }

    auto getHealthJson() -> nlohmann::json {
        auto [asks, bids] = orderBook_->getOrderBook();
        
        nlohmann::json health;
        health["status"] = "online";
        health["orderbook_asks_size"] = asks.size();
        health["orderbook_bids_size"] = bids.size();
        health["timestamp"] = std::time(nullptr);
        
        return health;
    }

public:
    DashboardServer(OrderBookContainer* orderBook, 
                    const std::string& predictionFile = "data/predictionData.txt")
        : orderBook_(orderBook), predictionFilePath_(predictionFile) {
        
        server_ = std::make_unique<httplib::Server>();
        
        // Enable CORS headers
        server_->set_post_routing_handler([](const httplib::Request&, httplib::Response& res) {
            res.set_header("Access-Control-Allow-Origin", "*");
            res.set_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
            res.set_header("Access-Control-Allow-Headers", "Content-Type");
        });
        
        // Health check endpoint
        server_->Get("/api/health", [this](const httplib::Request&, httplib::Response& res) {
            auto json = getHealthJson();
            res.set_content(json.dump(2), "application/json");
        });
        
        // Orderbook endpoint - JSON
        server_->Get("/api/orderbook", [this](const httplib::Request&, httplib::Response& res) {
            auto json = getOrderBookJson();
            res.set_content(json.dump(2), "application/json");
        });
        
        // Predictions endpoint - JSON
        server_->Get("/api/predictions", [this](const httplib::Request&, httplib::Response& res) {
            auto json = getPredictionsJson();
            res.set_content(json.dump(2), "application/json");
        });
        
        // Predictions endpoint - plain text
        server_->Get("/api/predictions/text", [this](const httplib::Request&, httplib::Response& res) {
            auto text = getPredictionsText();
            res.set_content(text, "text/plain");
        });
        
        // Combined live data endpoint
        server_->Get("/api/live", [this](const httplib::Request&, httplib::Response& res) {
            nlohmann::json liveData;
            liveData["orderbook"] = getOrderBookJson();
            liveData["predictions"] = getPredictionsJson();
            liveData["health"] = getHealthJson();
            res.set_content(liveData.dump(2), "application/json");
        });
        
        // Serve static files (index.html from public folder)
        server_->Get("/", [](const httplib::Request&, httplib::Response& res) {
            res.set_content(R"(
<!DOCTYPE html>
<html>
<head>
    <title>Live Market Dashboard</title>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: #0f1419; color: #e0e0e0; }
        .container { max-width: 1400px; margin: 0 auto; padding: 20px; }
        h1 { margin-bottom: 20px; color: #00d4ff; }
        h2 { font-size: 0.95rem; color: #00d4ff; margin-top: 20px; margin-bottom: 10px; border-bottom: 1px solid #333; padding-bottom: 5px; }
        .grid { display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 20px; margin-bottom: 20px; }
        .panel { background: #1a1f26; border: 1px solid #2a3038; border-radius: 8px; padding: 20px; overflow: auto; max-height: 600px; }
        .panel.full { grid-column: 1 / -1; }
        table { width: 100%; font-size: 0.85rem; border-collapse: collapse; }
        th { background: #242a32; padding: 8px; text-align: right; color: #00d4ff; font-weight: 600; border-bottom: 1px solid #333; }
        td { padding: 6px 8px; text-align: right; border-bottom: 1px solid #2a2f36; }
        .ask-row { color: #ff6b6b; }
        .bid-row { color: #51cf66; }
        .hot-price { background: rgba(0, 212, 255, 0.1); }
        .status { display: inline-block; width: 10px; height: 10px; border-radius: 50%; background: #51cf66; margin-right: 5px; }
        #status { margin-bottom: 10px; font-size: 0.9rem; }
        pre { background: #242a32; padding: 10px; border-radius: 4px; font-size: 0.8rem; overflow-x: auto; }
    </style>
</head>
<body>
    <div class="container">
        <h1>📊 Live Market Dashboard</h1>
        <div id="status"></div>
        
        <div class="grid">
            <div class="panel">
                <h2>Asks (50 best)</h2>
                <table>
                    <thead><tr><th>Price</th><th>Volume</th></tr></thead>
                    <tbody id="asks"></tbody>
                </table>
            </div>
            
            <div class="panel">
                <h2>Bids (50 best)</h2>
                <table>
                    <thead><tr><th>Price</th><th>Volume</th></tr></thead>
                    <tbody id="bids"></tbody>
                </table>
            </div>
            
            <div class="panel">
                <h2>System Status</h2>
                <pre id="health"></pre>
            </div>
        </div>
        
        <div class="panel full">
            <h2>Recent Predictions</h2>
            <pre id="predictions"></pre>
        </div>
    </div>
    
    <script>
        async function updateDashboard() {
            try {
                const response = await fetch('/api/live');
                const data = await response.json();
                
                // Update orderbook
                updateOrderBook(data.orderbook.asks, 'asks');
                updateOrderBook(data.orderbook.bids, 'bids');
                
                // Update health
                document.getElementById('status').innerHTML = 
                    `<span class="status"></span>Last updated: ${new Date().toLocaleTimeString()}`;
                
                const healthText = `Asks: ${data.health.orderbook_asks_size}\nBids: ${data.health.orderbook_bids_size}`;
                document.getElementById('health').textContent = healthText;
                
                // Update predictions
                const predText = data.predictions.map(p => p).join('\n');
                document.getElementById('predictions').textContent = predText || 'Waiting for predictions...';
                
            } catch (e) {
                console.error('Error updating dashboard:', e);
                document.getElementById('status').innerHTML = '<span style="color: #ff6b6b;">Connection error</span>';
            }
        }
        
        function updateOrderBook(levels, type) {
            const tbody = document.getElementById(type);
            tbody.innerHTML = '';
            levels.forEach(level => {
                const row = document.createElement('tr');
                row.className = type === 'asks' ? 'ask-row' : 'bid-row';
                row.innerHTML = `<td>${level.price}</td><td>${level.volume}</td>`;
                tbody.appendChild(row);
            });
        }
        
        // Update every 250ms
        updateDashboard();
        setInterval(updateDashboard, 250);
    </script>
</body>
</html>
            )", "text/html");
        });
        
        // Handle OPTIONS for CORS preflight
        server_->Options("/.*", [](const httplib::Request&, httplib::Response& res) {
            res.set_header("Access-Control-Allow-Origin", "*");
            res.set_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
            res.set_header("Access-Control-Allow-Headers", "Content-Type");
            res.status = 200;
        });
    }

    auto start(int port = 8080) -> bool {
        if (running_) return false;
        
        running_ = true;
        serverThread_ = std::thread([this, port]() {
            std::cout << "[DashboardServer] Starting on http://0.0.0.0:" << port << std::endl;
            server_->listen("0.0.0.0", port);
        });
        
        // Give server time to start
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        return true;
    }

    auto stop() -> void {
        if (!running_) return;
        running_ = false;
        server_->stop();
        if (serverThread_.joinable()) {
            serverThread_.join();
        }
        std::cout << "[DashboardServer] Stopped." << std::endl;
    }

    ~DashboardServer() {
        stop();
    }
};

}

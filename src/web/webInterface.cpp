#include "web/webInterface.hpp"
#include <algorithm>

namespace Web {

nlohmann::json orderBookToJson(
    const MarketData::OrderBookContainer& book,
    std::size_t depth
) {
    auto [asks, bids] = book.getOrderBook();

    nlohmann::json result;
    auto askArr = nlohmann::json::array();
    auto bidArr = nlohmann::json::array();

    auto pushLevel = [&](const MarketData::IntPriceVolume& lvl, nlohmann::json& arr) {
        arr.push_back({
            {"price", lvl.intPrice / 1e8},
            {"volume", lvl.intVolume / 1e8}
        });
    };

    for (std::size_t i = 0; i < std::min(depth, asks.size()); ++i) {
        pushLevel(asks[i], askArr);
    }
    for (std::size_t i = 0; i < std::min(depth, bids.size()); ++i) {
        pushLevel(bids[i], bidArr);
    }

    result["asks"] = std::move(askArr);
    result["bids"] = std::move(bidArr);
    result["timestamp"] = std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::system_clock::now().time_since_epoch()
    ).count();

    return result;
}

nlohmann::json recentTradesToJson(
    const MarketData::TradeVolumeContainer& tv,
    std::size_t count
) {
    // we rely on the container's mutex to snapshot the current sequence
    auto recent = tv.getLastTrades();
    nlohmann::json arr = nlohmann::json::array();

    std::size_t limit = std::min(count, recent.size());
    for (std::size_t i = 0; i < limit; ++i) {
        const auto& lvl = recent[recent.size() - limit + i];
        arr.push_back({
            {"price", lvl.intPrice / 1e8},
            {"volume", lvl.intVolume / 1e8}
        });
    }

    nlohmann::json result;
    result["trades"] = std::move(arr);
    result["timestamp"] = std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::system_clock::now().time_since_epoch()
    ).count();

    return result;
}

} // namespace Web

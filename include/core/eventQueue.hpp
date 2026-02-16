#pragma once

#include <mutex>
#include <condition_variable>
#include <vector>
#include <boost/beast/core/flat_buffer.hpp>

namespace MarketData {

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

} 

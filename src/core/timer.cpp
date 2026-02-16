#include <iostream>

#include "core/timer.hpp"

namespace MarketData {

Timer::Timer() {
    start_ = std::chrono::high_resolution_clock::now();
}

Timer::~Timer() {
    end_ = std::chrono::high_resolution_clock::now();
    
    auto start = std::chrono::time_point_cast<std::chrono::nanoseconds>(start_).time_since_epoch().count();
    auto end = std::chrono::time_point_cast<std::chrono::nanoseconds>(end_).time_since_epoch().count();

    auto duration = end - start;

    std::cout << duration << '\n';
}

auto Timer::stop() -> void {
    end_ = std::chrono::high_resolution_clock::now();
    duration_ = end_ - start_;
}

auto Timer::getDuration() -> double {
    return duration_.count();
}

auto Timer::now() -> std::chrono::time_point<std::chrono::high_resolution_clock> {
    return std::chrono::high_resolution_clock::now();
}

}
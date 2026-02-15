#include "timer.h"

Timer::Timer() {
    start = std::chrono::high_resolution_clock::now();
}

Timer::~Timer() {
    end = std::chrono::high_resolution_clock::now();
    auto startnano = std::chrono::time_point_cast<std::chrono::nanoseconds>(start).time_since_epoch().count();
    auto endnano = std::chrono::time_point_cast<std::chrono::nanoseconds>(end).time_since_epoch().count();
    auto duration = endnano - startnano;
    std::cout << duration << '\n';
}

auto Timer::stop() -> void {
    end = std::chrono::high_resolution_clock::now();
    duration = end - start;
}

auto Timer::getDuration() -> double {
    return duration.count();
}

auto Timer::now() -> std::chrono::time_point<std::chrono::high_resolution_clock> {
    return std::chrono::high_resolution_clock::now();
}
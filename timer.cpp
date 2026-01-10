#include "timer.h"

Timer::Timer() {
    start = std::chrono::high_resolution_clock::now();
}

Timer::~Timer() {
    end = std::chrono::high_resolution_clock::now();
    duration = end - start;
    std::cout << duration.count() << '\n';
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
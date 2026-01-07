#ifndef TIMER_H
#define TIMER_H

#include <chrono>
#include <iostream>

class Timer {
private:
    std::chrono::time_point<std::chrono::high_resolution_clock> start{}, end{};
    std::chrono::duration<double> duration;

public:
    Timer();

    ~Timer();

    auto stop() -> void;

    auto getDuration() -> double;
};

#endif

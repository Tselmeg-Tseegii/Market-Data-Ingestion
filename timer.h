#ifndef TIMER_H
#define TIMER_H

#include <chrono>
#include <iostream>

class Timer {
private:
    std::chrono::time_point<std::chrono::high_resolution_clock> start_;
    std::chrono::time_point<std::chrono::high_resolution_clock> end_;

    std::chrono::duration<double> duration_;

public:
    Timer();

    ~Timer();

    auto stop() -> void;

    auto getDuration() -> double;

    auto now() ->  std::chrono::time_point<std::chrono::high_resolution_clock>;
};

#endif

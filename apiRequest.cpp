#include <iostream>
#include <cstdlib>
#include <string>

#define CPPHTTPLIB_OPENSSL_SUPPORT
#include "httplib.h"

#include "timer.h"

auto MY_API_KEY = std::string{std::getenv("TWELVEDATA_MY_API_KEY")};
auto currSymbol = std::string{"EUR/USD"};

int main() {
    auto client = httplib::Client{"https://api.twelvedata.com"};

    Timer time{};
    auto res = httplib::Result{
        client.Get(
            "/exchange_rate?symbol=" + currSymbol + "&apikey=" + MY_API_KEY
        )
    };
    time.stop();

    std::cout << time.getDuration() << '\n';

    std::cout << "Status code " << res->status << '\n'; 
    std::cout << res->body;

}
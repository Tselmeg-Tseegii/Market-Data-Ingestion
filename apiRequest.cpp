#include <iostream>
#define CPPHTTPLIB_OPENSSL_SUPPORT
#include "httplib.h"

std::string myApiKey {"af01bbfaee8549739308d8668454684c"};
std::string currSymbol {"XAU/USD"};

int main() {
    httplib::Client cli("https://api.twelvedata.com");
    httplib::Result res = cli.Get("/earliest_timestamp?symbol=" + currSymbol + "&apikey=" + myApiKey + "&interval=15min");
    // httplib::Result res = cli.Get("/stocks");
    std::cout << "Status code " << res->status << '\n'; 
    std::cout << res->body;
}
cmake_minimum_required(VERSION 3.15)
project(DataIngestion)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_compile_definitions(CPPHTTPLIB_OPENSSL_SUPPORT)

list(APPEND CMAKE_PREFIX_PATH "/opt/local")

find_package(Boost REQUIRED COMPONENTS system filesystem)
find_package(OpenSSL REQUIRED)

include_directories(
    "${CMAKE_CURRENT_SOURCE_DIR}/include",
    "${CMAKE_CURRENT_SOURCE_DIR}/include/external"
)

set(SOURCES
    src/main.cpp
    
    src/core/timer.cpp
    src/core/marketDataType.cpp
    
    src/orderBook/orderBookEvent.cpp
    src/orderBook/orderBookContainer.cpp
    src/orderBook/orderBookUpdater.cpp
    
    src/tradeVolume/tradeVolumeContainer.cpp
    src/tradeVolume/tradeVolumeUpdater.cpp
    
    src/priceCandle/priceCandleContainer.cpp
    src/priceCandle/readDataThread.cpp
    
    include/external/simdjson/simdjson.cpp
)

add_executable(trader ${SOURCES})

target_link_libraries(trader PRIVATE
    Boost::system
    Boost::filesystem
    OpenSSL::SSL
    OpenSSL::Crypto
)

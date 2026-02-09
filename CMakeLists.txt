cmake_minimum_required(VERSION 3.15)
project(CryptoTrader)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

list(APPEND CMAKE_PREFIX_PATH "/opt/local")

# --- Find Dependencies ---
# Suppress the Boost warning (CMP0167) by explicit policy setting if needed, 
# or just ignore it as it's a warning.
find_package(Boost REQUIRED COMPONENTS system filesystem)
find_package(OpenSSL REQUIRED)

# --- Build Executable ---
# CHANGED: "main.cpp" -> "test.cpp" because main.cpp does not exist in your file list.
# Change "test.cpp" to whichever file contains your "int main()" function.
add_executable(trader apiRequest.cpp timer.cpp)

# --- Link Libraries ---
target_link_libraries(trader PRIVATE
    Boost::system
    Boost::filesystem
    OpenSSL::SSL
    OpenSSL::Crypto
)

# --- Include Directories ---
target_include_directories(trader PRIVATE 
    # CHANGED: Added current directory to find "httplib.h" which is in your root
    "${CMAKE_CURRENT_SOURCE_DIR}"
    
    # CHANGED: Added nlohmann folder to find "json.hpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/nlohmann"
    
    ${Boost_INCLUDE_DIRS}
    ${OPENSSL_INCLUDE_DIR}
)
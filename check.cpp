// ===========================================================================
// MINIMAL REPRODUCTION FIX
// ===========================================================================

// 1. Force Windows 10/11 Target
// This is required for Boost 1.90+ to enable modern features
#define _WIN32_WINNT 0x0A00 

// 2. Prevent Windows.h from loading old Winsock
#define WIN32_LEAN_AND_MEAN

// 3. MANUALLY include Winsock2 first.
// This is the "Nuclear Fix". It prevents boost/process from loading the wrong one.
#include <winsock2.h>
#include <windows.h>

// 4. Boost Asio (Must come before Process)
#include <boost/asio.hpp>

// 5. Boost Process
#include <boost/process.hpp>

#include <iostream>

int main() {
    std::cout << "Boost headers loaded successfully.\n";
    
    // Test the specific types you use
    boost::process::ipstream pipe_stream;
    boost::process::child c("cmd /c echo hello");
    
    return 0;
}
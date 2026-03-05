#pragma once

#include <string>
#include <thread>
#include <boost/process.hpp>

#include "shared/eventQueue.hpp"

namespace MarketData {

template<typename Event>
class ManagePythonProcess {
private:
    EventQueue<Event>& eventQueue_;
    std::string predictionSaveFile_;

    boost::process::opstream pipeToPython_;
    boost::process::ipstream pipeFromPython_;

    std::thread sendDataThread_;
    std::thread getDataThread_;

    std::unique_ptr<boost::process::child> pythonProcess_;

    bool stopProcess_ {false};

public:
    ManagePythonProcess(
        EventQueue<Event>& queue,
        std::string pythonFile,
        std::string_view predictionSaveFile
    )
        : eventQueue_ {queue}
        , predictionSaveFile_ {predictionSaveFile}
        , pipeToPython_ {}
        , pipeFromPython_ {}

    {
        // try to find an available python interpreter
        auto pythonExe = boost::process::search_path("python3.13");
        if (pythonExe.empty()) {
            pythonExe = boost::process::search_path("python3");
        }
        if (pythonExe.empty()) {
            pythonExe = boost::process::search_path("python");
        }
        
        if (pythonExe.empty()) {
            throw std::runtime_error("Could not find Python interpreter (tried python3.13, python3, python)");
        }

        pythonProcess_ = std::make_unique<boost::process::child>(
            pythonExe,
            "-u",
            pythonFile,
            boost::process::std_in < pipeToPython_,
            boost::process::std_out > pipeFromPython_, 
            boost::process::std_err > stderr
        );

        sendDataThread_ = std::thread{&ManagePythonProcess::sendDataToPython, this};
        getDataThread_ = std::thread{&ManagePythonProcess::getDataFromPython, this};
    }

    auto endProcess() -> void {
        stopProcess_ = true;
        eventQueue_.getCv().notify_all();

        if (sendDataThread_.joinable()) sendDataThread_.join();
        if (getDataThread_.joinable()) getDataThread_.join();
        if (pythonProcess_) {
            try {
                pythonProcess_->wait();
            } catch (const boost::process::process_error& e) {
                std::cerr << "warning: error waiting for python process: " << e.what() << "\n";
            }
        }
    }

    // destructor must not throw; split from endProcess so we can clean up safely
    ~ManagePythonProcess() noexcept {
        try {
            stopProcess_ = true;
            eventQueue_.getCv().notify_all();
        } catch(...) {}
        try {
            if (sendDataThread_.joinable()) sendDataThread_.join();
        } catch(...) {}
        try {
            if (getDataThread_.joinable()) getDataThread_.join();
        } catch(...) {}
        try {
            if (pythonProcess_ && pythonProcess_->running()) {
                try {
                    pythonProcess_->terminate();
                    pythonProcess_->wait();
                } catch(...) {}
            }
        } catch(...) {}
        try {
            pipeToPython_.close();
        } catch(...) {}
        try {
            pipeFromPython_.close();
        } catch(...) {}
        // finally delete the child object safely
        try {
            pythonProcess_.reset();
        } catch(...) {}
    }

private:
    auto sendDataToPython() -> void {
        auto& eventQueueCv = eventQueue_.getCv();
        auto eventQueueLock = std::unique_lock{eventQueue_.getMtx()};
        eventQueueLock.unlock();

        auto latestIntPrice = std::vector<IntPriceVolume>{};

        while (!stopProcess_) {
            auto currEvents = EventQueue<Event>{};

            eventQueueLock.lock();
            eventQueueCv.wait(eventQueueLock, [this] () {
                return eventQueue_.IsNotEmptyFlag() || stopProcess_;
            });
            if (stopProcess_) {
                break;
            }
            
            eventQueueLock.unlock();

            eventQueue_.spliceTo(currEvents);

            for (auto& currEvent : currEvents.getData()) {
                if (latestIntPrice.size() < 100) {
                    latestIntPrice.push_back(currEvent);
                }
                
                if (latestIntPrice.size() == 100) {
                    auto temp = PriceCandle{latestIntPrice};
                    std::cout << "python send: " << temp << '\n';
                    try {
                        pipeToPython_ << temp << std::endl;
                    } catch (const boost::process::process_error& e) {
                        std::cerr << "warning: failed to send to python (" << e.what() << ")\n";
                        // process may have died; we'll break out and let shutdown proceed
                        stopProcess_ = true;
                        break;
                    }
                    latestIntPrice.clear();  // reset after sending so new scripts can start fresh
                }
            }
        }

        try {
            pipeToPython_ << "STOP" << std::endl;
            pipeToPython_.close();
        } catch (const boost::process::process_error& e) {
            std::cerr << "warning: broken pipe when signalling stop: " << e.what() << "\n";
        }
    }

    auto getDataFromPython() -> void {
        auto prefictionSaveFile = std::fstream{predictionSaveFile_, std::ios::app};

        auto pythonResponse = std::string{};
        while (std::getline(pipeFromPython_, pythonResponse)) {
            std::cout << "got from python: " << pythonResponse << '\n';
            prefictionSaveFile << pythonResponse << std::endl;
        }
        pipeFromPython_.close();
    }
};

}
#pragma once

#include <string>
#include <thread>
#include <stdexcept>
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

    boost::process::child pythonProcess_;

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

        pythonProcess_ = boost::process::child{
            pythonExe,
            "-u",
            pythonFile,
            boost::process::std_in < pipeToPython_,
            boost::process::std_out > pipeFromPython_, 
            boost::process::std_err > stderr
        };

        sendDataThread_ = std::thread{&ManagePythonProcess::sendDataToPython, this};
        getDataThread_ = std::thread{&ManagePythonProcess::getDataFromPython, this};
    }

    auto endProcess() -> void {
        stopProcess_ = true;
        eventQueue_.getCv().notify_all();

        sendDataThread_.join();
        getDataThread_.join();
        pythonProcess_.wait();
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
                    pipeToPython_ << temp << std::endl;
                }
            }
        }

        pipeToPython_ << "STOP" << std::endl;
        pipeToPython_.close();
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
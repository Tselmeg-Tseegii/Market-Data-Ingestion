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

    boost::process::child pythonProcess_;

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
        pythonProcess_ = boost::process::child{
            boost::process::search_path("python3.13"),
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
        sendDataThread_.join();
        getDataThread_.join();
        pythonProcess_.wait();
    }

private:
    auto sendDataToPython() -> void {
        auto& eventQueueCv = eventQueue_.getCv();
        auto eventQueueLock = std::unique_lock{eventQueue_.getMtx()};
        eventQueueLock.unlock();

        while (true) {
            auto currEvents = EventQueue<Event>{};

            eventQueueLock.lock();
            eventQueueCv.wait(eventQueueLock, [this] () {
                return eventQueue_.IsNotEmptyFlag();
            });
            
            eventQueueLock.unlock();

            eventQueue_.spliceTo(currEvents);

            for (auto& currEvent : currEvents.getData()) {
                pipeToPython_ << currEvent << std::endl;
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
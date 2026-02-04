# Compiler to use
CXX = g++

# Compiler flags (e.g., -Wall for warnings)
CXXFLAGS = -Wall -std=c++20

# Linker flags (Libraries to link against)
LDFLAGS = -lboost_filesystem-mt -lssl -lcrypto -lcrypt32 -lws2_32

# The build target executable
TARGET = apiRequest.exe

# Source files
OBJECTS = apiRequest.o timer.o

# Default rule: runs when you type 'make'
all: $(TARGET)

# Rule to link the program
$(TARGET): $(OBJECTS)
	$(CXX) $(CXXFLAGS) -o $(TARGET) $(OBJECTS) $(LDFLAGS)

apiRequest.o: apiRequest.cpp timer.h
	$(CXX) $(CXXFLAGS) -c apiRequest.cpp

timer.o: timer.cpp timer.h
	$(CXX) $(CXXFLAGS) -c timer.cpp

# Rule to clean up build files (Windows CMD command)
clean:
	del $(TARGET)
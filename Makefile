# Compiler to use
CXX = g++

# Compiler flags (e.g., -Wall for warnings)
CXXFLAGS = -Wall

# Linker flags (Libraries to link against)
LDFLAGS = -lssl -lcrypto -lcrypt32 -lws2_32

# The build target executable
TARGET = apiRequest.exe

# Source files
SRC = apiRequest.cpp

# Default rule: runs when you type 'make'
all: $(TARGET)

# Rule to link the program
$(TARGET): $(SRC)
	$(CXX) $(CXXFLAGS) -o $(TARGET) $(SRC) $(LDFLAGS)

# Rule to clean up build files (Windows CMD command)
clean:
	del $(TARGET)
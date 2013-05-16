
INCL_DIR = ./include
BIN_DIR = ./bin
SRC_DIR = ./src
OBJ_DIR = ./obj

CXX = gcc 
# CXXFLAGS = -O0 -I$(INCL_DIR) -std=c99 -Wall -pthread 
# Debug configuration:
CXXFLAGS = -O3 -I$(INCL_DIR) -std=c99 -Wall -pthread -DDEBUG

OBJ_FILES = $(SRC_DIR)/server.o $(SRC_DIR)/worker.o

all: server worker
	@echo "Done!"

server: $(OBJ_DIR)/server.o
	$(CXX) $(CXXFLAGS) $^ -o $(BIN_DIR)/$@

$(OBJ_DIR)/server.o: $(SRC_DIR)/server.c
	$(CXX) $(CXXFLAGS) -c $^ -o $@

worker: $(OBJ_DIR)/integral.o $(OBJ_DIR)/worker.o
	$(CXX) $(CXXFLAGS) $^ -o $(BIN_DIR)/$@

$(OBJ_DIR)/worker.o: $(SRC_DIR)/worker.c
	$(CXX) $(CXXFLAGS) -c $^ -o $@

$(OBJ_DIR)/integral.o: $(SRC_DIR)/integral.c
	$(CXX) $(CXXFLAGS) -c $^ -o $@
	
clean:
	rm -rf $(OBJ_DIR)/*
	rm -rf $(BIN_DIR)/*

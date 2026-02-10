# Makefile
CC = gcc
CFLAGS = -Wall -O2 -D_GNU_SOURCE
LDFLAGS = -lpthread

TARGET = log_benchmark
SRC = log_transfer_benchmark.c

all: $(TARGET)

$(TARGET): $(SRC)
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS)

clean:
	rm -f $(TARGET) /tmp/test_log.txt

test: $(TARGET)
	@echo "Running 100MB test..."
	./$(TARGET) 100

test-large: $(TARGET)
	@echo "Running 1GB test..."
	./$(TARGET) 1000

.PHONY: all clean test test-large

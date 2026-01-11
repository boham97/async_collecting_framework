CC = gcc
CFLAGS = -Wall -Wextra -O2 -I/usr/include/postgresql
LDFLAGS = -lpq -lpthread
TARGET = server
SRC = server.c

all: $(TARGET)

$(TARGET): $(SRC)
	$(CC) $(CFLAGS) -o $(TARGET) $(SRC) $(LDFLAGS)

clean:
	rm -f $(TARGET)

run: $(TARGET)
	./$(TARGET)

debug: CFLAGS += -g -O0 -DDEBUG
debug: clean $(TARGET)

.PHONY: all clean run debug

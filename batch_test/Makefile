CC = gcc
CFLAGS = -O2 -Wall -Wextra -I/usr/include/postgresql
LDFLAGS = -lpq
TARGET = batch_test
SRC = batch_test.c

# 기본 타겟
all: $(TARGET)

# 실행 파일 생성
$(TARGET): $(SRC)
	$(CC) $(CFLAGS) -o $(TARGET) $(SRC) $(LDFLAGS)

# 실행
run: $(TARGET)
	./$(TARGET)

# 커스텀 연결 정보로 실행
run-custom: $(TARGET)
	./$(TARGET) "host=172.17.0.3 dbname=pgdb user=pguser password=pgpass"

# 10000개 테스트
run-large: $(TARGET)
	./$(TARGET) "host=172.17.0.3 dbname=pgdb user=pguser password=pgpass" 10000

# 정리
clean:
	rm -f $(TARGET)

# 재컴파일
rebuild: clean all

# 디버그 빌드
debug: CFLAGS = -g -Wall -Wextra -I/usr/include/postgresql
debug: clean $(TARGET)

# 도움말
help:
	@echo "Available targets:"
	@echo "  make          - Build the program"
	@echo "  make run      - Build and run with default settings"
	@echo "  make run-custom - Run with custom database connection"
	@echo "  make run-large - Run with 10000 rows test"
	@echo "  make debug    - Build with debug symbols"
	@echo "  make clean    - Remove binary"
	@echo "  make rebuild  - Clean and rebuild"

.PHONY: all run run-custom run-large clean rebuild debug help

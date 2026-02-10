// log_transfer_benchmark.c
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/sendfile.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <time.h>

#define TEST_FILE "/tmp/test_log.txt"
#define TEST_PORT 9999
#define BUFFER_SIZE 8192

// ========== 1. Zero-Copy (sendfile) ==========
int log_getLog_zeroCopy(int sock_fd, const char *file_name, int type, int request_size) {
    int log_fd;
    struct stat logfilestat;
    off_t offset = 0;
    ssize_t sent_bytes = 0;
    ssize_t total_sent = 0;
    off_t send_size;

    if ((log_fd = open(file_name, O_RDONLY)) == -1) {
        perror("open failed");
        return -1;
    }

    if (fstat(log_fd, &logfilestat) < 0) {
        perror("fstat failed");
        close(log_fd);
        return -1;
    }

    if (type == 1) {
        send_size = logfilestat.st_size;
    } else if (type == 2) {
        send_size = (request_size <= logfilestat.st_size) ? request_size : logfilestat.st_size;
        offset = logfilestat.st_size - send_size;
    } else {
        send_size = (request_size <= logfilestat.st_size) ? request_size : logfilestat.st_size;
        offset = 0;
    }

    while (total_sent < send_size) {
        sent_bytes = sendfile(sock_fd, log_fd, &offset, send_size - total_sent);
        if (sent_bytes <= 0) {
            if (errno == EINTR || errno == EAGAIN) continue;
            perror("sendfile failed");
            close(log_fd);
            return -1;
        }
        total_sent += sent_bytes;
    }

    close(log_fd);
    return (int)total_sent;
}

// ========== 2. Traditional read/write ==========
int log_getLog_traditional(int sock_fd, const char *file_name, int type, int request_size) {
    int log_fd;
    struct stat logfilestat;
    off_t offset = 0;
    ssize_t read_bytes, write_bytes;
    ssize_t total_sent = 0;
    off_t send_size;
    char buffer[BUFFER_SIZE];
    
    if ((log_fd = open(file_name, O_RDONLY)) == -1) {
        perror("open failed");
        return -1;
    }
    
    if (fstat(log_fd, &logfilestat) < 0) {
        perror("fstat failed");
        close(log_fd);
        return -1;
    }
    
    if (type == 1) {
        send_size = logfilestat.st_size;
    } else if (type == 2) {
        send_size = (request_size <= logfilestat.st_size) ? request_size : logfilestat.st_size;
        offset = logfilestat.st_size - send_size;
    } else {
        send_size = (request_size <= logfilestat.st_size) ? request_size : logfilestat.st_size;
        offset = 0;
    }
    
    if (offset > 0 && lseek(log_fd, offset, SEEK_SET) == -1) {
        perror("lseek failed");
        close(log_fd);
        return -1;
    }
    
    while (total_sent < send_size) {
        ssize_t to_read = (send_size - total_sent < sizeof(buffer)) 
                          ? (send_size - total_sent) : sizeof(buffer);
        
        read_bytes = read(log_fd, buffer, to_read);
        if (read_bytes < 0) {
            if (errno == EINTR) continue;
            perror("read failed");
            close(log_fd);
            return -1;
        }
        if (read_bytes == 0) break;
        
        ssize_t written = 0;
        while (written < read_bytes) {
            write_bytes = write(sock_fd, buffer + written, read_bytes - written);
            if (write_bytes < 0) {
                if (errno == EINTR || errno == EAGAIN) continue;
                perror("write failed");
                close(log_fd);
                return -1;
            }
            written += write_bytes;
        }
        total_sent += read_bytes;
    }
    
    close(log_fd);
    return (int)total_sent;
}

// ========== 3. No-Caching (posix_fadvise) ==========
int log_getLog_noCaching(int sock_fd, const char *file_name, int type, int request_size) {
    int log_fd;
    struct stat logfilestat;
    off_t offset = 0;
    ssize_t read_bytes, write_bytes;
    ssize_t total_sent = 0;
    off_t send_size;
    char buffer[BUFFER_SIZE];
    
    if ((log_fd = open(file_name, O_RDONLY)) == -1) {
        perror("open failed");
        return -1;
    }
    
    if (fstat(log_fd, &logfilestat) < 0) {
        perror("fstat failed");
        close(log_fd);
        return -1;
    }
    
    // 캐시 힌트: 순차 읽기, 재사용 안함
    posix_fadvise(log_fd, 0, 0, POSIX_FADV_SEQUENTIAL);
    posix_fadvise(log_fd, 0, 0, POSIX_FADV_NOREUSE);
    
    if (type == 1) {
        send_size = logfilestat.st_size;
    } else if (type == 2) {
        send_size = (request_size <= logfilestat.st_size) ? request_size : logfilestat.st_size;
        offset = logfilestat.st_size - send_size;
    } else {
        send_size = (request_size <= logfilestat.st_size) ? request_size : logfilestat.st_size;
        offset = 0;
    }
    
    if (offset > 0 && lseek(log_fd, offset, SEEK_SET) == -1) {
        perror("lseek failed");
        close(log_fd);
        return -1;
    }
    
    // 읽은 영역을 즉시 캐시에서 제거
    off_t current_offset = offset;
    
    while (total_sent < send_size) {
        ssize_t to_read = (send_size - total_sent < sizeof(buffer)) 
                          ? (send_size - total_sent) : sizeof(buffer);
        
        read_bytes = read(log_fd, buffer, to_read);
        if (read_bytes < 0) {
            if (errno == EINTR) continue;
            perror("read failed");
            close(log_fd);
            return -1;
        }
        if (read_bytes == 0) break;
        
        // 방금 읽은 부분을 캐시에서 제거
        posix_fadvise(log_fd, current_offset, read_bytes, POSIX_FADV_DONTNEED);
        current_offset += read_bytes;
        
        ssize_t written = 0;
        while (written < read_bytes) {
            write_bytes = write(sock_fd, buffer + written, read_bytes - written);
            if (write_bytes < 0) {
                if (errno == EINTR || errno == EAGAIN) continue;
                perror("write failed");
                close(log_fd);
                return -1;
            }
            written += write_bytes;
        }
        total_sent += read_bytes;
    }
    
    close(log_fd);
    return (int)total_sent;
}

// ========== 테스트 파일 생성 ==========
void create_test_file(const char *filename, size_t size_mb) {
    FILE *fp = fopen(filename, "w");
    if (!fp) {
        perror("Failed to create test file");
        exit(1);
    }
    
    printf("Creating %zu MB test file...\n", size_mb);
    
    char line[256];
    size_t written = 0;
    size_t target = size_mb * 1024 * 1024;
    int line_num = 0;
    
    while (written < target) {
        int len = snprintf(line, sizeof(line), 
                          "[%010d] %ld - This is a test log line with some data\n",
                          line_num++, time(NULL));
        fwrite(line, 1, len, fp);
        written += len;
    }
    
    fclose(fp);
    printf("Test file created: %s (%zu bytes)\n", filename, written);
}

// ========== 더미 소켓 수신 서버 ==========
void *receiver_thread(void *arg) {
    int server_fd, client_fd;
    struct sockaddr_in addr;
    char buffer[65536];
    
    server_fd = socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(TEST_PORT);
    
    bind(server_fd, (struct sockaddr*)&addr, sizeof(addr));
    listen(server_fd, 1);
    
    while (1) {
        client_fd = accept(server_fd, NULL, NULL);
        if (client_fd < 0) continue;
        
        ssize_t total = 0;
        ssize_t n;
        while ((n = read(client_fd, buffer, sizeof(buffer))) > 0) {
            total += n;
        }
        
        close(client_fd);
    }
    
    return NULL;
}

// ========== 벤치마크 실행 ==========
double benchmark(const char *method_name, 
                 int (*method)(int, const char*, int, int),
                 const char *file_name) {
    int sock_fd;
    struct sockaddr_in server_addr;
    struct timespec start, end;
    
    // 소켓 연결
    sock_fd = socket(AF_INET, SOCK_STREAM, 0);
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(TEST_PORT);
    inet_pton(AF_INET, "127.0.0.1", &server_addr.sin_addr);
    
    if (connect(sock_fd, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        perror("connect failed");
        close(sock_fd);
        return -1;
    }
    
    // 캐시 드롭 (Linux only, root 권한 필요)
    system("sync");
    // system("echo 3 > /proc/sys/vm/drop_caches");  // root 필요
    
    // 벤치마크 실행
    clock_gettime(CLOCK_MONOTONIC, &start);
    int bytes_sent = method(sock_fd, file_name, 1, 0);
    clock_gettime(CLOCK_MONOTONIC, &end);
    
    close(sock_fd);
    
    if (bytes_sent < 0) {
        printf("[%s] Failed\n", method_name);
        return -1;
    }
    
    double elapsed = (end.tv_sec - start.tv_sec) + 
                     (end.tv_nsec - start.tv_nsec) / 1e9;
    double throughput_mb = (bytes_sent / (1024.0 * 1024.0)) / elapsed;
    
    printf("[%s] %d bytes in %.4f sec (%.2f MB/s)\n", 
           method_name, bytes_sent, elapsed, throughput_mb);
    
    return elapsed;
}

// ========== 메인 ==========
int main(int argc, char *argv[]) {
    size_t file_size_mb = 100;  // 기본 100MB
    
    if (argc > 1) {
        file_size_mb = atoi(argv[1]);
    }
    
    printf("=== Log Transfer Benchmark ===\n");
    printf("File size: %zu MB\n\n", file_size_mb);
    
    // 1. 테스트 파일 생성
    create_test_file(TEST_FILE, file_size_mb);
    
    // 2. 수신 서버 시작 (백그라운드)
    pthread_t receiver;
    pthread_create(&receiver, NULL, receiver_thread, NULL);
    pthread_detach(receiver);
    sleep(1);  // 서버 준비 대기
    
    // 3. 벤치마크 실행
    printf("\n=== Starting Benchmark ===\n");
    
    double t1 = benchmark("Zero-Copy (sendfile)", log_getLog_zeroCopy, TEST_FILE);
    sleep(1);
    
    double t2 = benchmark("Traditional (read/write)", log_getLog_traditional, TEST_FILE);
    sleep(1);
    
    double t3 = benchmark("No-Caching (posix_fadvise)", log_getLog_noCaching, TEST_FILE);
    
    // 4. 결과 요약
    printf("\n=== Summary ===\n");
    if (t1 > 0 && t2 > 0) {
        printf("Speedup (sendfile vs traditional): %.2fx\n", t2 / t1);
    }
    if (t1 > 0 && t3 > 0) {
        printf("Speedup (sendfile vs posix_fadvise): %.2fx\n", t3 / t1);
    }
    
    // 5. 정리
    unlink(TEST_FILE);
    
    return 0;
}
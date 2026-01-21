#include <libpq-fe.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/time.h>
#include <arpa/inet.h>

#define BATCH_SIZE 100

typedef struct {
    int fds[BATCH_SIZE];
    char data[BATCH_SIZE][256];
    int count;
} log_buffer_t;

typedef struct {
    double prep_time;
    double query_time;
    double total_time;
} timing_t;

double get_time() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec + tv.tv_usec / 1000000.0;
}

// 테이블 생성
int create_test_table(PGconn *conn) {
    printf("Creating test table...\n");
    
    PGresult *res = PQexec(conn,
        "CREATE TABLE IF NOT EXISTS batch_test ("
        "    id SERIAL PRIMARY KEY,"
        "    client_fd INT NOT NULL,"
        "    message TEXT NOT NULL,"
        "    created_at TIMESTAMP DEFAULT NOW()"
        ")"
    );
    
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        fprintf(stderr, "CREATE TABLE failed: %s\n", PQerrorMessage(conn));
        PQclear(res);
        return -1;
    }
    
    PQclear(res);
    printf("Table created successfully\n\n");
    return 0;
}

// 테이블 삭제
int drop_test_table(PGconn *conn) {
    printf("\nDropping test table...\n");
    
    PGresult *res = PQexec(conn, "DROP TABLE IF EXISTS batch_test");
    
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        fprintf(stderr, "DROP TABLE failed: %s\n", PQerrorMessage(conn));
        PQclear(res);
        return -1;
    }
    
    PQclear(res);
    printf("Table dropped successfully\n");
    return 0;
}

// 테이블 초기화
int truncate_table(PGconn *conn) {
    PGresult *res = PQexec(conn, "TRUNCATE batch_test");
    
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        fprintf(stderr, "TRUNCATE failed: %s\n", PQerrorMessage(conn));
        PQclear(res);
        return -1;
    }
    
    PQclear(res);
    return 0;
}

// 레코드 개수 확인
int count_records(PGconn *conn) {
    PGresult *res = PQexec(conn, "SELECT COUNT(*) FROM batch_test");
    
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        fprintf(stderr, "COUNT failed: %s\n", PQerrorMessage(conn));
        PQclear(res);
        return -1;
    }
    
    int count = atoi(PQgetvalue(res, 0, 0));
    PQclear(res);
    return count;
}

// 방법 1: 개별 INSERT (동기)
timing_t test_individual_insert(PGconn *conn, int count) {
    printf("=== Test 1: Individual INSERT (sync) ===\n");
    printf("Inserting %d rows one by one...\n", count);
    
    truncate_table(conn);
    
    double total_start = get_time();
    double prep_time = 0;
    double query_time = 0;
        /* BEGIN */
    PGresult *res = PQexec(conn, "BEGIN");
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        fprintf(stderr, "BEGIN failed: %s\n", PQerrorMessage(conn));
        PQclear(res);
        timing_t err = {-1, -1, -1};
        return err;
    }
    PQclear(res);
    for (int i = 0; i < count; i++) {
        // 데이터 준비 시작
        double prep_start = get_time();
        
        char fd_str[32], data[128];
        snprintf(fd_str, sizeof(fd_str), "%d", i);
        snprintf(data, sizeof(data), "message_%d", i);
        
        const char *params[2] = {fd_str, data};
        
        prep_time += get_time() - prep_start;
        
        // 쿼리 실행 시작
        double query_start = get_time();
        
        PGresult *res = PQexecParams(conn,
            "INSERT INTO batch_test (client_fd, message) VALUES ($1, $2)",
            2, NULL, params, NULL, NULL, 0);
        
        query_time += get_time() - query_start;
        
        if (PQresultStatus(res) != PGRES_COMMAND_OK) {
            fprintf(stderr, "INSERT failed: %s\n", PQerrorMessage(conn));
            PQclear(res);
            timing_t err = {-1, -1, -1};
            return err;
        }
        
        PQclear(res);
        
        if ((i + 1) % 100 == 0) {
            printf("  Progress: %d/%d\r", i + 1, count);
            fflush(stdout);
        }
    }
   
    /* COMMIT */
    res = PQexec(conn, "COMMIT");
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        fprintf(stderr, "COMMIT failed: %s\n", PQerrorMessage(conn));
        PQclear(res);
        timing_t err = {-1, -1, -1};
        return err;
    }
    PQclear(res);

    double total_elapsed = get_time() - total_start;
    int final_count = count_records(conn);
    
    printf("\n");
    printf("Time elapsed:\n");
    printf("  Data preparation: %.6f seconds (%.1f%%)\n", prep_time, 100.0 * prep_time / total_elapsed);
    printf("  Query execution:  %.6f seconds (%.1f%%)\n", query_time, 100.0 * query_time / total_elapsed);
    printf("  Total:            %.6f seconds\n", total_elapsed);
    printf("Records inserted: %d\n", final_count);
    printf("Rate: %.1f inserts/sec\n\n", count / total_elapsed);
    
    timing_t result = {prep_time, query_time, total_elapsed};
    return result;
}

// 방법 2: 배치 INSERT (동기)
timing_t test_batch_insert(PGconn *conn, int count) {
    printf("=== Test 2: Batch INSERT (sync) ===\n");
    printf("Inserting %d rows in one query...\n", count);
    
    truncate_table(conn);
    
    double total_start = get_time();
    
    // 데이터 준비 시작
    double prep_start = get_time();
    
    // 배열 문자열 생성
    char *fd_array = malloc(count * 20);
    char *data_array = malloc(count * 100);
    
    if (!fd_array || !data_array) {
        fprintf(stderr, "Memory allocation failed\n");
        free(fd_array);
        free(data_array);
        timing_t err = {-1, -1, -1};
        return err;
    }
    
    strcpy(fd_array, "{");
    strcpy(data_array, "{");
    
    for (int i = 0; i < count; i++) {
        char temp[128];
        
        // fd 배열
        snprintf(temp, sizeof(temp), "%s%d", (i > 0) ? "," : "", i);
        strcat(fd_array, temp);
        
        // data 배열
        snprintf(temp, sizeof(temp), "%s\"message_%d\"", (i > 0) ? "," : "", i);
        strcat(data_array, temp);
        
        if ((i + 1) % 100 == 0) {
            printf("  Building array: %d/%d\r", i + 1, count);
            fflush(stdout);
        }
    }
    
    strcat(fd_array, "}");
    strcat(data_array, "}");
    
    const char *params[2] = {fd_array, data_array};
    
    double prep_time = get_time() - prep_start;
    
    printf("\n  Array built in %.6f sec, executing query...\n", prep_time);
    
    // 쿼리 실행 시작
    double query_start = get_time();
    
    PGresult *res = PQexecParams(conn,
        "INSERT INTO batch_test (client_fd, message) "
        "SELECT unnest($1::int[]), unnest($2::text[])",
        2, NULL, params, NULL, NULL, 0);
    
    double query_time = get_time() - query_start;
    
    free(fd_array);
    free(data_array);
    
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        fprintf(stderr, "Batch INSERT failed: %s\n", PQerrorMessage(conn));
        PQclear(res);
        timing_t err = {-1, -1, -1};
        return err;
    }
    
    double total_elapsed = get_time() - total_start;
    int final_count = count_records(conn);
    
    printf("Time elapsed:\n");
    printf("  Data preparation: %.6f seconds (%.1f%%)\n", prep_time, 100.0 * prep_time / total_elapsed);
    printf("  Query execution:  %.6f seconds (%.1f%%)\n", query_time, 100.0 * query_time / total_elapsed);
    printf("  Total:            %.6f seconds\n", total_elapsed);
    printf("Records inserted: %d (returned: %s)\n", final_count, PQcmdTuples(res));
    printf("Rate: %.1f inserts/sec\n\n", count / total_elapsed);
    
    PQclear(res);
    
    timing_t result = {prep_time, query_time, total_elapsed};
    return result;
}

// 방법 3: 배치 버퍼 방식 (동기)
double flush_buffer(PGconn *conn, log_buffer_t *buf, double *prep_time_out, double *query_time_out) {
    if (buf->count == 0) return 0;
    
    // 데이터 준비 시작
    double prep_start = get_time();
    
    char fd_array[4096] = "{";
    char data_array[65536] = "{";
    
    for (int i = 0; i < buf->count; i++) {
        char temp[1024];
        
        snprintf(temp, sizeof(temp), "%s%d",
                 (i > 0) ? "," : "", buf->fds[i]);
        strcat(fd_array, temp);
        
        // 이스케이프 처리
        char escaped[512];
        int len = 0;
        for (const char *p = buf->data[i]; *p; p++) {
            if (*p == '"' || *p == '\\') escaped[len++] = '\\';
            escaped[len++] = *p;
        }
        escaped[len] = '\0';
        
        snprintf(temp, sizeof(temp), "%s\"%s\"",
                 (i > 0) ? "," : "", escaped);
        strcat(data_array, temp);
    }
    
    strcat(fd_array, "}");
    strcat(data_array, "}");
    
    const char *params[2] = {fd_array, data_array};
    
    double prep_time = get_time() - prep_start;
    *prep_time_out += prep_time;
    
    // 쿼리 실행 시작
    double query_start = get_time();
    
    PGresult *res = PQexecParams(conn,
        "INSERT INTO batch_test (client_fd, message) "
        "SELECT unnest($1::int[]), unnest($2::text[])",
        2, NULL, params, NULL, NULL, 0);
    
    double query_time = get_time() - query_start;
    *query_time_out += query_time;
    
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        fprintf(stderr, "Buffer flush failed: %s\n", PQerrorMessage(conn));
    }
    
    PQclear(res);
    buf->count = 0;
    
    return prep_time + query_time;
}

timing_t test_buffered_insert(PGconn *conn, int count) {
    printf("=== Test 3: Buffered Batch INSERT (sync) ===\n");
    printf("Inserting %d rows with buffer size %d...\n", count, BATCH_SIZE);
    
    truncate_table(conn);
    
    double total_start = get_time();
    double prep_time = 0;
    double query_time = 0;
    
    log_buffer_t buf = {.count = 0};
    int flush_count = 0;
    
    for (int i = 0; i < count; i++) {
        // 버퍼에 추가 (데이터 준비)
        double prep_start = get_time();
        
        buf.fds[buf.count] = i;
        snprintf(buf.data[buf.count], 256, "message_%d", i);
        buf.count++;
        
        prep_time += get_time() - prep_start;
        
        if (buf.count >= BATCH_SIZE) {
            flush_buffer(conn, &buf, &prep_time, &query_time);
            flush_count++;
            
            if (flush_count % 10 == 0) {
                printf("  Flushed: %d batches (%d records)\r", 
                       flush_count, flush_count * BATCH_SIZE);
                fflush(stdout);
            }
        }
    }
    
    // 남은 데이터 flush
    if (buf.count > 0) {
        flush_buffer(conn, &buf, &prep_time, &query_time);
        flush_count++;
    }
    
    double total_elapsed = get_time() - total_start;
    int final_count = count_records(conn);
    
    printf("\n");
    printf("Time elapsed:\n");
    printf("  Data preparation: %.6f seconds (%.1f%%)\n", prep_time, 100.0 * prep_time / total_elapsed);
    printf("  Query execution:  %.6f seconds (%.1f%%)\n", query_time, 100.0 * query_time / total_elapsed);
    printf("  Total:            %.6f seconds\n", total_elapsed);
    printf("Total flushes: %d\n", flush_count);
    printf("Records inserted: %d\n", final_count);
    printf("Rate: %.1f inserts/sec\n\n", count / total_elapsed);
    
    timing_t result = {prep_time, query_time, total_elapsed};
    return result;
}

// 샘플 데이터 확인
void show_sample_data(PGconn *conn, int limit) {
    printf("Sample data (first %d rows):\n", limit);
    
    char query[256];
    snprintf(query, sizeof(query), 
             "SELECT id, client_fd, message, created_at FROM batch_test "
             "ORDER BY id LIMIT %d", limit);
    
    PGresult *res = PQexec(conn, query);
    
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        fprintf(stderr, "SELECT failed: %s\n", PQerrorMessage(conn));
        PQclear(res);
        return;
    }
    
    int nrows = PQntuples(res);
    printf("%-6s %-12s %-20s %s\n", "ID", "Client FD", "Message", "Created At");
    printf("------------------------------------------------------------\n");
    
    for (int i = 0; i < nrows; i++) {
        printf("%-6s %-12s %-20s %s\n",
               PQgetvalue(res, i, 0),
               PQgetvalue(res, i, 1),
               PQgetvalue(res, i, 2),
               PQgetvalue(res, i, 3));
    }
    
    PQclear(res);
    printf("\n");
}

/* 방법 4: COPY BINARY */
timing_t test_copy_binary(PGconn *conn, int count) {
    printf("=== Test 4: COPY BINARY ===\n");
    printf("Inserting %d rows using COPY BINARY...\n", count);

    truncate_table(conn);

    double total_start = get_time();
    double prep_time = 0;
    double query_time = 0;

    PGresult *res = PQexec(conn,
        "COPY batch_test (client_fd, message) FROM STDIN BINARY");
    if (PQresultStatus(res) != PGRES_COPY_IN) {
        fprintf(stderr, "COPY start failed: %s\n", PQerrorMessage(conn));
        PQclear(res);
        timing_t err = {-1, -1, -1};
        return err;
    }
    PQclear(res);

    /* ---- header ---- */
    const char hdr[] = "PGCOPY\n\377\r\n\0";
    int32_t flags = htonl(0);
    int32_t ext = htonl(0);

    double prep_start = get_time();
    PQputCopyData(conn, hdr, 11);
    PQputCopyData(conn, (char *)&flags, 4);
    PQputCopyData(conn, (char *)&ext, 4);
    prep_time += get_time() - prep_start;

    /* ---- rows ---- */
    double query_start = get_time();

    for (int i = 0; i < count; i++) {
        int16_t fc = htons(2);
        PQputCopyData(conn, (char *)&fc, 2);

        int32_t len = htonl(4);
        int32_t fd = htonl(i);
        PQputCopyData(conn, (char *)&len, 4);
        PQputCopyData(conn, (char *)&fd, 4);

        char msg[64];
        int mlen = snprintf(msg, sizeof(msg), "message_%d", i);
        int32_t mlen_net = htonl(mlen);
        PQputCopyData(conn, (char *)&mlen_net, 4);
        PQputCopyData(conn, msg, mlen);
    }

    int16_t end = htons(-1);
    PQputCopyData(conn, (char *)&end, 2);
    PQputCopyEnd(conn, NULL);

    while ((res = PQgetResult(conn)) != NULL)
        PQclear(res);

    query_time += get_time() - query_start;

    double total_elapsed = get_time() - total_start;
    int final_count = count_records(conn);

    printf("Time elapsed:\n");
    printf("  Data preparation: %.6f seconds (%.1f%%)\n",
           prep_time, 100.0 * prep_time / total_elapsed);
    printf("  Query execution:  %.6f seconds (%.1f%%)\n",
           query_time, 100.0 * query_time / total_elapsed);
    printf("  Total:            %.6f seconds\n", total_elapsed);
    printf("Records inserted: %d\n", final_count);
    printf("Rate: %.1f inserts/sec\n\n", count / total_elapsed);

    timing_t r = {prep_time, query_time, total_elapsed};
    return r;
}
int main(int argc, char *argv[]) {
    const char *conninfo;
    int test_count = 1000;
    
    if (argc > 1) {
        conninfo = argv[1];
    } else {
        conninfo = "host=172.17.0.3 dbname=pgdb user=pguser password=pgpass";
    }
    
    if (argc > 2) {
        test_count = atoi(argv[2]);
    }
    
    printf("========================================\n");
    printf("PostgreSQL Batch Insert Performance Test\n");
    printf("========================================\n");
    printf("Connection: %s\n", conninfo);
    printf("Test count: %d rows\n\n", test_count);
    
    PGconn *conn = PQconnectdb(conninfo);
    
    if (PQstatus(conn) != CONNECTION_OK) {
        fprintf(stderr, "Connection failed: %s\n", PQerrorMessage(conn));
        PQfinish(conn);
        return 1;
    }
    
    printf("Connected to PostgreSQL %s\n\n", 
           PQparameterStatus(conn, "server_version"));
    drop_test_table(conn);
    if (create_test_table(conn) != 0) {
        PQfinish(conn);
        return 1;
    }
    
    // 테스트 실행
    timing_t t1 = test_individual_insert(conn, test_count);
    timing_t t2 = test_batch_insert(conn, test_count);
    timing_t t3 = test_buffered_insert(conn, test_count);
    timing_t t4 = test_copy_binary(conn, test_count);

    // 결과 요약
    printf("========================================\n");
    printf("Performance Summary\n");
    printf("========================================\n");
    printf("Test count: %d rows\n\n", test_count);
    
    printf("Method                  Prep Time  Query Time  Total Time    Rate (rows/sec)  Speedup\n");
    printf("----------------------------------------------------------------------------------------\n");
    printf("1. Individual INSERT   %7.3fs    %7.3fs    %7.3fs    %10.1f         1.0x\n", 
           t1.prep_time, t1.query_time, t1.total_time, test_count / t1.total_time);
    printf("2. Single Batch INSERT  %7.3fs    %7.3fs    %7.3fs    %10.1f       %5.1fx\n", 
           t2.prep_time, t2.query_time, t2.total_time, test_count / t2.total_time, 
           t1.total_time / t2.total_time);
    printf("3. Buffered Batch       %7.3fs    %7.3fs    %7.3fs    %10.1f       %5.1fx\n", 
           t3.prep_time, t3.query_time, t3.total_time, test_count / t3.total_time,
           t1.total_time / t3.total_time);
    printf("4. COPY BINARY          %7.3fs    %7.3fs    %7.3fs    %10.1f       %5.1fx\n",
            t4.prep_time, t4.query_time, t4.total_time, test_count / t4.total_time,
            t1.total_time / t4.total_time);
    printf("\n");
    
    printf("Time Breakdown:\n");
    printf("Individual - Prep: %.1f%%, Query: %.1f%%\n", 
           100.0 * t1.prep_time / t1.total_time,
           100.0 * t1.query_time / t1.total_time);
    printf("Batch      - Prep: %.1f%%, Query: %.1f%%\n",
           100.0 * t2.prep_time / t2.total_time,
           100.0 * t2.query_time / t2.total_time);
    printf("Buffered   - Prep: %.1f%%, Query: %.1f%%\n\n",
           100.0 * t3.prep_time / t3.total_time,
           100.0 * t3.query_time / t3.total_time);
    printf("Binary     - Prep: %.1f%%, Query: %.1f%%\n\n",
           100.0 * t4.prep_time /  t4.total_time,
           100.0 * t4.query_time / t4.total_time);
    show_sample_data(conn, 5);
    drop_test_table(conn);
    PQfinish(conn);
    
    printf("\nTest completed successfully!\n");
    return 0;
}
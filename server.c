#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/epoll.h>
#include <fcntl.h>
#include <errno.h>
#include <pthread.h>
#include <libpq-fe.h>

#define PORT 8080
#define MAX_EVENTS 10
#define BUF_SIZE 1024
#define POOL_SIZE 10
#define QUEUE_SIZE 1000

// PostgreSQL 연결 정보
#define DB_HOST "172.17.0.3"
#define DB_PORT "5432"
#define DB_NAME "pgdb"
#define DB_USER "pguser"
#define DB_PASS "pgpass"

enum state {
    STATE_SERVER,
    STATE_CLIENT,
    STATE_PGSQL
};

typedef struct pg_conn_s pg_conn_t;

typedef struct {
    int type;      // 0: server, 1: client, 2: pgsql
    int fd;
    char *buf;
    size_t buf_len;
    pg_conn_t *pg_conn; // PostgreSQL 연결 포인터
} epoll_event_t;

typedef struct pg_conn_s {
    PGconn *conn;
    int in_use;
    epoll_event_t *client_data; // 클라이언트 데이터 저장
    epoll_event_t *pg_event_data; // epoll event_data 참조
} pg_conn_t;

typedef struct {
    pg_conn_t pool[POOL_SIZE];
    pthread_mutex_t pool_lock;
} pg_pool_t;

pg_pool_t *g_pool = NULL;
int g_epfd = -1; // 전역 epoll fd

// PostgreSQL 연결 풀 초기화
pg_pool_t* init_pg_pool(int epfd) 
{
    pg_pool_t *pool = malloc(sizeof(pg_pool_t));
    pthread_mutex_init(&pool->pool_lock, NULL);
    
    char conninfo[512];
    snprintf(conninfo, sizeof(conninfo),
                "host=%s port=%s dbname=%s user=%s password=%s",
                DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS);

    for (int i = 0; i < POOL_SIZE; i++) 
    {
        pool->pool[i].conn = PQconnectdb(conninfo);
        PQsetnonblocking(pool->pool[i].conn, 1);
        
        if (PQstatus(pool->pool[i].conn) != CONNECTION_OK) {
            fprintf(stderr, "Connection to database failed: %s\n",
                    PQerrorMessage(pool->pool[i].conn));
            PQfinish(pool->pool[i].conn);
            pool->pool[i].conn = NULL;
            continue;
        }
        
        pool->pool[i].in_use = 0;
        pool->pool[i].client_data = NULL;
        pool->pool[i].pg_event_data = NULL;
        
        // epoll에 PostgreSQL 소켓 등록 (초기에는 EPOLLIN만)
        int pg_fd = PQsocket(pool->pool[i].conn);
        if (pg_fd >= 0) {
            epoll_event_t *pg_data = malloc(sizeof(epoll_event_t));
            pg_data->type = STATE_PGSQL;
            pg_data->fd = pg_fd;
            pg_data->buf = NULL;
            pg_data->buf_len = 0;
            pg_data->pg_conn = &pool->pool[i]; // 연결 포인터 저장
            
            pool->pool[i].pg_event_data = pg_data; // 역참조
            
            struct epoll_event ev;
            ev.events = EPOLLIN | EPOLLET; // 초기에는 EPOLLOUT 제외
            ev.data.ptr = pg_data;
            
            if (epoll_ctl(epfd, EPOLL_CTL_ADD, pg_fd, &ev) == -1) {
                perror("epoll_ctl: pg socket");
                free(pg_data);
            } else {
                printf("PostgreSQL connection %d registered to epoll (fd=%d)\n", i, pg_fd);
            }
        }
    }
    
    return pool;
}

// 연결 풀에서 사용 가능한 연결 가져오기
pg_conn_t* get_pg_conn(pg_pool_t *pool) {
    pthread_mutex_lock(&pool->pool_lock);
    
    for (int i = 0; i < POOL_SIZE; i++) {
        if (!pool->pool[i].in_use && pool->pool[i].conn != NULL) {
            pool->pool[i].in_use = 1;
            pthread_mutex_unlock(&pool->pool_lock);
            return &pool->pool[i];
        }
    }
    
    pthread_mutex_unlock(&pool->pool_lock);
    return NULL; // 사용 가능한 연결 없음
}

// 연결 반환
void release_pg_conn(pg_pool_t *pool, pg_conn_t *conn_t) {
    pthread_mutex_lock(&pool->pool_lock);
    conn_t->in_use = 0;
    conn_t->client_data = NULL;
    pthread_mutex_unlock(&pool->pool_lock);
}

int set_nonblock(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

void dump_epoll_events(uint32_t events)
{
    if (events & EPOLLIN)        printf("EPOLLIN        "); // 읽을 수 있는 데이터 있음 (또는 FIN → read()==0)
    if (events & EPOLLOUT)       printf("EPOLLOUT       "); // write 가능 (send buffer 여유 / connect 완료)
    if (events & EPOLLRDHUP)     printf("EPOLLRDHUP     "); // 상대가 write 종료(FIN), 마지막 데이터 가능
    if (events & EPOLLHUP)       printf("EPOLLHUP       "); // 소켓 완전 종료(hang up), 즉시 close 대상
    if (events & EPOLLERR)       printf("EPOLLERR       "); // 소켓 에러 발생, read/write 금지
    if (events & EPOLLPRI)       printf("EPOLLPRI       "); // 긴급 데이터(OOB), 일반 서버는 거의 안 씀
    if (events & EPOLLET)        printf("EPOLLET        "); // Edge Triggered 모드
    if (events & EPOLLONESHOT)   printf("EPOLLONESHOT   "); // 이벤트 1회성, 처리 후 재등록 필요
    printf("(0x%x)\n", events);
}

// DB에 메시지 저장 시작
int start_db_insert(pg_conn_t *conn_t, epoll_event_t *client_data, int epfd) {
    char query[2048];
    snprintf(query, sizeof(query),
        "INSERT INTO messages (client_fd, data, timestamp) "
        "VALUES (%d, '%s', NOW())",
        client_data->fd, client_data->buf);
    
    if (!PQsendQuery(conn_t->conn, query)) {
        fprintf(stderr, "PQsendQuery failed: %s", PQerrorMessage(conn_t->conn));
        return -1;
    }
    
    printf("DB insert started for client_fd=%d, data='%s'\n", 
           client_data->fd, client_data->buf);
    
    conn_t->client_data = client_data; // 클라이언트 데이터 저장
    
    // 쿼리 전송 후 EPOLLOUT 추가 (flush 대기)
    int pg_fd = PQsocket(conn_t->conn);
    struct epoll_event ev;
    ev.events = EPOLLIN | EPOLLOUT | EPOLLET;           // EPOLLOUT 쓰기 가능 대기
    ev.data.ptr = conn_t->pg_event_data;
    
    if (epoll_ctl(epfd, EPOLL_CTL_MOD, pg_fd, &ev) == -1) {
        perror("epoll_ctl: mod EPOLLOUT");
    } else {
        printf("EPOLLOUT enabled for PG socket fd=%d\n", pg_fd);
    }
    
    return 0;
}

// DB 결과 처리
int handle_db_result(pg_conn_t *conn_t, uint32_t events, int epfd) {
    int pg_fd = PQsocket(conn_t->conn);
    
    // EPOLLOUT: flush 완료 대기
    if (events & EPOLLOUT) {
        int flush_result = PQflush(conn_t->conn);
        if (flush_result == 0) {
            // flush 완료, EPOLLOUT 제거
            printf("PG flush completed, removing EPOLLOUT\n");
            
            struct epoll_event ev;
            ev.events = EPOLLIN | EPOLLET; // EPOLLOUT 제거
            ev.data.ptr = conn_t->pg_event_data;
            
            if (epoll_ctl(epfd, EPOLL_CTL_MOD, pg_fd, &ev) == -1) {
                perror("epoll_ctl: mod remove EPOLLOUT");
            }
        } else if (flush_result == -1) {
            fprintf(stderr, "PQflush failed: %s", PQerrorMessage(conn_t->conn));
            return -1;
        }
        // flush_result == 1: 아직 flush 중, 계속 대기
    }
    
    // EPOLLIN: 결과 수신
    if (events & EPOLLIN) {
        // 입력 데이터 소비
        if (!PQconsumeInput(conn_t->conn)) {
            fprintf(stderr, "PQconsumeInput failed: %s", PQerrorMessage(conn_t->conn));
            return -1;
        }
        
        // 아직 처리 중
        if (PQisBusy(conn_t->conn)) {
            return 0;
        }
        
        // 결과 가져오기
        PGresult *res;
        while ((res = PQgetResult(conn_t->conn)) != NULL) {
            ExecStatusType status = PQresultStatus(res);
            
            if (status == PGRES_COMMAND_OK) {
                printf("DB insert successful! Rows: %s\n", PQcmdTuples(res));
            } else {
                fprintf(stderr, "DB insert failed: %s\n", PQerrorMessage(conn_t->conn));
            }
            
            PQclear(res);
        }
        
        // 클라이언트 데이터 정리
        if (conn_t->client_data) {
            if (conn_t->client_data->buf) {
                free(conn_t->client_data->buf);
            }
            free(conn_t->client_data);
        }
        
        return 1; // 완료
    }
    
    return 0;
}

int create_table()
{
    // 테이블 생성 (없으면)
    pg_conn_t *conn_t = get_pg_conn(g_pool);
    if (!conn_t) {
        fprintf(stderr, "Failed to get PG connection\n");
        return -1;
    }
    
    PGconn *conn = conn_t->conn;
    PQsetnonblocking(conn, 0); 
    PGresult *res = PQexec(conn,
        "CREATE TABLE IF NOT EXISTS messages ("
        "id SERIAL PRIMARY KEY, "
        "client_fd INT, "
        "data TEXT, "
        "timestamp TIMESTAMP DEFAULT NOW())");
        
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        fprintf(stderr, "CREATE TABLE failed: %s", PQerrorMessage(conn));
    } else {
        printf("Table 'messages' ready\n");
    }
    PQclear(res);
    PQsetnonblocking(conn, 1);
    release_pg_conn(g_pool, conn_t);
    return 0;
}

int main() {
    // 서버 소켓 설정
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    set_nonblock(server_fd);
    
    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(PORT);

    bind(server_fd, (struct sockaddr*)&addr, sizeof(addr));
    listen(server_fd, 128);
    
    printf("Server listening on port %d\n", PORT);

    g_epfd = epoll_create1(0);
    struct epoll_event ev, events[MAX_EVENTS + POOL_SIZE];

    epoll_event_t *server_data = malloc(sizeof(*server_data));
    if (!server_data) {
        perror("malloc");
        close(server_fd);
        return 1;
    }
    ev.events = EPOLLIN;
    server_data->type = STATE_SERVER;
    server_data->fd = server_fd;
    ev.data.ptr = server_data;
    epoll_ctl(g_epfd, EPOLL_CTL_ADD, server_fd, &ev);

    // PostgreSQL 연결 풀 초기화 및 epoll 등록
    g_pool = init_pg_pool(g_epfd);
    printf("PostgreSQL connection pool initialized (%d connections)\n", POOL_SIZE);
    
    create_table();

    while (1) {
        int n = epoll_wait(g_epfd, events, MAX_EVENTS + POOL_SIZE, -1);         //최대 MAX_EVENTS + POOL_SIZE 만큼 events 에 담김

        for (int i = 0; i < n; i++) 
        {
            epoll_event_t *event_data = events[i].data.ptr;
            
            /* listen socket */
            if (event_data->type == STATE_SERVER) {
                while (1) 
                {
                    int client_fd = accept(server_fd, NULL, NULL);
                    if (client_fd < 0) 
                    {
                        if (errno == EAGAIN || errno == EWOULDBLOCK)
                            break;
                        perror("accept");
                        break;
                    }

                    set_nonblock(client_fd);

                    epoll_event_t *new_data = malloc(sizeof(*new_data));
                    new_data->buf = malloc(BUF_SIZE);
                    if (!new_data || !new_data->buf) 
                    {
                        close(client_fd);
                        continue;
                    }

                    new_data->fd = client_fd;
                    new_data->buf_len = 0;
                    new_data->type = STATE_CLIENT;
                    ev.events = EPOLLIN | EPOLLRDHUP | EPOLLET;
                    ev.data.ptr = new_data;
                    epoll_ctl(g_epfd, EPOLL_CTL_ADD, client_fd, &ev);

                    printf("Client connected: fd=%d\n", client_fd);
                }
                continue;
            }
            else if (event_data->type == STATE_CLIENT)
            {
                /* client socket */
                int fd = event_data->fd;
                uint32_t evs = events[i].events;
                printf("Client fd=%d: ", fd);
                dump_epoll_events(evs);
                
                if (evs & (EPOLLERR | EPOLLHUP)) {
                    epoll_ctl(g_epfd, EPOLL_CTL_DEL, fd, NULL);
                    close(fd);
                    if (event_data->buf) free(event_data->buf);
                    free(event_data);
                    printf("Client error/hangup: fd=%d\n", fd);
                    continue;
                }
                
                if (evs & EPOLLIN) 
                {
                    while (1) 
                    {
                        ssize_t r = recv(fd, event_data->buf + event_data->buf_len, 
                                        BUF_SIZE - event_data->buf_len - 1, 0);
                        if (r > 0) 
                        {
                            event_data->buf_len += r;
                            event_data->buf[event_data->buf_len] = '\0';
                            printf("recv(fd=%d): %.*s\n", fd, (int)r, 
                                   event_data->buf + event_data->buf_len - r);
                        } 
                        else if (r == 0) 
                        { 
                            // 연결 종료 -> DB에 저장
                            printf("Client disconnected: fd=%d, saving to DB...\n", fd);
                            
                            pg_conn_t *conn_t = get_pg_conn(g_pool);
                            if (conn_t) {
                                if (start_db_insert(conn_t, event_data, g_epfd) == 0) {
                                    // DB 저장 시작 성공, client_data는 DB 완료 후 정리
                                } else {
                                    // DB 저장 실패, 즉시 정리
                                    if (event_data->buf) free(event_data->buf);
                                    free(event_data);
                                    release_pg_conn(g_pool, conn_t);
                                }
                            } else {
                                fprintf(stderr, "No available DB connection\n");
                                if (event_data->buf) free(event_data->buf);
                                free(event_data);
                            }
                            
                            epoll_ctl(g_epfd, EPOLL_CTL_DEL, fd, NULL);
                            close(fd);
                            break;
                        } 
                        else 
                        {
                            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                                break;
                            } else {
                                perror("recv");
                                epoll_ctl(g_epfd, EPOLL_CTL_DEL, fd, NULL);
                                close(fd);
                                if (event_data->buf) free(event_data->buf);
                                free(event_data);
                                break;
                            }
                        }
                    }
                }
            }
            else if (event_data->type == STATE_PGSQL)
            {
                /* PostgreSQL 소켓 이벤트 */
                pg_conn_t *conn_t = event_data->pg_conn; // 포인터로 직접 접근
                uint32_t evs = events[i].events;
                
                printf("PG event: conn=%p, in_use=%d, events=", 
                       (void*)conn_t, conn_t->in_use);
                dump_epoll_events(evs);
                
                if (conn_t->in_use && conn_t->client_data) {
                    int result = handle_db_result(conn_t, evs, g_epfd);
                    if (result == 1) {
                        // DB 작업 완료
                        printf("DB operation completed for conn=%p\n", (void*)conn_t);
                        release_pg_conn(g_pool, conn_t);
                    } else if (result == -1) {
                        // DB 에러
                        fprintf(stderr, "DB operation failed for conn=%p\n", (void*)conn_t);
                        if (conn_t->client_data) {
                            if (conn_t->client_data->buf) free(conn_t->client_data->buf);
                            free(conn_t->client_data);
                        }
                        release_pg_conn(g_pool, conn_t);
                    }
                }
            }
            else
            {
                printf("Unknown event type: %d\n", event_data->type);
            }
        }
    }
    
    // 정리
    for (int i = 0; i < POOL_SIZE; i++) {
        if (g_pool->pool[i].conn) {
            PQfinish(g_pool->pool[i].conn);
        }
    }
    pthread_mutex_destroy(&g_pool->pool_lock);
    free(g_pool);
    close(server_fd);
    close(g_epfd);
    return 0;
}
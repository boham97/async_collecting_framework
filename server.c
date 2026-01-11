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

typedef struct {
    int type; // 0: server, 1: client, 2: pgsql
    char *buf;
    size_t buf_len;
} epoll_event_t;

typedef struct {
    PGconn *conn;
    int in_use;
    epoll_event_t ptr; // 임시 데이터 저장용
} pg_conn_t;

typedef struct {
    pg_conn_t pool[POOL_SIZE];
    pthread_mutex_t pool_lock;  // 전체 풀만 보호하면 충분
} pg_pool_t;

pg_pool_t *g_pool = NULL;

// PostgreSQL 연결 풀 초기화
pg_pool_t* init_pg_pool() 
{
    pg_pool_t *pool = malloc(sizeof(pg_pool_t));
    pthread_mutex_init(&pool->pool_lock, NULL);
    
    char conninfo[512];
    snprintf(conninfo, sizeof(conninfo),
                "host=%s port=%s dbname=%s user=%s password=%s",
                DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS);

    for (int i = 0; i < POOL_SIZE; i++) 
    {
        //
        pool->pool[i].conn = PQconnectdb(conninfo);
        PQsetnonblocking(pool->pool[i].conn, 1);                                  // 논블록킹 모드 설정
        if (PQstatus(pool->pool[i].conn) != CONNECTION_OK) {
            fprintf(stderr, "Connection to database failed: %s\n",
                    PQerrorMessage(pool->pool[i].conn));
            PQfinish(pool->pool[i].conn);
            pool->pool[i].conn = NULL;
        }
        
        pool->pool[i].in_use = 0;
        pool->pool[i].ptr.type = STATE_PGSQL;
        //epoll_ctl(epfd, EPOLL_CTL_MOD, pg_fd, &ev);
    }
    
    return pool;
}

// 연결 풀에서 사용 가능한 연결 가져오기
pg_conn_t* get_pg_conn(pg_pool_t *pool) {
    while (1) {
        pthread_mutex_lock(&pool->pool_lock);
        
        for (int i = 0; i < POOL_SIZE; i++) {
            if (!pool->pool[i].in_use && pool->pool[i].conn != NULL) {
                pool->pool[i].in_use = 1;
                pthread_mutex_unlock(&pool->pool_lock);
                return pool->pool + i;
            }
        }
        
        pthread_mutex_unlock(&pool->pool_lock);
        usleep(10000); // 10ms 대기
    }
}

// 연결 반환
void release_pg_conn(pg_pool_t *pool, PGconn *conn) {
    pthread_mutex_lock(&pool->pool_lock);
    
    for (int i = 0; i < POOL_SIZE; i++) {
        if (pool->pool[i].conn == conn) {
            pool->pool[i].in_use = 0;
            break;
        }
    }
    
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

int create_table()
{

    // PostgreSQL 연결 풀 초기화
    g_pool = init_pg_pool();
    printf("PostgreSQL connection pool initialized (%d connections)\n", POOL_SIZE);
    
    
    // 테이블 생성 (없으면)
    PGconn *conn = get_pg_conn(g_pool)->conn;
    PQsetnonblocking(conn, 0); 
    PGresult *res = PQexec(conn,
        "CREATE TABLE IF NOT EXISTS messages ("
        "id SERIAL PRIMARY KEY, "
        "client_fd INT, "
        "data TEXT, "
        "timestamp TIMESTAMP)");
        
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        fprintf(stderr, "CREATE TABLE failed: %s", PQerrorMessage(conn));
    }
    PQclear(res);
    PQsetnonblocking(conn, 1);
    release_pg_conn(g_pool, conn);
    return 0;
}


int main() {
    //db pool
    //setting pg

    
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

    int epfd = epoll_create1(0);
    struct epoll_event ev, events[MAX_EVENTS + POOL_SIZE];

    epoll_event_t *data = malloc(sizeof(*data));
    if (!data) {
        perror("malloc");
        close(server_fd);
        return 1;
    }
    ev.events = EPOLLIN;
    data->type = 0; // server
    ev.data.ptr = data;
    epoll_ctl(epfd, EPOLL_CTL_ADD, server_fd, &ev);

    create_table();

    while (1) {
        int n = epoll_wait(epfd, events, MAX_EVENTS + POOL_SIZE, -1);

        for (int i = 0; i < n; i++) 
        {
            /* listen socket */
            epoll_event_t *event_data = events[i].data.ptr;
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

                    epoll_event_t *new_data = malloc(sizeof(*data));
                    new_data->buf = malloc(BUF_SIZE);
                    if (!new_data || !new_data->buf) 
                    {
                        close(client_fd);
                        continue;
                    }

                    new_data->fd = client_fd;
                    new_data->buf_len = 0;
                    new_data->type = 1; // client
                    ev.events = EPOLLIN | EPOLLRDHUP | EPOLLET;
                    ev.data.ptr = new_data;
                    epoll_ctl(epfd, EPOLL_CTL_ADD, client_fd, &ev);

                    printf("Client connected: fd=%d\n", client_fd);
                }

                continue;
            }else if (event_data->type == STATE_CLIENT)
            {
                /* client socket */
                epoll_event_t *data = events[i].data.ptr;
                int fd = data->fd;
                uint32_t evs = events[i].events;
                dump_epoll_events(evs);
                
                /* 
                    🔥 종료/에러 먼저 
                    EPOLLIN EPOLLRDHUP 같이 올수 있음 테스트 케이스 printf 'aaa' | nc localhost 8080     
                */
               if (evs & (EPOLLERR | EPOLLHUP)) {
                   epoll_ctl(epfd, EPOLL_CTL_DEL, fd, NULL);
                   close(fd);
                   free(event_data);
                   continue;
                }
                
                
                //읽을 데이터가 있음
                if (evs & EPOLLIN) 
                {
                    while (1) 
                    {
                        ssize_t r = recv(fd, data->buf + data->buf_len, BUF_SIZE - 1, 0);
                        if (r > 0) 
                        {
                            data->buf_len += r;
                            data->buf[data->buf_len] = '\0';
                            
                            printf("recv(fd=%d): %.*s\n", fd, (int)r, data->buf);
                            
                        } else if (r == 0) 
                        { 
                            pg_conn_t *conn_t = get_pg_conn(g_pool);    // 연결 가져오기
                            conn_t->ptr = event_data->buf;              //  buf 포인터 저장 -> flush 전까지 가지고 있기위해서
                            epoll_ctl(epfd, PGsocket(conn_t->conn), EPOLLIN | EPOLLOUT);

                            

                            epoll_ctl(epfd, EPOLL_CTL_DEL, fd, NULL);
                            close(fd);
                            free(event_data);
                            printf("Client disconnected: fd=%d\n", fd);
                            
                            
                            break;
                        } else 
                        {
                            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                                break;
                            } else {
                                perror("recv");
                                epoll_ctl(epfd, EPOLL_CTL_DEL, fd, NULL);
                                close(fd);
                                free(event_data);
                                break;
                            }
                        }
                    }
                }
                
                //lt 모드여서 확인 X EPOLLRDHUP  은 힌트!
                if(evs & EPOLLRDHUP) continue;
            }
            else if (event_data->type == STATE_PGSQL)
            {
                /* code */
            }
            else
            {
                //알수없는 이벤트
                printf("Unknown event type: %d\n",event_data->type);
            }
        }
    }
    //커넥션 풀 정리도 추가 필요
    close(server_fd);
    return 0;
}
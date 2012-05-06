#include <stdio.h>
#include <stdlib.h>
#include <stddef.h>
#include <pthread.h>
#include <stropts.h>
#include <errno.h>
#include <sys/socket.h>

#include "Logger.h"
#include "Structures.h"
#include "ClientConnectionPool.h"

void try_reconnect(struct asd_pool* pool, int sockfd) {
    int fdi;
    for(fdi = 0; fdi < pool->num_connections; fdi++) {
    if(pool->connections[fdi].fd == sockfd) {
        pool->connections[fdi].fd = connect_to_server(pool->host.hostname, pool->host.port);
        sockfd = pool->connections[fdi].fd;
        log("[ERROR][POOL] Socket died, attempted a reconnect with FD %d", sockfd);
        struct obj_header head;
        head.op = OP_NONE;
        int status = send(sockfd, &head, sizeof(head), MSG_NOSIGNAL);
        if(status < 0) {
            log("[ERROR][POOL] Host is DEAD, result of send(NOOP) is %d", status);
            pthread_mutex_unlock(&pool->connections[fdi].mutex);
            pool->connections[fdi].fd = -1;
            pool->alive = 0;
        }
        return;
    }
    }
}

struct asd_pool* create_pool(struct asd_host host, int num_connections) {
    struct asd_pool* pool = malloc(sizeof(struct asd_pool));
    log("[INIT][POOL] Created a Connection Pool %p", pool);
    pool->num_connections = num_connections;
    pool->connections = malloc(sizeof(struct asd_connection) * num_connections);
    pool->host = host;
    pool->alive = 1;
    int i;
    for(i = 0; i < num_connections; i++) {
        pool->connections[i].available = 1;
        int fd = connect_to_server(host);
        pool->connections[i].fd = fd;
        pthread_mutex_init(&pool->connections[i].mutex, NULL);
        log("[INIT][POOL] Pool connection has file descriptor %d", pool->connections[i].fd);
    }
    pthread_mutex_init(&pool->mutex, NULL);
    return pool;
};

int get_connection_b(struct asd_pool* pool) {
    char nbuf[1];

    if(pool->alive == 0) {
        log("[INFO][POOL] Connection pool is not responsive. Attempting Reconnect.");
        int i;
        for(i = 0; i < pool->num_connections; i++) {
            pool->connections[i].available = 1;
            int fd = connect_to_server(pool->host);
            struct obj_header head;
            head.op = OP_NONE;
            int status = send(fd, &head, sizeof(head), MSG_NOSIGNAL);
            if(status >= 0) {
                log("[INFO][POOL] Host is ALIVE, open FD = %d, result of send(NOOP) is %d", fd, status);
                recv(fd, nbuf, sizeof(char), 0);
                pool->connections[i].fd = fd;
                pool->alive = 1;
            }
        }
    }

    if(pool->alive == 0)
        return -1;

    log("[INFO][POOL] Scanning %d Connections in Pool %p", pool->num_connections, pool);
    int i, fd = -1;
    while(fd < 0) {
        pthread_mutex_lock(&pool->mutex);
        for(i = 0; i < pool->num_connections; i++) {
            if(pool->connections[i].available = 1) {
                pool->connections[i].available = 0;
                fd = pool->connections[i].fd;
                break;
            }
        }
        pthread_mutex_unlock(&pool->mutex);
    }
    pthread_mutex_lock(&pool->connections[i].mutex);
    return fd;
};

int release_connection(struct asd_pool* pool, int fd) {
    log("[INFO][POOL] Release a Pool FD %d", fd);
    pthread_mutex_lock(&pool->mutex);
    int i;
    for(i = 0; i < pool->num_connections; i++) {
        if(pool->connections[i].fd == fd) {
            pool->connections[i].available = 1;
            pthread_mutex_unlock(&pool->connections[i].mutex);
            break;
        }
    }
    pthread_mutex_unlock(&pool->mutex);
}

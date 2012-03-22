#include <stdio.h>
#include <stdlib.h>
#include <stddef.h>
#include <pthread.h>
#include <stropts.h>
#include <errno.h>

#include "Logger.h"
#include "Structures.h"
#include "ClientConnectionPool.h"

struct asd_pool* create_pool(struct asd_host host, int num_connections) {
    log("Creating a connection pool.");
    struct asd_pool* pool = malloc(sizeof(struct asd_pool));
    pool->num_connections = num_connections;
    pool->connections = malloc(sizeof(struct asd_connection) * num_connections);
    pool->host = host;
    int i;
    for(i = 0; i < num_connections; i++) {
        pool->connections[i].available = 1;
        int fd = connect_to_server(host);
        pool->connections[i].fd = fd;
        pthread_mutex_init(&pool->connections[i].mutex, NULL);
        log("Connection with fd %d", pool->connections[i].fd);
    }
    pthread_mutex_init(&pool->mutex, NULL);
    log("Pool has addr %d", pool);
    return pool;
};

int get_connection_b(struct asd_pool* pool) {
    log("Scanning conns");
    log("Scanning total of %d conns", pool->num_connections);
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
    log("Releasing a pool with fd %d", fd);
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

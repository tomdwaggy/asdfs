#include <string.h>
#include <stdio.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/socket.h>

#include "Structures.h"
#include "Logger.h"

#include "ClientConnectionPool.h"

int write_remote(struct asd_pool* pool, const char* buf, struct obj_header head) {
    int sockfd, n;
    log("[INFO][WRITE] Trying to get a Writer connection with Connection Pool (%p)", pool);
    sockfd = get_connection_b(pool);
    log("[INFO][WRITE] Found a free FD for writing (%d)", sockfd);

    if(sockfd < 0) {
        log("[ERROR][WRITE] Tried to write to a dead socket.");
        return 0;
    }

    int written = write(sockfd, &head, sizeof(head));

    if(written <= 0) {
        try_reconnect(pool, sockfd);
        return 0;
    }

    n = write(sockfd, buf, head.size);

    if(n <= 0) {
        try_reconnect(pool, sockfd);
        return 0;
    }

    release_connection(pool, sockfd);

    return n;
}

int read_remote(struct asd_pool* pool, char* buf, struct obj_header head) {
    int sockfd;
    log("[INFO][READ] Trying to get a Reader connection with Connection Pool (%p)", pool);
    sockfd = get_connection_b(pool);
    log("[INFO][READ] Found a free FD for reading (%d)", sockfd);

    if(sockfd < 0) {
        log("[ERROR][READ] Tried to read from a dead socket.");
        return 0;
    }

    int written = write(sockfd, &head, sizeof(head));
    if(written <= 0) {
        try_reconnect(pool, sockfd);
        return 0;
    }

    int remaining = head.size;

    int n = 1;
    int offset = 0;
    while(remaining > 0 && n > 0) {
        n = read(sockfd, buf + offset, remaining);

        if(n <= 0) {
            try_reconnect(pool, sockfd);
            return 0;
        }

        log("[INFO][READ] Read %d bytes", n);
        remaining -= n;
        offset += n;
    }

    release_connection(pool, sockfd);

    return offset + n;
}

int write_mirrored(struct asd_pool** pools, int nhosts, const char* buf, size_t size, off_t offset, int file, int stripe) {

    log("[INFO] Writing objects data to stripe mirrors.");
    int i, n;
    n = 0;
    for(i = 0; i < nhosts; i++) {
        struct obj_header head;
        head.op     = OP_WRITE;
        head.size   = size;
        head.offset = offset;
        head.file   = file;
        head.stripe = stripe;
        n += write_remote(pools[i], buf, head);
    }

    return n;
}

int read_mirrored(struct asd_pool** pools, int nhosts, char* buf, size_t size, off_t offset, int file, int stripe) {

    log("[INFO] Reading objects data from stripe mirrors.");
    int i = 0, n = 0;
    for(i = 0; i < nhosts; i++) {
        struct obj_header head;
        head.op     = OP_READ;
        head.size   = size;
        head.offset = offset;
        head.file   = file;
        head.stripe = stripe;
        if((n = read_remote(pools[i], buf, head)) > 0)
            break;
    }

    return n;
}

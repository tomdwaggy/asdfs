#include <string.h>
#include <stdio.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/socket.h>

#include "Structures.h"
#include "Logger.h"

#include "ClientConnectionPool.h"

char destroy_buffer[10000];

int write_remote(struct asd_pool* pool, const char* buf, struct obj_header head) {
    int sockfd, n;
    sockfd = get_connection_b(pool);

    write(sockfd, &head, sizeof(head));
    n = write(sockfd, buf, head.size);

    //head.op = OP_DONE;
    //write(sockfd, &head, sizeof(head));

    release_connection(pool, sockfd);

    return n;
}

int read_remote(struct asd_pool* pool, char* buf, struct obj_header head) {
    int sockfd;
    log("trying to get a reader connection with pool %d", pool);
    sockfd = get_connection_b(pool);
    log("%d sockfd read", sockfd);
    write(sockfd, &head, sizeof(head));

    int remaining = head.size;

    int n = 1;
    int offset = 0;
    while(remaining > 0 && n > 0) {
        n = read(sockfd, buf + offset, remaining);
        log("just read %d bytes", n);
        // memcpy(buf + offset, rbuffer, n);
        remaining -= n;
        offset += n;
    }

    if( n < 0 ) {
        log("SOCKET HAS FAILED!");
    }

//    while(n > 0) {
//        n = recv(sockfd, destroy_buffer, sizeof(destroy_buffer), 
// MSG_DONTWAIT);
//    }

    release_connection(pool, sockfd);

    return offset + n;
}

int write_mirrored(struct asd_pool* pools, int nhosts, const char* buf, size_t size, off_t offset, int file, int stripe) {

    log("Writing objects data to stripe mirrors.");
    int i, n;
    n = 0;
    for(i = 0; i < nhosts; i++) {
        struct obj_header head;
        head.op     = OP_WRITE;
        head.size   = size;
        head.offset = offset;
        head.file   = file;
        head.stripe = stripe;
        n += write_remote(pools + i, buf, head);
    }

    return n;
}

int read_mirrored(struct asd_pool* pools, int nhosts, char* buf, size_t size, off_t offset, int file, int stripe) {

    log("Reading objects data from stripe mirrors.");
    int i = 0, n = 0;
    for(i = 0; i < nhosts; i++) {
        struct obj_header head;
        head.op     = OP_READ;
        head.size   = size;
        head.offset = offset;
        head.file   = file;
        head.stripe = stripe;
        if((n = read_remote(pools + i, buf, head)) > 0)
            break;
    }

    return n;
}

#ifndef __STRUCTURES_H
#define __STRUCTURES_H

#include <sys/stat.h>

#define BLOCK_SIZE 1000000
#define BUFFER_SIZE 10000
#define NUMBER_STRIPES 2
#define NUMBER_MIRRORS 2

#define OP_WRITE 0
#define OP_READ  1
#define OP_DONE  2
#define OP_NONE  3
#define OP_READALL 4

// A hostname and port pair.
struct asd_host {
    char* hostname;
    int port;
};

// A connection object, containing a file descriptor,
// whether or not it is available, and a mutex to lock
// the connection.
struct asd_connection {
    int fd;
    int available;
    pthread_mutex_t mutex;
};

// A pool of connections, which contains an array of
// connection objects, a host object specifying what
// host the pool is for, and a number of connections
// that are supported.
struct asd_pool {
    pthread_mutex_t mutex;
    int alive;
    int invalidate;
    struct asd_host host;
    int num_connections;
    struct asd_connection* connections;
};

// Header to make a request to the object server which
// is sent with a direct packet write()
struct obj_header {
    char op;
    int file;
    int stripe;
    size_t size;
    off_t offset;
};

// An array of object servers, based on the predefined
// number of stripes and mirrors of the RAID10 setup.
struct asd_objects {
    int file;
    struct asd_host array[NUMBER_STRIPES][NUMBER_MIRRORS];
    struct asd_pool* pools[NUMBER_STRIPES][NUMBER_MIRRORS];
};

// A File with corresponding information such as file
// name and path. Many operations happen sequentially,
// with the same file, this is for a basic LRU cache.
struct asd_file {
    int id;
    struct stat stbuf;
    char* path;
};

#endif

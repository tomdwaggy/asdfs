#ifndef __STRUCTURES_H
#define __STRUCTURES_H

#define BLOCK_SIZE 1000000
#define BUFFER_SIZE 10000
#define NUMBER_STRIPES 2
#define NUMBER_MIRRORS 2

#define OP_WRITE 0
#define OP_READ  1
#define OP_DONE  2

// A hostname and port pair.
struct asd_host {
    char* hostname;
    int port;
};

struct asd_connection {
    int fd;
    int available;
    pthread_mutex_t mutex;
};

struct asd_pool {
    pthread_mutex_t mutex;
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
    int size;
    char* path;
};

#endif

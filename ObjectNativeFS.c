//
// This is the NativeFS implementation for the object servers, which
// stores data on an underlying filesystem.
//

#define FUSE_USE_VERSION 26
#define PACKAGE_VERSION 1

#define RESTBUF_SIZE 50000

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <dirent.h>
#include <signal.h>
#include <fcntl.h>
#include <fuse.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <netinet/in.h>

#include "Structures.h"
#include "MetadataClient.h"

static char* directory;

static struct asd_objects g_objs;

// A fatal error has occurred.
void error(const char* msg)
{
    perror(msg);
    exit(1);
}

void recover_file(int file_num, intmax_t file_size) {
    printf("Recovering file...\n");
    int stripe, mirror;
    char buf[RESTBUF_SIZE];
    for(stripe = 0; stripe < NUMBER_STRIPES; stripe++) {
        for(mirror = 0; mirror < NUMBER_MIRRORS; mirror++) {
            struct asd_host h = g_objs.array[stripe][mirror];
            printf("Trying to connect to host %s port %d\n", h.hostname, h.port);
            int fd = connect_to_server(g_objs.array[stripe][mirror]);
            if(fd > 0) {
                int success = 1;
                printf("Connected to a peer object server with FD %d\n", fd);
                struct obj_header head;
                head.op = OP_READ;
                head.file = file_num;
                head.stripe = stripe;
                head.size = file_size;
                head.offset = 0;
                char identifier[512];
                snprintf(identifier, sizeof(identifier), "%s/%d.s%d", directory, head.file, head.stripe);
                int local = open(identifier, O_CREAT | O_RDWR, S_IWUSR | S_IRUSR );

                int written = write(fd, &head, sizeof(head));
                if ( written < 0 ) {
                    printf("Error writing anything to socket...\n");
                    success = 0;
                }

                int remaining = head.size;
                int n = 1;
                lseek64(local, head.offset, SEEK_SET);
                while(remaining > 0 && n > 0) {
                    int toRead = (remaining > sizeof(buf)) ? sizeof(buf) : remaining;
                    n = read(fd, buf, toRead);
                    if( n < 0 ) {
                        printf("Error reading data...\n");
                        success = 0;
                    } else {
                        printf("Read some data... %d bytes\n", n);
                    }
                    remaining -= n;
                    int written = write(local, buf, n);
                    if ( written <= 0 ) {
                        printf("Couldn't write to the disk at file %s\n", identifier);
                    }
                }
                close(fd);
                close(local);
                if(success == 1)
                    break;
            }
        }
    }
}

// The main loop for the listener, will spawn off multiple child
// processes.
int main(int argc, char** argv) {
    int sockfd, newsockfd, port;
    socklen_t clilen;
    struct sockaddr_in serv_addr, cli_addr;
    int n;
    if (argc != 8) {
        fprintf(stderr, "USAGE: ./ObjectNativeFS <directory> <myhost> <port> <mdhost> <mdport> <smdhost> <smdport>\n");
        exit(1);
    }

    printf("Starting up Object Server...\n");
    directory = argv[1];

    // Build the host and slave structures for the meta data server
    struct asd_host mdhost;
    struct asd_host mdslave;
    mdhost.hostname = argv[4];
    mdhost.port = atoi(argv[5]);
    mdslave.hostname = argv[6];
    mdslave.port = atoi(argv[7]);

    // Get the invalids list from the server
    int iarray[255];
    intmax_t sarray[255];
    iarray[0] = 0;
    printf("Trying to get invalid data...\n");
    mdc_getinvalid(mdhost, mdslave, argv[2], atoi(argv[3]), iarray, sarray, 255);
    if(iarray[0] != 0) {
        int ii;
        g_objs = mdc_getobjects(mdhost, mdslave);
        for(ii = 0; ii < 255 && iarray[ii] != 0; ii++) {
            printf("Invalid file found: %d - size %jd\n", iarray[ii], sarray[ii]);
            recover_file(iarray[ii], sarray[ii]);
        }
    }
    // Done with processing invalids

    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0)
        error("Can't create socket!");
    bzero((char *) &serv_addr, sizeof(serv_addr));
    port = atoi(argv[3]);
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_addr.s_addr = INADDR_ANY;
    serv_addr.sin_port = htons(port);
    if(bind(sockfd, (struct sockaddr*) &serv_addr, sizeof(serv_addr)) < 0)
        error("Can't bind to port!");

    // We don't want to fill up the process table, and we don't care
    // about any of the children's state.
    signal(SIGCHLD, SIG_IGN);
    listen(sockfd, 25);

    // Listen for connections.
    while( 1 ) {
        clilen = sizeof(cli_addr);
        newsockfd = accept(sockfd, (struct sockaddr*) &cli_addr, &clilen);
        if (newsockfd < 0)
            error("Error on accept.");

        // Fork off a new child process.
        pid_t pID = fork();
        if(pID == 0) {
            // Close socket FDs that child does not need.
            close(sockfd);
            dispatch(newsockfd);
            close(newsockfd);
            exit(0);
        }
        // Close socket FD that is not needed for parent (listener)
        close(newsockfd);
    }

    // Close the listener socket.
    close(sockfd);

    return 0;
}

// This method is called when a command to store data to the object
// store is received.
int store_data(int remote, struct obj_header head) {
    char identifier[512];
    char buf[10000];

    snprintf(identifier, sizeof(identifier), "%s/%d.s%d", directory, head.file, head.stripe);
    int local = open(identifier, O_CREAT | O_RDWR, S_IWUSR | S_IRUSR );

    int remaining = head.size;
    int n = 1;
    lseek64(local, head.offset, SEEK_SET);
    while(remaining > 0 && n > 0) {
        n = read(remote, buf, remaining > sizeof(buf) ? sizeof(buf) : remaining);
        remaining -= n;
        write(local, buf, n);
    }

    close(local);

    return 0;
}

// This method is called when a command to retrieve data from the object
// store is received.
int retrieve_data(int remote, struct obj_header head) {
    char identifier[512];
    char buf[10000];

    snprintf(identifier, sizeof(identifier), "%s/%d.s%d", directory, head.file, head.stripe);
    int local = open(identifier, O_RDONLY);

    int remaining = head.size;
    int n = 1;
    lseek64(local, head.offset, SEEK_SET);
    while(remaining > 0 && n > 0) {
        n = read(local, buf, remaining > sizeof(buf) ? sizeof(buf) : remaining);
        remaining -= n;
        write(remote, buf, n);
    }

    close(local);

    return 0;
}

// This method reads the next command in the input stream, on a 
// particular socket, to determine whether to read or write.
int dispatch(int fd) {
    char nbuf[1];
    struct obj_header head;
    do {
        read(fd, &head, sizeof(head));
        if(head.op == OP_WRITE) {
            store_data(fd, head);
        }
        else if(head.op == OP_READ) {
            retrieve_data(fd, head);
        }
        else if(head.op == OP_NONE) {
            send(fd, nbuf, sizeof(char), 0);
        }
    } while (head.op != OP_DONE);
}

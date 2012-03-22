/*
   This is the NativeFS implementation for the object servers, which
   stores data on an underlying filesystem.
*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <dirent.h>
#include <signal.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <netinet/in.h>

#include "Structures.h"

static char* directory;
static char buffer[BLOCK_SIZE];

void error(const char* msg)
{
    perror(msg);
    exit(1);
}

int main(int argc, char** argv) {
    int sockfd, newsockfd, port;
    socklen_t clilen;
    struct sockaddr_in serv_addr, cli_addr;
    int n;
    if (argc < 3) {
        fprintf(stderr, "USAGE: ./obj_nativefs <directory> <port>\n");
        exit(1);
    }
    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0)
        error("Can't create socket!");
    bzero((char *) &serv_addr, sizeof(serv_addr));
    directory = argv[1];
    port = atoi(argv[2]);
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_addr.s_addr = INADDR_ANY;
    serv_addr.sin_port = htons(port);
    if(bind(sockfd, (struct sockaddr*) &serv_addr, sizeof(serv_addr)) < 0)
        error("Can't bind to port!");

    /* We don't want to fill up the process table. */
    signal(SIGCHLD, SIG_IGN);
    listen(sockfd, 25);

    while( 1 ) {
        clilen = sizeof(cli_addr);
        newsockfd = accept(sockfd, (struct sockaddr*) &cli_addr, &clilen);
        if (newsockfd < 0)
            error("Error on accept.");

        pid_t pID = fork();
        if(pID == 0) {
            // close socket fd for the handler
            close(sockfd);
            dispatch(newsockfd);
            close(newsockfd);
            exit(0);
        }
        // close socket fd for the listener
        close(newsockfd);
    }

    close(sockfd);

    return 0;
}

int store_data(int fd, struct obj_header head) {
    // fprintf(stderr, "request to store data...");
    // fprintf(stderr, "file request size: %zu, offset: %jd, file: %d, stripe: 
    // %d\n", head.size, (intmax_t)head.offset, head.file, head.stripe);

    char identifier[512];
    snprintf(identifier, sizeof(identifier), "%s/%d.s%d", directory, head.file, head.stripe);
    int local = open(identifier, O_CREAT | O_RDWR, S_IWUSR | S_IRUSR );

    int n;
    lseek(local, head.offset, SEEK_SET);
    while(n = read(fd, &buffer, sizeof(buffer))) {
        fflush(stderr);
        write(local, buffer, n);
    }

    close(local);
    // close(fd);

    return 0;
}

int retrieve_data(int remote, struct obj_header head) {
    char identifier[256];
    char buf[10000];

    snprintf(identifier, sizeof(identifier), "%s/%d.s%d", directory, head.file, head.stripe);
    int local = open(identifier, O_RDONLY);

    int remaining = head.size;
    int n = 1;
    lseek(local, head.offset, SEEK_SET);
    while(remaining > 0 && n > 0) {
        n = read(local, buf, remaining > sizeof(buf) ? sizeof(buf) : remaining);
        remaining -= n;
        //while(n > 0) {
        write(remote, buf, n);
        //}
    }

    close(local);
    // close(remotefd);

    return 0;
}

int dispatch(int fd) {
    struct obj_header head;
    do {
        read(fd, &head, sizeof(head));
        if(head.op == OP_WRITE) {
            store_data(fd, head);
        }
        else if(head.op == OP_READ) {
            retrieve_data(fd, head);
        }
    } while (head.op != OP_DONE);
}

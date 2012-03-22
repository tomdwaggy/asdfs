#include <stddef.h>
#include <stdio.h>
#include <string.h>
#include <netdb.h>
#include <netinet/in.h>

#include "Logger.h"
#include "Structures.h"

int connect_to_server_t(struct asd_host host, int socktype)
{
    int sockfd;
    struct sockaddr_in serv_addr;
    struct hostent *server;

    sockfd = socket(AF_INET, socktype, 0);
    if (sockfd < 0)
        log("Error opening socket");
    server = gethostbyname(host.hostname);
    if (server == NULL) {
        log("No such host.\n");
        return -1;
    }

    bzero((char*) &serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    bcopy((char*)server->h_addr,(char*)&serv_addr.sin_addr.s_addr,server->h_length);
    serv_addr.sin_port = htons(host.port);
    if(connect(sockfd,(const struct sockaddr*)&serv_addr,sizeof(serv_addr))<0) {
        log("Error connecting to server: %s port %d", host.hostname, host.port);
    }

    return sockfd;
}

int connect_to_server(struct asd_host host) {
    return connect_to_server_t(host, SOCK_STREAM);
}

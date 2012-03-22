/*
   This is the SQLite Implementation for the Metadata
   server's database.
*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>

#include <sqlite3.h>

#include <sys/stat.h>

static sqlite3 *md_db;

int md_open() {
    char *errMsg = NULL;
    int rc;

    rc = sqlite3_open("metadata.db", &md_db);
    if( rc ){
        fprintf(stderr, "Can't open database: %s\n", sqlite3_errmsg(md_db));
        sqlite3_close(md_db);
        exit(1);
    }
}

void md_close() {
    sqlite3_close(md_db);
}

void md_clerr(int rc) {
    if( rc ) {
        fprintf(stderr, "SQLite Error: %s\n", sqlite3_errmsg(md_db));
        md_close();
    }
}

void md_done(int rc) {
    if( rc != SQLITE_DONE ) {
        fprintf(stderr, "Operation not performed.\n");
    }
}

static int md_truncate(const char* path, off_t size) {
    sqlite3_stmt *stmt = NULL;
    const char *tail = NULL;
    int rc;
    rc = sqlite3_prepare(md_db,
        "UPDATE File SET bytes=? WHERE name=?", 1024, &stmt, &tail);
    md_clerr(rc);
    sqlite3_bind_int(stmt, 1, size);
    sqlite3_bind_text(stmt, 2, path, strlen(path), SQLITE_STATIC);
    rc = sqlite3_step(stmt);
    md_done(rc);
    return 0;
}

static int md_chown(const char* path, uid_t uid, gid_t gid) {
    sqlite3_stmt *stmt = NULL;
    const char *tail = NULL;
    int rc;
    rc = sqlite3_prepare(md_db,
        "UPDATE File SET uid=?, gid=? WHERE name=?", 1024, &stmt, &tail);
    md_clerr(rc);
    sqlite3_bind_int(stmt, 1, uid);
    sqlite3_bind_int(stmt, 2, gid);
    sqlite3_bind_text(stmt, 3, path, strlen(path), SQLITE_STATIC);
    rc = sqlite3_step(stmt);
    md_done(rc);
    return 0;
}

static int md_mknod(const char* path, mode_t mode) {
    sqlite3_stmt *stmt = NULL;
    const char *tail = NULL;
    int rc;
    rc = sqlite3_prepare(md_db,
        "INSERT INTO File (name, mode, mtime, atime, ctime) VALUES (?, ?, strftime('%s','now'), strftime('%s','now'), strftime('%s','now'))", 1024, &stmt, &tail );
    md_clerr(rc);
    sqlite3_bind_text(stmt, 1, path, strlen(path), SQLITE_STATIC);
    sqlite3_bind_int(stmt, 2, mode);
    rc = sqlite3_step(stmt);
    md_done(rc);

    return 0;
}

static int md_unlink(const char* path) {
    sqlite3_stmt *stmt = NULL;
    const char *tail = NULL;
    int rc;
    rc = sqlite3_prepare(md_db,
        "DELETE FROM File WHERE name = ?",
        1024,
        &stmt, &tail );
    md_clerr(rc);
    sqlite3_bind_text(stmt, 1, path, strlen(path), SQLITE_STATIC);
    rc = sqlite3_step(stmt);
    md_done(rc);
    return 0;
}

static int md_readdir_cb(void *sock, int argc, char **argv, char **cName) {
    int i;
    for(i=0; i<argc; i++){
        if(strcmp(cName[i], "name") == 0) {
            fprintf(sock, "%s\n", argv[i] ? argv[i] : "");
        }
    }
    fflush(sock);
    return 0;
}

static int md_readdir(void *sock, const char* path) {
    char *errMsg = NULL;
    int rc;
    rc = sqlite3_exec(md_db,
        "SELECT name FROM File",
        md_readdir_cb,
        sock,
        &errMsg );
    md_clerr(rc);
    return 0;
}

static int md_getattr(void *sock, const char* path) {
    char *errMsg = NULL;
    const char *tail = NULL;
    sqlite3_stmt *stmt = NULL;
    int rc;
    rc = sqlite3_prepare(md_db,
        "SELECT mode,bytes,uid,gid,atime,mtime,ctime FROM File WHERE name=?", 1024, &stmt, &tail );
    sqlite3_bind_text(stmt, 1, path, strlen(path), SQLITE_STATIC);
    if((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        fprintf(sock, "%d %d %d %d %d %d\n", sqlite3_column_int(stmt, 0),
                                    sqlite3_column_int(stmt, 1),
                                    sqlite3_column_int(stmt, 2),
                                    sqlite3_column_int(stmt, 3),
                                    sqlite3_column_int(stmt, 4),
                                    sqlite3_column_int(stmt, 5),
                                    sqlite3_column_int(stmt, 6));
        fflush(sock);
    }
    return 0;
}

static int md_getobjects(void *sock) {
    char *errMsg = NULL;
    const char *tail = NULL;
    sqlite3_stmt *stmt = NULL;
    int rc;
    rc = sqlite3_prepare(md_db,
        "SELECT stripe,mirror,addr,port FROM Shard NATURAL JOIN StoreAddress",
        1024, &stmt, &tail );
    while((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        fprintf(sock, "%d %d %s %d\n",
                                    sqlite3_column_int(stmt, 0),
                                    sqlite3_column_int(stmt, 1),
                                    sqlite3_column_text(stmt, 2),
                                    sqlite3_column_int(stmt, 3));
        fflush(sock);
    }
    return 0;
}

static int md_getfile(void *sock, const char* path) {
    char *errMsg = NULL;
    const char *tail = NULL;
    sqlite3_stmt *stmt = NULL;
    int rc;
    rc = sqlite3_prepare(md_db,
        "SELECT file FROM File WHERE name=?", 1024, &stmt, &tail );
    sqlite3_bind_text(stmt, 1, path, strlen(path), SQLITE_STATIC);
    if((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        fprintf(sock, "%d\n", sqlite3_column_int(stmt, 0));
        fflush(sock);
    } else {
        fprintf(sock, "-1\n");
        fflush(sock);
    }
    return 0;
}

void error(const char* msg)
{
    perror(msg);
    exit(1);
}

int main(int argc, char** argv) {
    md_open();

    int sockfd, newsockfd, port;
    socklen_t clilen;
    char buffer[256];
    struct sockaddr_in serv_addr, cli_addr;
    int n;
    if (argc < 2) {
        fprintf(stderr, "Error: No port number\n");
        exit(1);
    }
    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0)
        error("Can't create socket!");
    bzero((char *) &serv_addr, sizeof(serv_addr));
    port = atoi(argv[1]);
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_addr.s_addr = INADDR_ANY;
    serv_addr.sin_port = htons(port);
    if(bind(sockfd, (struct sockaddr*) &serv_addr, sizeof(serv_addr)) < 0)
        error("Can't bind to port!");

    while( 1 ) {
        listen(sockfd, 25);
        clilen = sizeof(cli_addr);
        newsockfd = accept(sockfd, (struct sockaddr*) &cli_addr, &clilen);
        if (newsockfd < 0)
            error("Error on accept.");
        bzero(buffer, 256);
        n = read(newsockfd, buffer, 255);
        if (n < 0)
            error("Error reading from socket.");
        n = dispatch(strdup(buffer), newsockfd);
        if (n < 0)
            error("Error dispatching command.");
        close(newsockfd);
    }
 
    close(sockfd);

    md_close();
    return 0;
}

int dispatch(char* str, int client) {
    FILE* stream = fdopen(client, "w");
    char* cmd;
    cmd = strtok(str, "|");
    if(strcmp(cmd, "readdir") == 0) {
        printf("'readdir' received\n");
        md_readdir(stream, "/");
    } else if(strcmp(cmd, "mknod") == 0) {
        printf("'mknod' received\n");
        char* name = strtok(NULL, "|");
        int mode = atoi(strtok(NULL, "|"));
        md_mknod(name, mode);
    } else if(strcmp(cmd, "unlink") == 0) {
        printf("'unlink' received\n");
        char* name = strtok(NULL, "|");
        md_unlink(name);
    } else if(strcmp(cmd, "truncate") == 0) {
        char* name = strtok(NULL, "|");
        int size = atoi(strtok(NULL, "|"));
        printf("'truncate' '%s' '%d' received\n", name, size);
        md_truncate(name, size);
    } else if(strcmp(cmd, "getattr") == 0) {
        char* name = strtok(NULL, "|");
        printf("'getattr' '%s' received\n", name);
        md_getattr(stream, name);
    } else if(strcmp(cmd, "getobjects") == 0) {
        printf("'getobjects' received\n");
        md_getobjects(stream);
    } else if(strcmp(cmd, "getfile") == 0) {
        printf("'getfile' received\n");
        char* name = strtok(NULL, "|");
        md_getfile(stream, name);
    } else if(strcmp(cmd, "chown") == 0) {
        printf("'chown' received\n");
        char* name = strtok(NULL, "|");
        uid_t uid = atoi(strtok(NULL, "|"));
        gid_t gid = atoi(strtok(NULL, "|"));
        md_chown(name, uid, gid);
    }
    return 0;
}

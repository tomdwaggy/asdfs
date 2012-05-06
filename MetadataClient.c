#include <string.h>
#include <stdio.h>
#include <stddef.h>
#include <sys/types.h>
#include <errno.h>
#include <fuse.h>

#include "ClientConnectionPool.h"
#include "Structures.h"
#include "Logger.h"

int mdc_mknod(struct asd_host host, const char* path, mode_t mode){
    log("Sending command to server to create node.");
    int sockfd, n;
    sockfd = connect_to_server(host);
    char str[512];
    snprintf(str, sizeof(str), "mknod|%s|%d", path, mode);
    n = write(sockfd, str, strlen(str));
    close(sockfd);
    return 0;
}

int mdc_chown(struct asd_host host, const char* path, uid_t uid, gid_t gid){
    log("Sending command to server to change node owner.");
    int sockfd, n;
    sockfd = connect_to_server(host);
    char str[512];
    snprintf(str, sizeof(str), "chown|%s|%d|%d", path, uid, gid);
    n = write(sockfd, str, strlen(str));
    close(sockfd);
    return 0;
}

int mdc_truncate(struct asd_host host, const char* path, unsigned long long size){
    log("Sending command to server to truncate node.");
    int sockfd, n;
    sockfd = connect_to_server(host);
    char str[512];
    snprintf(str, sizeof(str), "truncate|%s|%llu", path,  (unsigned long long) size);
    n = write(sockfd, str, strlen(str));
    close(sockfd);
    return 0;
}

int mdc_unlink(struct asd_host host, const char* path){
    log("Sending command to server to unlink node.");
    int sockfd, n;
    sockfd = connect_to_server(host);
    char str[512];
    snprintf(str, sizeof(str), "unlink|%s", path);
    n = write(sockfd, str, strlen(str));
    close(sockfd);
    return 0;
}

int mdc_readdir(struct asd_host host, const char *path, void *buf, 
                        fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi) {
    int sockfd, n;
    log("Reading directory data from server.");
    sockfd = connect_to_server(host);
    n = write(sockfd, "readdir", strlen("readdir"));
    FILE* stream = fdopen(sockfd, "r");
    char str[512];
    while(fgets(str, sizeof(str), stream)) {
        int len = strlen(str);
        str[len - 1] = '\0';
        char* name = strdup(str);
        filler( buf , name, NULL, 0);
    }
    fclose(stream);
    close(sockfd);

    return 0;
}

struct asd_objects mdc_getobjects(struct asd_host host) {
    int sockfd, n;
    struct asd_objects objs;
    log("Reading file objects data from server.");
    sockfd = connect_to_server(host);
    char str[512];
    snprintf(str, sizeof(str), "getobjects");
    n = write(sockfd, str, strlen(str));
    log("Written request");
    FILE* stream = fdopen(sockfd, "r");
    while(fgets(str, sizeof(str), stream)) {
        int stripe, mirror, port;
        char hostname[512];
        int rc = sscanf(str, "%d %d %512s %d", &stripe, &mirror, hostname, &port);
        if(rc > 0) {
            struct asd_host newhost;
            newhost.hostname = strdup(hostname);
            newhost.port = port;
            objs.array[stripe][mirror] = newhost;
            objs.pools[stripe][mirror] = create_pool(newhost, 10);
            log("%s hostname %d port %d str %d mirr", hostname, port, stripe, mirror);
        } else {
            break;
        }
    }
    fclose(stream);
    close(sockfd);

    return objs;
}

int mdc_getattr(struct asd_host host, const char* path, struct stat* stbuf) {
    int sockfd, n;
    log("Getting attributes from server.");
    sockfd = connect_to_server(host);
    char str[512];
    snprintf(str, sizeof(str), "getattr|%s", path);
    n = write(sockfd, str, strlen(str));
    FILE* stream = fdopen(sockfd, "r");
    fgets(str, sizeof(str), stream);
    int mode, size, uid, gid, atime, mtime, ctime;
    int rc = sscanf(str, "%d %d %d %d %d %d %d", &mode, &size, &uid, &gid, &atime, &mtime, &ctime);
    if(rc > 0) {
        stbuf->st_mode = mode;
        stbuf->st_size = size;
        stbuf->st_uid  = uid;
        stbuf->st_gid  = gid;
        stbuf->st_atime= atime;
        stbuf->st_mtime= mtime;
        stbuf->st_ctime= ctime;
        log("%i %jd %i", stbuf->st_mode, (intmax_t)stbuf->st_size, stbuf->st_uid);
    } else {
        log("File not found on server.");
        return -ENOENT;
    }
    fclose(stream);
    close(sockfd);

    return 0;
}

int mdc_getfile(struct asd_host host, const char* path) {
    int sockfd, n;
    int ret = 0;

    log("Getting attributes from server.");
    sockfd = connect_to_server(host);
    char str[512];
    snprintf(str, sizeof(str), "getfile|%s", path);
    n = write(sockfd, str, strlen(str));
    FILE* stream = fdopen(sockfd, "r");
    fgets(str, sizeof(str), stream);
    int file_num;
    int rc = sscanf(str, "%d", &file_num);

    if(rc < 0) {
        log("File not found on server.");
        ret = -ENOENT;
    } else {
        ret = file_num;
    }

    fclose(stream);
    close(sockfd);

    return ret;
}

#define FUSE_USE_VERSION 26
#define PACKAGE_VERSION 1

#include <stddef.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <pthread.h>
#include <signal.h>
#include <fuse.h>

#include "Logger.h"
#include "Structures.h"

#include "GenericClient.h"
#include "MetadataClient.h"
#include "ObjectClient.h"

pthread_rwlock_t lock;

struct asd_config {
    char* hostname;
    char* slavehostname;
    int port;
    int slaveport;
};

enum {
    KEY_HELP,
    KEY_VERSION,
};

static struct asd_file last_file;
static struct asd_file last_read;

static struct asd_host host;
static struct asd_host slave;
static struct asd_objects g_objs;

static int asd_mknod(const char *path, mode_t mode, dev_t dev)
{
    log("mknod called");
    mdc_mknod(host, slave, path + 1, mode);
    return 0;
}

static int asd_unlink(const char *path)
{
    log("unlink called");
    mdc_unlink(host, slave, path + 1);
    return 0;
}

static int asd_rename(const char *path, const char *newpath)
{
    log("rename called");
    return 0;
}

static int asd_chmod(const char *path, mode_t mode)
{
    log("chmod called");
    return 0;
}

static int asd_chown(const char *path, uid_t uid, gid_t gid)
{
    log("chown called");
    mdc_chown(host, slave, path + 1, uid, gid);
    return 0;
}

static int asd_truncate(const char *path, off_t newsize)
{
    log("truncate called");
    mdc_truncate(host, slave, path + 1, newsize);
    return 0;
}

static int asd_utime(const char *path, struct utimbuf *ubuf)
{
    log("utime called");
    return 0;
}

static int asd_getattr(const char *path, struct stat *stbuf)
{
    log("getattr called");
    int res = 0;
    memset(stbuf, 0, sizeof(struct stat));
    if(strcmp(path, "/") == 0) {
        stbuf->st_mode = S_IFDIR | 0755;
        stbuf->st_nlink = 2;
    }
    else {
        stbuf->st_nlink = 1;
        res = mdc_getattr(host, slave, path + 1, stbuf);
    }

    return res;
}

static int asd_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
                         off_t offset, struct fuse_file_info *fi)
{
    (void) offset;
    (void) fi;

    if(strcmp(path, "/") != 0)
        return -ENOENT;

    filler(buf, ".", NULL, 0);
    filler(buf, "..", NULL, 0);
    mdc_readdir(host, slave, path, buf, filler, offset, fi);

    return 0;
}

static int asd_open(const char *path, struct fuse_file_info *fi)
{
    log("open called");

    int file_num = mdc_getfile(host, slave, path + 1);
    if(file_num < 0)
        return -ENOENT;

    log("[FUSE][INFO] File number returned is %d", file_num);

    struct asd_file* fbuf = malloc(sizeof(struct asd_file));
    mdc_getattr(host, slave, path + 1, &fbuf->stbuf);
    fbuf->path = strdup(path);
    fbuf->id = file_num;
    fi->fh = (uintptr_t) fbuf;

    return 0;
}

static int asd_release(const char *path, struct fuse_file_info *fi)
{
    log("release called");

    int file_num = mdc_getfile(host, slave, path + 1);
    if(file_num < 0)
        return -ENOENT;

    struct asd_file* fbuf = (struct asd_file*) fi->fh;
    free(fbuf->path);
    free(fbuf);

    return 0;
}

static int asd_read(const char *path, char *buf, size_t size, off_t offset,
                    struct fuse_file_info *fi)
{
    log("read called");

    pthread_rwlock_rdlock(&lock);

    struct asd_file* fbuf = (struct asd_file*) fi->fh;
    int file_num  = fbuf->id;
    off_t file_size = fbuf->stbuf.st_size;

    if(size + offset > file_size)
        size = file_size - offset;

    off_t block = offset / BLOCK_SIZE;
    off_t offInBlock = offset % BLOCK_SIZE;
    off_t bytesLeft = BLOCK_SIZE - offInBlock;
    int stripeNumber = block % NUMBER_STRIPES;

    if(size > bytesLeft) {
        read_mirrored(g_objs.pools[stripeNumber], NUMBER_MIRRORS, buf, bytesLeft, offInBlock + (BLOCK_SIZE * (block/NUMBER_STRIPES)), file_num, stripeNumber);
        block++;
        stripeNumber = block % NUMBER_STRIPES;
        off_t unread = size - bytesLeft;
        read_mirrored(g_objs.pools[stripeNumber], NUMBER_MIRRORS, buf + bytesLeft, unread, BLOCK_SIZE * (block/NUMBER_STRIPES), file_num, stripeNumber);
    } else {
        read_mirrored(g_objs.pools[stripeNumber], NUMBER_MIRRORS, buf, size, offInBlock + (BLOCK_SIZE * (block/NUMBER_STRIPES)), file_num, stripeNumber);
    }

    pthread_rwlock_unlock(&lock);

    return size;
}

static int asd_write(const char *path, const char *buf, size_t size, off_t offset,
                     struct fuse_file_info *fi)
{
    log("write called(size=%zu,offset=%jd offset", size,(intmax_t)offset);

    pthread_rwlock_wrlock(&lock);

    struct asd_file* fbuf = (struct asd_file*) fi->fh;
    int file_num  = fbuf->id;
    off_t file_size = fbuf->stbuf.st_size;

    // Enlarge the file size to size + offset, if we are writing past
    // the current EOF. Do not persist this with the metadata server,
    // due to major bottleneck issues. Only persist on flushing the
    // buffer.
    if(size + offset > file_size) {
        file_size = size + offset;
        fbuf->stbuf.st_size = file_size;
    }

    off_t block = offset / BLOCK_SIZE;
    off_t offInBlock = offset % BLOCK_SIZE;
    off_t bytesLeft = BLOCK_SIZE - offInBlock;
    int stripeNumber = block % NUMBER_STRIPES;

    if(size > bytesLeft) {
        write_mirrored(g_objs.pools[stripeNumber], NUMBER_MIRRORS, buf, bytesLeft, offInBlock + (BLOCK_SIZE * (block/NUMBER_STRIPES)), file_num, stripeNumber);
        block++;
        stripeNumber = block % NUMBER_STRIPES;
        off_t unwritten = size - bytesLeft;
        write_mirrored(g_objs.pools[stripeNumber], NUMBER_MIRRORS, buf + bytesLeft, unwritten, BLOCK_SIZE * (block/NUMBER_STRIPES), file_num, stripeNumber);
    } else {
        write_mirrored(g_objs.pools[stripeNumber], NUMBER_MIRRORS, buf, size, offInBlock + (BLOCK_SIZE * (block/NUMBER_STRIPES)), file_num, stripeNumber);
    }

    int p, q;
    for(q = 0; q < NUMBER_STRIPES; q++) {
        for(p = 0; p < NUMBER_MIRRORS; p++) {
            if(g_objs.pools[q][p]->invalidate == 1) {
                mdc_invalidate(host, slave, file_num, p);
            }
        }
    }

    pthread_rwlock_unlock(&lock);

    return size;
}

static int asd_statfs(const char *path, struct statvfs *statv)
{
    log("statfs called");
    return 0;
}

static int asd_flush(const char *path, struct fuse_file_info *fi)
{
    log("flush called on %s with last %s", path, last_file.path);

    struct asd_file* fbuf = (struct asd_file*) fi->fh;
    int file_num  = fbuf->id;
    off_t file_size = fbuf->stbuf.st_size;

    mdc_truncate(host, slave, path + 1, file_size);

    return 0;
}

static int asd_fsync(const char *path, int datasync, struct fuse_file_info *fi)
{
    log("fsync called");
    return 0;
}

#define ASD_OPT(t, p, v) { t, offsetof(struct asd_config, p), v }

static struct fuse_opt asd_opts[] = {
    ASD_OPT("host=%s", hostname, 0),
    ASD_OPT("slavehost=%s", slavehostname, 0),
    ASD_OPT("port=%i", port, 0),
    ASD_OPT("slaveport=%i", slaveport, 0),

    FUSE_OPT_KEY("-V", KEY_VERSION),
    FUSE_OPT_KEY("--version", KEY_VERSION),
    FUSE_OPT_KEY("-h", KEY_HELP),
    FUSE_OPT_KEY("--help", KEY_HELP),
    FUSE_OPT_END
};

static struct fuse_operations asd_oper = {
    .mknod   = asd_mknod,
    .getattr = asd_getattr,
    .chown   = asd_chown,
    .readdir = asd_readdir,
    .open    = asd_open,
    .release = asd_release,
    .read    = asd_read,
    .unlink  = asd_unlink,
    .write   = asd_write,
    .flush   = asd_flush,
    .truncate = asd_truncate
};

static int asd_opt_proc(void *data, const char *arg, int key, struct fuse_args *outargs)
{
    switch(key) {
    case KEY_HELP:
        fprintf(stderr,
            "usage: %s mountpoint [options]\n"
            "\n"
            "general options:\n"
            "    -o opt,[opt...]  mount options\n"
            "    -h   --help      print help\n"
            "    -V   --version   print version\n"
            "\n"
            "asdfs options:\n"
            "    -o host=STRING\n  hostname of metadata server\n"
            "    -o port=NUM\n     port of metadata server\n"
            "    -o slavehost=STRING\n hostname of slave metadata server\n"
            "    -o slaveport=NUM\n port of slave metadata server\n"
            , outargs->argv[0]);
        fuse_opt_add_arg(outargs, "-ho");
        fuse_main(outargs->argc, outargs->argv, &asd_oper, NULL);
        exit(1);

    case KEY_VERSION:
        fprintf(stderr, "asdfs version %d\n", PACKAGE_VERSION);
        fuse_opt_add_arg(outargs, "--version");
        fuse_main(outargs->argc, outargs->argv, &asd_oper, NULL);
        exit(0);
    }
    return 1;
}

int main(int argc, char *argv[])
{
    struct fuse_args args = FUSE_ARGS_INIT(argc, argv);
    struct asd_config conf;

    struct sigaction new_actn, old_actn;
    new_actn.sa_handler = SIG_IGN;
    sigemptyset (&new_actn.sa_mask);
    new_actn.sa_flags = 0;
    sigaction (SIGPIPE, &new_actn, &old_actn);

    memset(&conf, 0, sizeof(conf));
    fuse_opt_parse(&args, &conf, asd_opts, asd_opt_proc);
    host.hostname = conf.hostname;
    host.port = conf.port;
    slave.hostname = conf.slavehostname;
    slave.port = conf.slaveport;
    g_objs = mdc_getobjects(host, slave);

    return fuse_main(args.argc, args.argv, &asd_oper, NULL);
}

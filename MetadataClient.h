int mdc_invalidate(struct asd_host host, struct asd_host slave, int file, int store);

int mdc_mknod(struct asd_host host, struct asd_host slave, const char* path, mode_t mode);

int mdc_chown(struct asd_host host, struct asd_host slave, const char* path, uid_t uid, gid_t gid);

int mdc_truncate(struct asd_host host, struct asd_host slave, const char* path, off_t size);

int mdc_unlink(struct asd_host host, struct asd_host slave, const char* path);

int mdc_readdir(struct asd_host host, struct asd_host slave, const char *path, void *buf,
                   fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi);

struct asd_objects mdc_getobjects(struct asd_host host, struct asd_host slave);

int mdc_getattr(struct asd_host host, struct asd_host slave, const char* path, struct stat* stbuf);

int mdc_getfile(struct asd_host host, struct asd_host slave, const char* path);

int mdc_getinvalid(struct asd_host host, struct asd_host slave, const char* hostname, int port, int* array, intmax_t* sarray, int maximum);

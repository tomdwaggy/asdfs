int write_remote(struct asd_pool* host, const char* buf, struct obj_header head);

int read_remote(struct asd_pool* host, char* buf, struct obj_header head);

int write_mirrored(struct asd_pool** pools, int nhosts, const char* buf, size_t size, off_t offset, int file, int stripe);

int read_mirrored(struct asd_pool** pools, int nhosts, char* buf, size_t size, off_t offset, int file, int stripe);

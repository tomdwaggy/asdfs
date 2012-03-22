#include <pthread.h>

#include "Structures.h"

struct asd_pool* create_pool(struct asd_host host, int num_connections);

int get_connection_b(struct asd_pool* pool);

int release_connection(struct asd_pool* pool, int fd);

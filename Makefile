CFLAGS=-D_FILE_OFFSET_BITS=64 -g -lpthread
all:
	gcc ${CFLAGS} -lsqlite3 MetadataSQLite.c -o MetadataSQLite
	gcc ${CFLAGS} ObjectNativeFS.c -o ObjectNativeFS
	gcc ${CFLAGS} -lfuse ClientConnectionPool.c GenericClient.c ObjectClient.c MetadataClient.c asdfs.c -o asdfs
clean:
	rm ObjectNativeFS
	rm MetadataSQLite
	rm asdfs

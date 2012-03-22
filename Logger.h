#ifdef NDEBUG
#   define log(format, ...)
#else
#   define log(format, ...) \
{ \
        FILE *FH = fopen( "/tmp/dfs.log", "a" ); \
        if( FH ) { \
                fprintf( FH, "l. %4d: " format "\n", __LINE__, ##__VA_ARGS__ ); \
                fclose( FH ); \
        } \
}
#endif


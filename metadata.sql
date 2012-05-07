-- *******************************************************************
--  metadata server
--   Usage:
--       $ sqlite3 metadata.db < metadata.sql
--
-- *******************************************************************
-- *******************************************************************

-- File table, containing file id, filename, and other parameters
-- which are required by the stat interface. Replication is the type
-- of replication it will use, either mirrored or spanned.
BEGIN;

CREATE TABLE File (
                   file            INTEGER PRIMARY KEY,
                   name            TEXT UNIQUE NOT NULL,
                   uid             INTEGER,
                   gid             INTEGER,
                   bytes           INTEGER,
                   blocksize       INTEGER,
                   blocks          INTEGER,
                   mode            INTEGER,
                   atime           INTEGER,
                   mtime           INTEGER,
                   ctime           INTEGER,
                   repl            INTEGER
);

-- INSERT INTO File VALUES (1, "Hello", 1000, 1000, 11, 512, 1, 33188, 0, 0, 0, 1);

-- Invalidated data table, which contains file id and store stating
-- that a file is invalidated on a particular data store.

CREATE TABLE Invalid (
                        inv         INTEGER PRIMARY KEY,
                        file        INTEGER NOT NULL,
                        store       INTEGER NOT NULL
);

-- StoreAddress relation, containing a mapping from store ids to the
-- server address, which is just an IP or hostname which must be
-- resolved onthe client.

CREATE TABLE StoreAddress (store    INTEGER PRIMARY KEY,
                           addr     TEXT NOT NULL,
                           port     INTEGER NOT NULL
);

INSERT INTO StoreAddress VALUES (0, "localhost", 2090);
INSERT INTO StoreAddress VALUES (1, "localhost", 2091);

-- Shard table, stripe #, mirror #, and the store id, which refers to a 
-- store in StoreAddress. Used to contain a file, and was going to have
-- per file replication semantics, now replication is per file system.

CREATE TABLE Shard (
                         stripe     INTEGER NOT NULL,
                         mirror     INTEGER NOT NULL,
                         store      INTEGER NOT NULL
);

INSERT INTO Shard VALUES (0, 0, 0);
INSERT INTO Shard VALUES (1, 0, 1);
INSERT INTO Shard VALUES (0, 1, 1);
INSERT INTO Shard VALUES (1, 1, 0);

END;

-- *******************************************************************
-- *******************************************************************


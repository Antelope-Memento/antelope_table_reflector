CREATE TABLE SYNC
(
 sourceid          INT PRIMARY KEY,
 block_num         BIGINT NOT NULL,
 block_time        TIMESTAMP WITHOUT TIME ZONE NOT NULL,
 irreversible      BIGINT NOT NULL,
 is_master         SMALLINT NOT NULL,
 last_updated      TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT '2000-01-01'
);

INSERT INTO SYNC (sourceid, block_num, block_time, irreversible, is_master) values (1,0, '2020-01-10 00:00',0, 1);


/*
   table journaling tables for fork management
   selector is sha256 on contract:scope:table:pk 
*/

CREATE TABLE JCURRENT_1
(
 selector      BYTEA PRIMARY KEY,
 jsdata        BYTEA                  /* JSON string from Chronicle table update */
);

CREATE TABLE JUPDATES_1
(
 seqnum        BIGSERIAL PRIMARY KEY,
 block_num     BIGINT NOT NULL,
 op            SMALLINT NOT NULL,     /* row operation: 1=new, 2=modified, 3=deleted */
 selector      BYTEA NOT NULL,
 prev_jsdata   BYTEA,                 /* previous content in table row */
 new_jsdata    BYTEA                  /* new content in table row */
);

CREATE INDEX JUPDATES_1_I01 ON JUPDATES_1 (block_num, seqnum);


CREATE TABLE JCURRENT_2
(
 selector      BYTEA PRIMARY KEY,
 jsdata        BYTEA                  /* JSON string from Chronicle table update */
);

CREATE TABLE JUPDATES_2
(
 seqnum        BIGSERIAL PRIMARY KEY,
 block_num     BIGINT NOT NULL,
 op            SMALLINT NOT NULL,     /* row operation: 1=new, 2=modified, 3=deleted */
 selector      BYTEA NOT NULL,
 prev_jsdata   BYTEA,                 /* previous content in table row */
 new_jsdata    BYTEA                  /* new content in table row */
);

CREATE INDEX JUPDATES_2_I01 ON JUPDATES_2 (block_num, seqnum);


/* this is only required as a lock between master and slave */
CREATE TABLE WRITER_LOCK
(
 dummy         INT PRIMARY KEY
);



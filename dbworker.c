// Author: Vimukthi
#include <zmq.h>
#include <msgpack.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <assert.h>
#include <sqlite3.h> 
#include <zhelpers.h>

const char* INSERT_STMT = "1";
const char* SELECT_STMT = "2";

static int callback(void* NotUsed, int argc, char** argv, char** azColName) {
    int i;
    for (i = 0; i < argc; i++) {
        printf("%s = %s\n", azColName[i], argv[i] ? argv[i] : "NULL");
    }
    printf("\n");
    return 0;
}

static sqlite3* create_db() {
    sqlite3* db;
    char* zErrMsg = 0;
    char* sql;
    int rc;
    /* Open database */
    rc = sqlite3_open(":memory:", &db);
    if (rc) {
        fprintf(stderr, "Can't open database: %s\n", sqlite3_errmsg(db));
        exit(0);
    } else {
        fprintf(stdout, "Opened database successfully\n");
    }

    // Create SQL statement 
    sql = "CREATE TABLE COMPANY ("  \
         "ID INT NOT NULL," \
         "NAME           TEXT    NOT NULL," \
         "AGE            INT     NOT NULL," \
         "ADDRESS1        CHAR(50)," \
         "ADDRESS2        CHAR(50)," \
         "ADDRESS3        CHAR(50)," \
         "ADDRESS4        CHAR(50)," \
         "ADDRESS5        CHAR(50)," \
         "ADDRESS6        CHAR(50)," \
         "ADDRESS7        CHAR(50)," \
         "ADDRESS8        CHAR(50)," \
         "ADDRESS9        CHAR(50)," \
         "ADDRESS10        CHAR(50)," \
         "ADDRESS11        CHAR(50)," \
         "ADDRESS12        CHAR(50)," \
         "ADDRESS13        CHAR(50)," \
         "ADDRESS14        CHAR(50)," \
         "ADDRESS15        CHAR(50)," \
         "ADDRESS16        CHAR(50)," \
         "ADDRESS17        CHAR(50)," \
         "ADDRESS18        CHAR(50)," \
         "ADDRESS19        CHAR(50)," \
         "ADDRESS20        CHAR(50)," \
         "SALARY         REAL );";

    /* Execute SQL statement */
    rc = sqlite3_exec(db, sql, callback, 0, &zErrMsg);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "SQL error: %s\n", zErrMsg);
        sqlite3_free(zErrMsg);
    } else {
        fprintf(stdout, "Table created successfully\n");
    }

    // Indexes SQL statement 
    sql = "CREATE INDEX index_age ON COMPANY(AGE);";

    /* Execute SQL statement */
    rc = sqlite3_exec(db, sql, callback, 0, &zErrMsg);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "SQL error: %s\n", zErrMsg);
        sqlite3_free(zErrMsg);
    } else {
        fprintf(stdout, "Index created successfully\n");
    }

    // Indexes SQL statement 
    sql = "CREATE INDEX index_address20 ON COMPANY(ADDRESS20);";

    /* Execute SQL statement */
    rc = sqlite3_exec(db, sql, callback, 0, &zErrMsg);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "SQL error: %s\n", zErrMsg);
        sqlite3_free(zErrMsg);
    } else {
        fprintf(stdout, "Index created successfully\n");
    }
    return db;
}

// get the proper string from msgpack raw string

static char* get_msgpack_str(const char* raw_str, uint32_t size) {
    // initiate str from raw_str size
    char* str = malloc(size + 1);
    // terminate string properly
    *(str + size) = 0;
    memcpy(str, raw_str, size);
    return str;
}

void* db_worker(void* context) {
    sqlite3* db;
    char* zErrMsg = 0;
    int rc;

    // connect
    db = create_db();

    //  Socket to talk to dispatcher
    void* receiver = zmq_socket(context, ZMQ_REP);
    zmq_connect(receiver, "inproc://workers");

    while (1) {
        //  Process the message
        zmq_msg_t msg;
        zmq_msg_init(&msg);
        int size = zmq_msg_recv(&msg, receiver, 0);

        if (size != -1) {
            /* deserializes the message */
            msgpack_unpacked unpacked_msg;
            msgpack_unpacked_init(&unpacked_msg);
            bool success = msgpack_unpack_next(&unpacked_msg, zmq_msg_data(&msg), size, NULL);

            if (success) {
                msgpack_object obj = unpacked_msg.data;

                if (obj.type == MSGPACK_OBJECT_ARRAY) {
                    //msgpack_object_print(stdout, obj.via.array.ptr->via.); /*=> ["Hello", "MessagePack"] */
                    if (obj.via.array.size != 0) {
                        // recover string array elements eg:- ["insert", "insert into t..."]
                        msgpack_object* p = obj.via.array.ptr;
                        char* stmt_type = get_msgpack_str(p->via.raw.ptr, p->via.raw.size);
                        ++p;
                        // initiate sql statement from received data
                        char* sql = get_msgpack_str(p->via.raw.ptr, p->via.raw.size);
                        if (strcmp(stmt_type, INSERT_STMT) == 0) {
                            /* Execute insert SQL statement */
                            rc = sqlite3_exec(db, sql, callback, 0, &zErrMsg);
                            if (rc != SQLITE_OK) {
                                fprintf(stderr, "SQL error: %s\n", zErrMsg);
                                sqlite3_free(zErrMsg);
                            }
                        } else if (strcmp(stmt_type, SELECT_STMT) == 0) {
                            /* Execute select SQL statement */
                            printf("%s\n", sql);
                        }
                        //printf("The ID of this thread is: %u\n", (unsigned int)pthread_self());                        
                        free(sql);
                    }
                }
            }
            /* cleaning */
            msgpack_unpacked_destroy(&unpacked_msg);
        }
        /* cleaning */
        zmq_msg_close(&msg);
        zmq_send(receiver, "World", 5, 0);
    }
    zmq_close(receiver);
    return NULL;
}
//  Hello World server

#include <zmq.h>
#include <msgpack.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <assert.h>
#include <sqlite3.h> 
#include <zhelpers.h>

static int callback(void *NotUsed, int argc, char **argv, char **azColName) {
    int i;
    for (i = 0; i < argc; i++) {
        printf("%s = %s\n", azColName[i], argv[i] ? argv[i] : "NULL");
    }
    printf("\n");
    return 0;
}

int main(void) {
    // Connect to db
    sqlite3 *db;
    char *zErrMsg = 0;
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
         "SALARY         REAL );";

    /* Execute SQL statement */
    rc = sqlite3_exec(db, sql, callback, 0, &zErrMsg);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "SQL error: %s\n", zErrMsg);
        sqlite3_free(zErrMsg);
    } else {
        fprintf(stdout, "Table created successfully\n");
    }

    //  Socket to talk to clients
    void *context = zmq_ctx_new();
    void *responder = zmq_socket(context, ZMQ_REP);
    rc = zmq_bind(responder, "tcp://*:5555");
    assert(rc == 0);

    while (1) {
        /* creates buffer and serializer instance. */
        //msgpack_sbuffer* buffer = msgpack_sbuffer_new();
        //msgpack_packer* pk = msgpack_packer_new(buffer, msgpack_sbuffer_write);

        //  Process the message
        zmq_msg_t message;
        zmq_msg_init(&message);
        int size = zmq_msg_recv(&message, responder, 0);
        //  Dump the message as text or binary            
        char* data = zmq_msg_data(&message);


        /* serializes ["Hello", "MessagePack"]. */
        //msgpack_pack_array(pk, 2);
        //msgpack_pack_raw(pk, 5);
        //msgpack_pack_raw_body(pk, "Hello", 5);
        //msgpack_sbuffer_write(data, buffer, size);

        /* deserializes it. */
        msgpack_unpacked msg;
        msgpack_unpacked_init(&msg);
        bool success = msgpack_unpack_next(&msg, data, size, NULL);

        if (success) {
            /* prints the deserialized object. */
            printf("\n***********************************\n");
            msgpack_object obj = msg.data;
            if (obj.type = MSGPACK_OBJECT_RAW) {
                char* sql = malloc(obj.via.raw.size);
                memcpy(sql, obj.via.raw.ptr, obj.via.raw.size);
                /* Execute SQL statement */
                rc = sqlite3_exec(db, sql, callback, 0, &zErrMsg);
                if (rc != SQLITE_OK) {
                    fprintf(stderr, "SQL error: %s\n", zErrMsg);
                    sqlite3_free(zErrMsg);
                } else {
                    fprintf(stdout, "Insert successfully\n");
                }
                //printf("%s\n", sql);
                free(sql);
            }
            //            printf("***********************************\n");
            //            msgpack_object_print(stdout, obj); /*=> ["Hello", "MessagePack"] */
            printf("\n***********************************");
        }
        /* cleaning */
        msgpack_unpacked_destroy(&msg);
        zmq_msg_close(&message);
        //msgpack_sbuffer_free(buffer);
        //msgpack_packer_free(pk);
        //        char buffer [10];
        //        zmq_recv (responder, buffer, 10, 0);
        //        printf ("Received Hello\n");
        //sleep(10); //  Do some 'work'
        zmq_send(responder, "World", 5, 0);
    }
    zmq_close(responder);
    zmq_ctx_destroy(context);
    return 0;
}
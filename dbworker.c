// Author: Vimukthi
#include <zmq.h>
#include <msgpack.h>
#include <stdio.h>
#include <unistd.h>
//#include <string.h>
#include <assert.h>
#include <sqlite3.h> 
#include <zhelpers.h>
#include "globals.h"
#include "dbworker.h"
#include "dbresult.h"

//static int resultrow_callback(void* result, int argc, char** argv, char** azColName) {
//    int i;
//    ((db_worker_resultrow_t *) result)->num_cols = argc;
//    ((db_worker_resultrow_t *) result)->values = argv;
//    ((db_worker_resultrow_t *) result)->az_col_names = azColName;
//    //printf("%s = %s\n", azColName[0], argv[0] ? argv[0] : "NULL");
//    //    for (i = 0; i < argc; i++) {
//    //        printf("%s = %s\n", azColName[i], argv[i] ? argv[i] : "NULL");
//    //    }
//    //   printf("\n");
//    return 0;
//}

static sqlite3* create_db() {
    sqlite3* db;
    int rc;
    /* Open database */
    rc = sqlite3_open(":memory:", &db);
    if (rc) {
        fprintf(stderr, "Can't open database: %s\n", sqlite3_errmsg(db));
        exit(0);
    } else {
        fprintf(stdout, "Opened database successfully\n");
    }
    sqlite3_config(SQLITE_CONFIG_SINGLETHREAD);
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

void* dbworker_single_partition(const dbworker_params_t* params) {
    //  Socket to talk to multi partition workers
    void* mworkers = zmq_socket(params->zmq_context, ZMQ_REP);
    int rc = zmq_bind(mworkers, SINGLE_PARTITION_WORKER_URL);
    assert(rc == 0);

    sqlite3* db;
    char* zErrMsg = 0;

    // connect
    db = create_db();

    //  Socket to talk to dispatcher
    void* receiver = zmq_socket(params->zmq_context, ZMQ_REP);
    rc = zmq_connect(receiver, SINGLE_PARTITION_WORKERS_URL);
    assert(rc == 0);

    //  Initialize poll set
    zmq_pollitem_t items [] = {
        { receiver, 0, ZMQ_POLLIN, 0},
        { mworkers, 0, ZMQ_POLLIN, 0}
    };

    while (1) {
        //  Process the message
        zmq_msg_t msg;
        zmq_poll(items, 2, -1);
        if (items [0].revents & ZMQ_POLLIN) {
            while (1) {
                zmq_msg_init(&msg);
                int size = zmq_msg_recv(&msg, receiver, 0);
                int more = zmq_msg_more(&msg);

                if (size != -1) {
                    /* deserializes the message */
                    msgpack_unpacked unpacked_msg;
                    msgpack_unpacked_init(&unpacked_msg);
                    bool success = msgpack_unpack_next(&unpacked_msg, zmq_msg_data(&msg), size, NULL);

                    if (success) {
                        msgpack_object obj = unpacked_msg.data;
                        if (obj.type == MSGPACK_OBJECT_RAW) {
                            //msgpack_object_print(stdout, obj.via.array.ptr->via.); /*=> ["Hello", "MessagePack"] */
                            if (obj.via.raw.size != 0) {
                                // initiate sql statement from received data
                                char* sql = get_msgpack_str(obj.via.raw.ptr, obj.via.raw.size);
                                /* Execute insert SQL statement */
                                rc = sqlite3_exec(db, sql, NULL, 0, &zErrMsg);
                                if (rc != SQLITE_OK) {
                                    fprintf(stderr, "SQL error: %s\n", zErrMsg);
                                    sqlite3_free(zErrMsg);
                                }
                                //printf("%s\n", sql);
                                //printf("The ID of this thread is: %u\n", (unsigned int)pthread_self());                        
                                free(sql);
                            }
                        }
                    }
                    /* cleaning */
                    msgpack_unpacked_destroy(&unpacked_msg);
                    zmq_send(receiver, "World", 5, 0);
                }
                /* cleaning */
                zmq_msg_close(&msg);
                if (!more)
                    break; //  Last message part            
            }
        }
        if (items [1].revents & ZMQ_POLLIN) {
            while (1) {
                zmq_msg_init(&msg);
                int size = zmq_msg_recv(&msg, mworkers, 0);
                int more = zmq_msg_more(&msg);

                if (size != -1) {
                    //printf("Received Hello - %u\n", (unsigned int) pthread_self());
                    /* deserializes the message */
                    msgpack_unpacked unpacked_msg;
                    msgpack_unpacked_init(&unpacked_msg);
                    bool success = msgpack_unpack_next(&unpacked_msg, zmq_msg_data(&msg), size, NULL);

                    if (success) {
                        msgpack_object obj = unpacked_msg.data;

                        if (obj.type == MSGPACK_OBJECT_RAW) {
                            //msgpack_object_print(stdout, obj.via.array.ptr->via.); /*=> ["Hello", "MessagePack"] */
                            if (obj.via.raw.size != 0) {
                                // A prepered statement for fetching tables
                                sqlite3_stmt *stmt;
                                // initiate sql statement from received data
                                char* sql = get_msgpack_str(obj.via.raw.ptr, obj.via.raw.size);
                                if (sqlite3_prepare_v2(db, sql, -1, &stmt, 0) == SQLITE_OK) {
                                    // Read the number of rows fetched
                                    int numcols = sqlite3_column_count(stmt);
                                    // initially 5 rows
                                    //init_resultset(&res, 5, numcols);
                                    //res->num_cols = numcols;
                                    // malloc the data structs because we share this with other threads
                                    //res->cols = malloc(numcols * sizeof (char*));
                                    int col = 0;
                                    const char* cols[numcols];
                                    for (col = 0; col < numcols; col++) {
                                        cols[col] = sqlite3_column_name(stmt, col);
                                    }

                                    while (1) {
                                        // fetch a row’s status
                                        int retval = sqlite3_step(stmt);

                                        if (retval == SQLITE_ROW) {
                                            /* create buffer and serializer instance. */
                                            msgpack_sbuffer* buffer = msgpack_sbuffer_new();
                                            msgpack_packer* pk = msgpack_packer_new(buffer, msgpack_sbuffer_write);
                                            msgpack_pack_map(pk, numcols);
                                            // SQLITE_ROW means fetched a row 
                                            // add each column value to the resultset
                                            for (col = 0; col < numcols; col++) {
                                                const unsigned char* val = sqlite3_column_text(stmt, col);
                                                msgpack_pack_raw(pk, strlen(cols[col]));
                                                msgpack_pack_raw_body(pk, cols[col], strlen(cols[col]));
                                                msgpack_pack_raw(pk, strlen(val));
                                                msgpack_pack_raw_body(pk, val, strlen(val));
                                            }
                                            msgpack_packer_free(pk);
                                            zmq_send(mworkers, &buffer, sizeof (msgpack_sbuffer**), 1);
                                            //add_result_row(res, row);
                                        } else if (retval == SQLITE_DONE) {
                                            // All rows finished
                                            //printf("All rows fetched\n");
                                            zmq_send(mworkers, "DONE", 4, 0);
                                            break;
                                        } else {
                                            // Some error encountered
                                            //printf("Some error encountered\n");
                                        }
                                    }
                                    sqlite3_finalize(stmt);
                                    //printf("spt %u sending results\n", (unsigned int)pthread_self());

                                } else {
                                    //printf("Selecting data from DB Failed\n");
                                }
                                //printf("%s\n", sql);
                                //printf("The ID of this thread is: %u\n", (unsigned int)pthread_self());  
                                zmq_send(mworkers, "Hello", 5, 0);
                                free(sql);
                            }
                        }
                    }
                    /* cleaning */
                    msgpack_unpacked_destroy(&unpacked_msg);
                }
                /* cleaning */
                zmq_msg_close(&msg);
                if (!more)
                    break; //  Last message part            
            }
        }
    }
    zmq_close(receiver);
    return NULL;
}

void* dbworker_multi_partition(void* zmq_context) {
    //printf("The ID of this multi partition thread is: %u\n", (unsigned int) pthread_self());
    //  Socket to talk to dispatcher
    sleep(2);
    void* receiver = zmq_socket(zmq_context, ZMQ_REP);
    int rc = zmq_connect(receiver, MULTI_PARTITION_WORKERS_URL);
    assert(rc == 0);

    //  create sockets to talk to single partition workers
    void* sworkers;
    sworkers = zmq_socket(zmq_context, ZMQ_REQ);
    rc = zmq_connect(sworkers, SINGLE_PARTITION_WORKER_URL);
    assert(rc == 0);

    while (1) {
        //  Process the message
        zmq_msg_t msg;
        while (1) {
            zmq_msg_t reply;
            zmq_msg_t msg_copy;
            zmq_msg_init(&msg);
            int size = zmq_msg_recv(&msg, receiver, 0);
            int more;
            //printf("--%d--\n", size);

            if (size != -1) {

                zmq_msg_init(&msg_copy);
                zmq_msg_copy(&msg_copy, &msg);

                // TRY ASYNC HERE
                zmq_msg_send(&msg_copy, sworkers, 0);
                while (1) {
                    zmq_msg_init(&reply);
                    size = zmq_msg_recv(&reply, sworkers, 0);
                    more = zmq_msg_more(&reply);
                    if (size != -1) {
                        msgpack_sbuffer** result = (msgpack_sbuffer**) zmq_msg_data(&reply);
                        zmq_send(receiver, (*result)->data, (*result)->size, 0);
                        /* cleaning */
                        msgpack_sbuffer_free(*result);
                    }
                    zmq_msg_close(&reply);
                    if (!more)
                        break;
                }
            }
            zmq_msg_close(&msg);
        }
        //sleep(1);
    }
 
    zmq_close(sworkers);
    zmq_close(receiver);
    return NULL;
}

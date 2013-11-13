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
    char address[22];
    sprintf(address, SINGLE_PARTITION_WORKER_URL, params->partition_id);
    void* mworkers = zmq_socket(params->zmq_context, ZMQ_REP);
    int rc = zmq_bind(mworkers, address);
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
                                    dbresult_resultset_t* res = dbresult_new(5, numcols);
                                    // initially 5 rows
                                    //init_resultset(&res, 5, numcols);
                                    //res->num_cols = numcols;
                                    // malloc the data structs because we share this with other threads
                                    //res->cols = malloc(numcols * sizeof (char*));
                                    int col = 0;
                                    for (col = 0; col < numcols; col++) {
                                        const char* colname = sqlite3_column_name(stmt, col);
                                        dbresult_add_column(res, colname);
                                    }

                                    while (1) {
                                        // fetch a row’s status
                                        int retval = sqlite3_step(stmt);

                                        if (retval == SQLITE_ROW) {
                                            dbresult_row_t* row = dbresult_new_row(res);
                                            // SQLITE_ROW means fetched a row 
                                            // add each column value to the resultset
                                            for (col = 0; col < numcols; col++) {
                                                const unsigned char* val = sqlite3_column_text(stmt, col);
                                                dbresult_add_rowdata(row, val);
                                                //printf("%s = %s\t", sqlite3_column_name(stmt, col), val);
                                            }
                                            //add_result_row(res, row);
                                        } else if (retval == SQLITE_DONE) {
                                            // All rows finished
                                            //printf("All rows fetched\n");
                                            break;
                                        } else {
                                            // Some error encountered
                                            //printf("Some error encountered\n");
                                        }
                                    }
                                    sqlite3_finalize(stmt);
                                    //printf("spt %u sending results\n", (unsigned int)pthread_self());
                                    zmq_send(mworkers, (void*) (&res), sizeof (dbresult_resultset_t*), 0);
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
    void* sworkers[NUM_PARTITIONS];
    int thread_nbr;
    for (thread_nbr = 0; thread_nbr < NUM_PARTITIONS; thread_nbr++) {
        char address[22];
        sprintf(address, SINGLE_PARTITION_WORKER_URL, thread_nbr);
        sworkers[thread_nbr] = zmq_socket(zmq_context, ZMQ_REQ);
        rc = zmq_connect(sworkers[thread_nbr], address);
        assert(rc == 0);
    }

    while (1) {
        //  Process the message
        zmq_msg_t msg;
        while (1) {
            dbresult_resultset_t * data[NUM_PARTITIONS];
            zmq_msg_t replies[NUM_PARTITIONS];
            zmq_msg_t msg_copies[NUM_PARTITIONS];
            zmq_msg_init(&msg);
            int size = zmq_msg_recv(&msg, receiver, 0);
            //printf("--%d--\n", size);

            if (size != -1) {
                for (thread_nbr = 0; thread_nbr < NUM_PARTITIONS; thread_nbr++) {
                    zmq_msg_init(&msg_copies[thread_nbr]);
                    zmq_msg_copy(&msg_copies[thread_nbr], &msg);
                    zmq_msg_init(&replies[thread_nbr]);
                    // TRY ASYNC HERE
                    zmq_msg_send(&msg_copies[thread_nbr], sworkers[thread_nbr], 0);
                    size = zmq_msg_recv(&replies[thread_nbr], sworkers[thread_nbr], 0);
                    if (size != -1) {
                        dbresult_resultset_t** result = (dbresult_resultset_t**) zmq_msg_data(&replies[thread_nbr]);
                        data[thread_nbr] = *result;
                        //printf("%s\n", (*result)->result[1]->values[4]);
                        //printf("%s\n", result->cols[0]);
                        // free the resultset memory

                    }
                }
            }
            dbresult_mergeparams_t merge_params;

            // send response result
            //printf("mpt %u sending results\n", (unsigned int) pthread_self());
            msgpack_sbuffer* buffer = (msgpack_sbuffer*) dbresult_merge_serialize(data, &merge_params);    
            //printf("%d\n", (unsigned int)(buffer->size));
            zmq_send(receiver, buffer->data, buffer->size, 0);

            /* cleaning */
            msgpack_sbuffer_free(buffer);
            zmq_msg_close(&msg);
            for (thread_nbr = 0; thread_nbr < NUM_PARTITIONS; thread_nbr++) {
                dbresult_free(data[thread_nbr]);
                zmq_msg_close(&msg_copies[thread_nbr]);
                zmq_msg_close(&replies[thread_nbr]);
            }
        }
        //sleep(1);
    }
    // close sockets
    for (thread_nbr = 0; thread_nbr < NUM_PARTITIONS; thread_nbr++) {
        zmq_close(sworkers[thread_nbr]);
    }
    zmq_close(receiver);
    return NULL;
}

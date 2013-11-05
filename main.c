//  Sqlite server main
// Author : Vimukthi

#include <zmq.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <assert.h>
#include <zhelpers.h>
#include <dbworker.h>
#include <msgpack.h>
#include "globals.h"
#include <pthread.h>

#define SQL_MSG_SIZE 3

int main(void) {
    void* context = zmq_ctx_new();
    //  Socket to talk to clients
    void* clients = zmq_socket(context, ZMQ_ROUTER);
    int rc = zmq_bind(clients, DB_URL);
    assert (rc == 0);

    //  Socket to talk to single partition workers
    void* sworkers = zmq_socket(context, ZMQ_DEALER);
    rc = zmq_bind(sworkers, SINGLE_PARTITION_WORKERS_URL);
    assert (rc == 0);

    //  Socket to talk to multi partition workers
    void* mworkers = zmq_socket(context, ZMQ_DEALER);
    rc = zmq_bind(mworkers, MULTI_PARTITION_WORKERS_URL);
    assert (rc == 0);

    //  Launch the pool of single partition worker threads
    pthread_t sworkers_arr[NUM_PARTITIONS];
    dbworker_params_t* params[NUM_PARTITIONS];
    int thread_nbr;
    for (thread_nbr = 0; thread_nbr < NUM_PARTITIONS; thread_nbr++) {
        params[thread_nbr] = (dbworker_params_t *) malloc(sizeof (dbworker_params_t));
        params[thread_nbr]->partition_id = thread_nbr;
        params[thread_nbr]->zmq_context = context;
        pthread_create(&sworkers_arr[thread_nbr], NULL, (void *) dbworker_single_partition, params[thread_nbr]);
    }

    //  Launch the pool of multi partition worker threads
    pthread_t mworkers_arr[NUM_MPARTITION_WORKERS];
    thread_nbr = 0;
    for (thread_nbr = 0; thread_nbr < NUM_MPARTITION_WORKERS; thread_nbr++) {
        pthread_create(&mworkers_arr[thread_nbr], NULL, (void *) dbworker_multi_partition, context);
    }
    
    //  Initialize poll set
    zmq_pollitem_t items [] = {
        { clients, 0, ZMQ_POLLIN, 0 },
        { sworkers,  0, ZMQ_POLLIN, 0 },
        { mworkers,  0, ZMQ_POLLIN, 0 }
    };
    //  Switch messages between sockets
    while (1) {
        zmq_msg_t message;
        zmq_poll (items, 3, -1);
        if (items [0].revents & ZMQ_POLLIN) {
            bool sp = true; // single partition?
            int num_sqlmsg_parts = 0;
            zmq_msg_t sqlmsgs[SQL_MSG_SIZE];
            while (1) {
                //  Process all parts of the message
                zmq_msg_t msg;
                zmq_msg_init (&msg);
                int size = zmq_msg_recv (&msg, clients, 0);
                int more = zmq_msg_more (&msg);
                
                if(size == 2 && more){     
                    // this is a partition spec message
                    // update sp variable according to message
                    /* deserializes the message */
                    msgpack_unpacked unpacked_msg;
                    msgpack_unpacked_init(&unpacked_msg);
                    bool unpacked = msgpack_unpack_next(&unpacked_msg, zmq_msg_data(&msg), size, NULL);

                    if (unpacked) {
                        msgpack_object obj = unpacked_msg.data;
                        if (obj.type == MSGPACK_OBJECT_RAW && obj.via.raw.size != 0) {
                            // check if this is a single or multi partition sql stmt
                            if (*((char *)obj.via.raw.ptr) == MULTI_PARTITION_STMT){                                
                                sp = false;
                            }
                        }
                    }
                    zmq_msg_close (&msg);
                } else {
                    // this is a msg part to be sent to backend                    
                    sqlmsgs[num_sqlmsg_parts] = msg;
                    num_sqlmsg_parts++;
                    
                    // check if its time to send msgs to backend
                    if (num_sqlmsg_parts == SQL_MSG_SIZE){
                        // check if single partition
                        if(sp) {
                            // send to single partition workers
                            zmq_msg_send (&sqlmsgs[0], sworkers, ZMQ_SNDMORE);
                            zmq_msg_send (&sqlmsgs[1], sworkers, ZMQ_SNDMORE);
                            zmq_msg_send (&sqlmsgs[2], sworkers, 0);
                        } else {
                            //printf("--%d--\n", num_sqlmsg_parts);
                            // send to multi partition workers
                            zmq_msg_send (&sqlmsgs[0], mworkers, ZMQ_SNDMORE);
                            zmq_msg_send (&sqlmsgs[1], mworkers, ZMQ_SNDMORE);
                            zmq_msg_send (&sqlmsgs[2], mworkers, 0);
                        }
                        zmq_msg_close (&sqlmsgs[0]);
                        zmq_msg_close (&sqlmsgs[1]);
                        zmq_msg_close (&sqlmsgs[2]);
                        break; 
                    }    
                } 
                //zmq_msg_close (&message);
                if (!more)
                    break;      //  Last message part
            }
        }
        if (items [1].revents & ZMQ_POLLIN) {
            while (1) {
                //  Process 3 parts of the message
                zmq_msg_init (&message);
                zmq_msg_recv (&message, sworkers, 0);
                int more = zmq_msg_more (&message);
                zmq_msg_send (&message, clients, more? ZMQ_SNDMORE: 0);
                zmq_msg_close (&message);
                if (!more)
                    break;      //  Last message part
            }
        }
        if (items [2].revents & ZMQ_POLLIN) {
            while (1) {
                //  Process all parts of the message
                zmq_msg_init (&message);
                zmq_msg_recv (&message, mworkers, 0);
                int more = zmq_msg_more (&message);
                zmq_msg_send (&message, clients, more? ZMQ_SNDMORE: 0);
                zmq_msg_close (&message);
                if (!more)
                    break;      //  Last message part
            }
        }
        //sleep(1);
    }
    
    //  We never get here, but clean up anyhow
    zmq_close(clients);
    zmq_close(sworkers);
    zmq_close(mworkers);
    thread_nbr = 0;
    for (thread_nbr = 0; thread_nbr < NUM_PARTITIONS; thread_nbr++) {
        pthread_join(sworkers_arr[thread_nbr], NULL);
        free(params[thread_nbr]);
    }
    zmq_ctx_destroy(context);
    return 0;
}
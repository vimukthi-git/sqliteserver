//  Sqlite server main
// Author : Vimukthi

#include <zmq.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <assert.h>
#include <zhelpers.h>
#include <dbworker.h>
#include "globals.h"
#include <pthread.h>

int main(void) {
    void* context = zmq_ctx_new();
    //  Socket to talk to clients
    void* clients = zmq_socket(context, ZMQ_ROUTER);
    zmq_bind(clients, DB_URL);

    //  Socket to talk to single partition workers
    void* sworkers = zmq_socket(context, ZMQ_DEALER);
    zmq_bind(sworkers, SINGLE_PARTITION_WORKERS_URL);

    //  Socket to talk to multi partition workers
    void* mworkers = zmq_socket(context, ZMQ_DEALER);
    zmq_bind(mworkers, MULTI_PARTITION_WORKERS_URL);

    //  Launch the pool of single partition worker threads
    pthread_t sworkers_arr[NUM_PARTITIONS];
    db_worker_params_t* params[NUM_PARTITIONS];
    int thread_nbr;
    for (thread_nbr = 0; thread_nbr < NUM_PARTITIONS; thread_nbr++) {
        params[thread_nbr] = (db_worker_params_t *) malloc(sizeof (db_worker_params_t));
        params[thread_nbr]->partition_id = thread_nbr;
        params[thread_nbr]->zmq_context = context;
        pthread_create(&sworkers_arr[thread_nbr], NULL, (void *) db_single_partition_worker, params[thread_nbr]);
    }

    //  Launch the pool of multi partition worker threads
    pthread_t mworkers_arr[NUM_MPARTITION_WORKERS];
    thread_nbr = 0;
    for (thread_nbr = 0; thread_nbr < NUM_MPARTITION_WORKERS; thread_nbr++) {
        //printf("**%d\n", (db_worker_params_t*)(*(params + 1))->partition_id); 
        pthread_create(&mworkers_arr[thread_nbr], NULL, (void *) db_multi_partition_worker, &params);
    }

    //  Connect work threads to client threads via a queue proxy
    zmq_proxy(clients, sworkers, NULL);

    //  We never get here, but clean up anyhow
    zmq_close(clients);
    zmq_close(sworkers);
    thread_nbr = 0;
    for (thread_nbr = 0; thread_nbr < NUM_PARTITIONS; thread_nbr++) {
        pthread_join(sworkers_arr[thread_nbr], NULL);
        free(params[thread_nbr]);
    }
    zmq_ctx_destroy(context);
    return 0;
}
//  Sqlite server main
// Author : Vimukthi

#include <zmq.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <assert.h>
#include <zhelpers.h>
#include <dbworker.h>

int main(void) {
    //  Socket to talk to clients
    void* context = zmq_ctx_new();
    //  Socket to talk to clients
    void* clients = zmq_socket(context, ZMQ_ROUTER);
    zmq_bind(clients, "tcp://*:5555");

    //  Socket to talk to workers
    void* workers = zmq_socket(context, ZMQ_DEALER);
    zmq_bind(workers, "inproc://workers");

    //  Launch pool of worker threads
    int thread_nbr;
    for (thread_nbr = 0; thread_nbr < 4; thread_nbr++) {
        pthread_t worker;
        pthread_create(&worker, NULL, db_worker, context);
    }
    //  Connect work threads to client threads via a queue proxy
    zmq_proxy(clients, workers, NULL);

    //  We never get here, but clean up anyhow
    zmq_close(clients);
    zmq_close(workers);
    zmq_ctx_destroy(context);
    return 0;
}
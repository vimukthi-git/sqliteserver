/* 
 * File:   dbworker.h
 * Author: Vimukthi
 *
 * Created on October 28, 2013, 2:47 PM
 */

#ifndef DBWORKER_H
#define	DBWORKER_H

#ifdef	__cplusplus
extern "C" {
#endif




#ifdef	__cplusplus
}
#endif

#endif	/* DBWORKER_H */

#define INSERT_STMT "1"
#define SELECT_STMT "2"

typedef struct {
    int partition_id;
    void* zmq_context;
} dbworker_params_t;

void* dbworker_single_partition(const dbworker_params_t* params);

void* dbworker_multi_partition(void* zmq_context);

/* 
 * File:   globals.h
 * Author: newclearfe1
 *
 * Created on October 29, 2013, 8:21 PM
 */

#ifndef GLOBALS_H
#define	GLOBALS_H

#ifdef	__cplusplus
extern "C" {
#endif




#ifdef	__cplusplus
}
#endif

#endif	/* GLOBALS_H */

#define NUM_PARTITIONS 4
#define NUM_MPARTITION_WORKERS 1

#define DB_URL "tcp://*:5555"
#define SINGLE_PARTITION_WORKERS_URL "inproc://sworkers"
#define SINGLE_PARTITION_WORKER_URL "inproc://partition%d"
#define MULTI_PARTITION_WORKERS_URL "inproc://mworkers"


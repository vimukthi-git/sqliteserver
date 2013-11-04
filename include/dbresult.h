/* 
 * File:   result.h
 * Author: newclearfe1
 *
 * Created on November 4, 2013, 4:06 PM
 */

#ifndef RESULT_H
#define	RESULT_H

#ifdef	__cplusplus
extern "C" {
#endif


#ifdef	__cplusplus
}
#endif

#endif	/* RESULT_H */

// Struct to hold a db result row

typedef struct {
    char** values;
} row_t;

// free the row

void free_row(row_t* row);

// Struct to hold a db result set

typedef struct {
    int num_cols;
    row_t** result;    
    char** cols;
    size_t used;
    size_t size;
} resultset_t;

// init result set

void init_resultset(resultset_t *a, size_t initial_size);

// add a row to the resultset

void add_result_row(resultset_t *a, row_t* row);

// free the result set

void free_resultset(resultset_t *a);
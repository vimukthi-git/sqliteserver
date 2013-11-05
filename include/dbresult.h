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

// declare dependant types first

typedef struct dbresult_row_t dbresult_row_t;
typedef struct dbresult_resultset_t dbresult_resultset_t;

// Struct to hold a db result row

struct dbresult_row_t {
    char** values;
    dbresult_resultset_t* resultset;
    int num_added_data;
};

// Struct to hold a db result set

struct dbresult_resultset_t{
    int num_cols;
    int num_added_cols;
    dbresult_row_t** result;    
    char** cols;
    size_t used;
    size_t size;
};

// add a column to result

void dbresult_add_rowdata(dbresult_row_t* row, const char* data);

// create result set

dbresult_resultset_t* dbresult_new(size_t initial_size, int num_columns);

// create row

dbresult_row_t* dbresult_new_row(dbresult_resultset_t *a);

// add a column to result

void dbresult_add_column(dbresult_resultset_t *a, const char* column_name);

// free the result set

void dbresult_free(dbresult_resultset_t *a);
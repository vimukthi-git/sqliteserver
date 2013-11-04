// Author: Vimukthi
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <assert.h>
#include "dbresult.h"

// free the row

void free_row(row_t* row){
    free(row->values);
    row->values = NULL;
}

// init result set

void init_resultset(resultset_t *a, size_t initial_size) {
    a->result = (row_t**) malloc(initial_size * sizeof (row_t*));
    a->used = 0;
    a->size = initial_size;
}

// add a row to the resultset

void add_result_row(resultset_t *a, row_t* row) {
    if (a->used == a->size) {
        a->size *= 2;
        a->result = (row_t**) realloc(a->result, a->size * sizeof (row_t*));
    }
    a->result[a->used++] = row;
}

// free the result set

void free_resultset(resultset_t *a) {
    // check each colname and result row for memory leaks
    free(a->result);
    free(a->cols);
    free(a);
    a->result = NULL;
    a->cols = NULL;
    a->used = a->size = 0;
}

void set_num_cols(resultset_t *a, int cols){
    a->num_cols = cols;
}

void set_cols(resultset_t *a, char** cols){
    a->cols = cols;
}
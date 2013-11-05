// Author: Vimukthi
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <assert.h>
#include "dbresult.h"

// add a row to the resultset

static void dbresult_add_row(dbresult_resultset_t *a, dbresult_row_t* row) {
    if (a->used == a->size) {
        a->size *= 2;
        a->result = (dbresult_row_t**) realloc(a->result, a->size * sizeof (dbresult_row_t*));
    }
    a->result[a->used++] = row;
    row->resultset = a;
    row->num_added_data = 0;
}

// init result set

dbresult_resultset_t* dbresult_new(size_t initial_size, int num_columns) {
    dbresult_resultset_t* a = malloc(sizeof (dbresult_resultset_t));
    a->result = (dbresult_row_t**) malloc(initial_size * sizeof (dbresult_row_t*));
    a->num_cols = num_columns;
    a->num_added_cols = 0;
    a->cols = malloc(num_columns * sizeof (char*));
    a->used = 0;
    a->size = initial_size;
    return a;
}

// create new row

dbresult_row_t* dbresult_new_row(dbresult_resultset_t *a) {
    dbresult_row_t* row = malloc(sizeof (dbresult_row_t));
    row->values = malloc(a->num_cols * sizeof (char*));
    dbresult_add_row(a, row);
    return row;
}

// add column

void dbresult_add_column(dbresult_resultset_t *a, const char* column_name) {
    if (a->num_added_cols < a->num_cols) {
        a->cols[a->num_added_cols] = malloc(strlen(column_name) + 1);
        strcpy(a->cols[a->num_added_cols], column_name);
        a->num_added_cols++;
    }
}

// add data to a row

void dbresult_add_rowdata(dbresult_row_t* row, const char* data) {
    // check whether the column number limit has exceeded and add data
    if (row->num_added_data < row->resultset->num_cols) {
        row->values[row->num_added_data] = malloc(strlen(data) + 1);
        strcpy(row->values[row->num_added_data], data);
        row->num_added_data++;
    }
}

// free the result set

void dbresult_free(dbresult_resultset_t *a) {
    int i, j;
    for (i = 0; i < a->used; i++) {
        if (a->result[i]->num_added_data > 0) {
            // free each column of data
            for (j = 0; j < a->result[i]->num_added_data; j++) {
                free((char*)(a->result[i]->values[j]));
                a->result[i]->values[j] = NULL;
            }
            // free values array
            free((char**)(a->result[i]->values));
            a->result[i]->values = NULL;
        }
        // free row_t
        a->result[i]->resultset = NULL;
        free((dbresult_row_t*)(a->result[i]));
        a->result[i] = NULL;
    }
    
    // free the result array
    free((dbresult_row_t**)(a->result));
    a->result = NULL;
    
    // free the column names
    for (i = 0; i < a->num_added_cols; i++) {
        free((char*)(a->cols[i]));
        a->cols[i] = NULL;
    }
    
    // free the column name array
    free((char**)(a->cols));
    a->cols = NULL;
    
    // free the resultset_t
    free((dbresult_resultset_t*)a);
    a = NULL;
}
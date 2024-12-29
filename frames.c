#include "frames.h"
#include "stdbool.h"
#include "stdlib.h"
#include "stdio.h"
#include "math.h"

/* PARAMETERS */
// THRESHOLD
#define THRESHOLD 1000 // Local condition

// DELTA
#define DELTA 1000 // Local condition

// AGGREGATE
/**
 * AVG = 0
 * SUM = 1
 **/
#define AGGREGATE 1 // Specify aggregation function
#define AGGREGATE_THRESHOLD 1000 // Local condition

#define GAP 500

/**
 * Insert a tuple in the tail of a linked list
 * @param last pointer to last element of the buffer
 * @param data the tuple to be appended
 */
void enqueue(node** last, tuple data) {
    node* new_node = (node*)malloc(sizeof(node));
    new_node->data = data;
    new_node->next = NULL;

    if (*last == NULL) {
        *last = new_node;
        return;
    }

    node* last_node = *last;
    last_node->next = new_node;
    *last = new_node;
}

double avg(node* buffer, long size) {
    if (buffer == NULL) {
        perror("Empty buffer, impossible to compute aggregate function\n");
        return -1;
    }

    long iterator = 0;
    double sum = 0;
    int count = 0;
    node* current = buffer;

    while (current != NULL && iterator < size) {
        sum += current->data.A;
        count++;
        current = current->next;
        iterator++;
    }

    return (double) sum / count;
}

double sum(node* buffer, long size) {
    if (buffer == NULL) {
        perror("Empty buffer, impossible to compute aggregate function\n");
        return -1;
    }

    long iterator = 0;
    double sum = 0;
    node* current = buffer;

    while (current != NULL && iterator < size) {
        sum += current->data.A;
        current = current->next;
        iterator++;
    }

    return sum;
}

double aggregation(int agg_function, node* buffer, long size){
    switch (agg_function) {
        case 0: return avg(buffer, size);
        case 1: return sum(buffer, size);
        default:
            printf("Error while selecting aggregation function");
            return -1;
    }
}

// Open a new frame and insert the current processed tuple.
context *open(tuple tuple, context *C) {
    //enqueue(buffer, tuple);
    switch (C->frame_type) {
        case 0:
            C->count++;
            return C;
        case 1:
            C->start = true;
            C->v = tuple.A;
            return C;
        case 2:
            C->v = tuple.A; // aggregation function on a single value corresponds to that value
            C->start = true;
            return C;
        case 3:
            C->curr_timestamp = tuple.timestamp;
            C->start = true;
            return C;
    }
}

// Update the current frame, extends it to include the current processed tuple.
context *update(tuple tuple, context *C, node *buffer, long size) {
    //enqueue(buffer, tuple);
    switch (C->frame_type) {
        case 0:
            C->count++;
            return C;
        case 1:
            return C;
        case 2:
            C->v = aggregation(AGGREGATE, buffer, size);
            return C;
        case 3:
            C->curr_timestamp = tuple.timestamp;
            return C;
    }
}

// Close the current frame and check for global conditions to eventually evict.
// Removes the evicted/discarded frame from the buffer.
context *close(tuple tuple, context *C) {
    switch (C->frame_type) {
        case 0:
            C->count = 0;
            return C;
        case 1:
            C->start = false;
            return C;
        case 2:
            C->start = false;
            return C;
        case 3:
            C->start = false;
            return C;
    }
}

bool open_pred(tuple tuple, context *C) {
    switch (C->frame_type) {
        case 0: return (tuple.A >= THRESHOLD && C->count == 0);
        case 1: return (!(C->start));
        case 2: return (!(C->start));
        case 3: return (tuple.timestamp - C->curr_timestamp <= GAP && !(C->start));
    }
}

bool update_pred(tuple tuple, context *C) {
    switch (C->frame_type) {
        case 0: return (tuple.A >= THRESHOLD && C->count > 0);
        case 1: return (fabs(C->v - tuple.A) < DELTA && C->start);
        case 2: return (C->v < AGGREGATE_THRESHOLD && C->start);
        case 3: return (tuple.timestamp - C->curr_timestamp <= GAP && C->start);
    }
}

bool close_pred(tuple tuple, context *C) {
    switch (C->frame_type) {
        case 0: return (tuple.A < THRESHOLD && C->count > 0);
        case 1: return (fabs(C->v - tuple.A) >= DELTA && C->start);
        case 2: return (C->v >= AGGREGATE_THRESHOLD && C->start);
        case 3: return (tuple.timestamp - C->curr_timestamp > GAP && C->start);
    }
}

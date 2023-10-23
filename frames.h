#ifndef CFRAMES_FRAMES_H
#define CFRAMES_FRAMES_H

#include <stdbool.h>

// tuple definition
typedef struct Data {
    long timestamp;
    long key;
    double A; // value
} tuple;

// buffer element definition
typedef struct Node {
    tuple data;
    struct Node* next;
} node;

// context definition
typedef struct Context {
    int frame_type;
    double v;
    int count;
    bool start;
} context;

typedef struct Frame {
    long t_start;
    long t_end;
    long size;
    node* start;
    node* end;
    long count;
    struct Frame* next;
} frame;

void enqueue(node** last, tuple data);

context *open(tuple tuple, context *C);
context *update(tuple tuple, context *C, node *buffer, long size);
context *close(tuple tuple, context *C);

bool open_pred(tuple tuple, context *C);
bool update_pred(tuple tuple, context *C);
bool close_pred(tuple tuple, context *C);

#endif //CFRAMES_FRAMES_H

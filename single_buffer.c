#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <math.h>

/**
 * THRESHOLD = 0 -> Detect time periods where a specified attribute is over under a specified threshold.
 * DELTA = 1 -> A new frame starts whenever the value of a particular attribute changes by more than an amount.
 * AGGREGATE = 2 -> End a frame when an aggregate of the values of a specified attribute within the frame exceeds a threshold.
 **/
#define FRAME 2

/* PARAMETERS */
// THRESHOLD
#define THRESHOLD 27 // Local condition
#define MIN_COUNT 1 // Global condition

// DELTA
#define DELTA 4 // Local condition

// AGGREGATE
/**
 * AVG = 0
 * SUM = 1
 **/
#define AGGREGATE 0 // Specify aggregation function
#define AGGREGATE_THRESHOLD 30 // Local condition

#define MAX_TUPLES 100

// tuple definition
typedef struct Data {
    int VID;
    int A;
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

/**
 * This function decides which aggregation function has to be applied to the current frame
 * @param agg_function aggregation to apply
 * @param buffer current frame
 * @return the result of the aggregation
 */
double aggregation(int agg_function, node* buffer);

/**** AGGREGATION FUNCTIONS ****/
double avg(node* buffer);
double sum(node* buffer);
/*******************************/

/** FRAMES FUNCTIONS **/
// PREDICATES
// The predicate functions trigger the actions depending on the framing scheme.
bool close_pred(tuple data, context *pContext);
bool update_pred(tuple data, context *pContext);
bool open_pred(tuple data, context *pContext);
// FUNCTIONS
// Concrete implementation of the framing action
node *close(tuple tuple, context *C, node* buffer);
node *update(tuple tuple, context *C, node *buffer);
node *open(tuple tuple, context *C, node *buffer);
/********************/

// put a tuple in the buffer
// FIXME: directly add in the queue rear
node* enqueue(node** head, tuple data) {

    node* new_node = (node*)malloc(sizeof(node));
    new_node->data = data;
    new_node->next = NULL;

    if (*head == NULL) {
        *head = new_node;
        return *head;
    }

    node* last = *head;
    while (last->next != NULL) {
        last = last->next;
    }

    last->next = new_node;
    return *head;
}

void print_buffer(node* head) {
    while (head != NULL) {
        printf("[VID: %d, A: %d] ", head->data.VID, head->data.A);
        head = head->next;
    }
    printf("\n");
}

void evict(node* buffer){
    print_buffer(buffer);
}

int main() {

    // FIXME: process tuples on the fly
    tuple tuples[MAX_TUPLES];
    int i, num_tuples;

    // parse CSV
    // FIXME: can't use relative path
    FILE *file = fopen("/home/maurofama/Documents/PolyFlow/OVERT/frink_project/frink/src/main/resources/small.csv", "r");
    if (file == NULL) {
        perror("Error loading input file");
        return 1;
    }

    // store input CSV in array
    i = 0;
    char line[256];
    while (fgets(line, sizeof(line), file)) {
        int vid, speed;
        if (sscanf(line, "[%d, %d", &vid, &speed) == 2) {
            tuples[i].VID = vid;
            tuples[i].A = speed;
            i++;
        }
    }
    num_tuples = i;
    fclose(file);

    printf("DATA\n---------------------------\n");
    for (int j = 0; j < i; j++) {
        printf("VID: %d, A: %d\n", tuples[j].VID, tuples[j].A);
    }
    printf("---------------------------\n\n");

    // initialize buffer and context
    node* buffer = NULL;
    context* C = (context*)malloc(sizeof(context));
    C->frame_type = FRAME;
    C->count = 0;
    C->start = false;
    C->v = -1;

    for (i = 0; i < num_tuples; i++) {
        // process tuple
        if (close_pred(tuples[i], C)) buffer = close(tuples[i], C, buffer);
        if (update_pred(tuples[i], C)) buffer = update(tuples[i], C, buffer);
        if (open_pred(tuples[i], C)) buffer = open(tuples[i], C, buffer);
    }
    if (buffer != NULL){ // evict last frame
        if ((C->frame_type != 0) || (C->count > MIN_COUNT)){
            evict(buffer);
            free(buffer);
            buffer = NULL;
        }
    }

    return 0;
}

// Open a new frame and insert the current processed tuple.
node *open(tuple tuple, context *C, node *buffer) {
    switch (C->frame_type) {
        case 0:
            C->count++;
            return enqueue(&buffer, tuple);
        case 1:
            C->start = true;
            C->v = tuple.A;
            return enqueue(&buffer, tuple);
        case 2:
            C->v = tuple.A; // aggregation function on a single value corresponds to that value
            C->start = true;
            return enqueue(&buffer, tuple);
    }
}

// Update the current frame, extends it to include the current processed tuple.
node *update(tuple tuple, context *C, node *buffer) {
    switch (C->frame_type) {
        case 0:
            C->count++;
            return enqueue(&buffer, tuple);
        case 1:
            return enqueue(&buffer, tuple);
        case 2:
            C->v = aggregation(AGGREGATE, buffer);
            return enqueue(&buffer, tuple);
    }
}

// Close the current frame and check for global conditions to eventually evict.
// Removes the evicted/discarded frame from the buffer.
node *close(tuple tuple, context *C, node* buffer) {
    switch (C->frame_type) {
        case 0:
            if (C->count > MIN_COUNT) {
                evict(buffer);
            }
            free(buffer);
            C->count = 0;
            buffer = NULL;
            return NULL;
        case 1:
            evict(buffer);
            free(buffer);
            buffer = NULL;
            C->start = false;
            return NULL;
        case 2:
            evict(buffer);
            free(buffer);
            buffer = NULL;
            C->start = false;
            return NULL;
    }
}

bool open_pred(tuple tuple, context *C) {
    switch (C->frame_type) {
        case 0: return (tuple.A < THRESHOLD && C->count == 0);
        case 1: return (!(C->start));
        case 2: return (!(C->start));
    }
}

bool update_pred(tuple tuple, context *C) {
    switch (C->frame_type) {
        case 0: return (tuple.A < THRESHOLD && C->count > 0);
        case 1: return (fabs(C->v - tuple.A) < DELTA && C->start);
        case 2: return (C->v < AGGREGATE_THRESHOLD && C->start);
    }
}

bool close_pred(tuple tuple, context *C) {
    switch (C->frame_type) {
        case 0: return (tuple.A >= THRESHOLD && C->count > 0);
        case 1: return (fabs(C->v - tuple.A) >= DELTA && C->start);
        case 2: return (C->v >= AGGREGATE_THRESHOLD && C->start);
    }
}

double avg(node* buffer) {
    if (buffer == NULL) {
        perror("Empty buffer, impossible to compute aggregate function\n");
        return -1;
    }

    int sum = 0;
    int count = 0;
    node* current = buffer;

    while (current != NULL) {
        sum += current->data.A;
        count++;
        current = current->next;
    }

    return (double) sum / count;
}

double sum(node* buffer) {
    if (buffer == NULL) {
        perror("Empty buffer, impossible to compute aggregate function\n");
        return -1;
    }

    double sum = 0;
    node* current = buffer;

    while (current != NULL) {
        sum += current->data.A;
        current = current->next;
    }

    return sum;
}

double aggregation(int agg_function, node* buffer){
    switch (agg_function) {
        case 0: return avg(buffer);
        case 1: return sum(buffer);
        default:
            perror("Error while selecting aggregation function");
            return -1;
    }
}
#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <math.h>
#include <string.h>

/* PARAMETERS */
// THRESHOLD
#define THRESHOLD 1000 // Local condition
#define MIN_COUNT 1 // Global condition

// DELTA
#define DELTA 1000 // Local condition

// AGGREGATE
/**
 * AVG = 0
 * SUM = 1
 **/
#define AGGREGATE 1 // Specify aggregation function
#define AGGREGATE_THRESHOLD 1000 // Local condition

#define MAX_CHARS 256

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
void close(tuple tuple, context *C, node* buffer);
void update(tuple tuple, context *C, node **buffer, node *list) ;
void open(tuple tuple, context *C, node **buffer);
/********************/

// put a tuple in the buffer
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

void print_buffer(node* head) {
    while (head != NULL) {
        printf("[VID: %ld, A: %f] ", head->data.timestamp, head->data.A);
        head = head->next;
    }
    printf("\n");
}

void evict(node* buffer){
    print_buffer(buffer);
}

int main(int argc, char *argv[]) {

    if (argc != 3) {
        printf("Usage: <input file path> <Frame type (THRESHOLD = 0 | DELTA = 1 | AGGREGATE = 2)>\n ");
        return 1;
    }
    char *file_path = argv[1];
    /**
    * THRESHOLD = 0 -> Detect time periods where a specified attribute is over under a specified threshold.
    * DELTA = 1 -> A new frame starts whenever the value of a particular attribute changes by more than an amount.
    * AGGREGATE = 2 -> End a frame when an aggregate of the values of a specified attribute within the frame exceeds a threshold.
     **/
    int FRAME = atoi(argv[2]);

    int num_tuples = 0;
    char line[MAX_CHARS];

    // initialize buffer
    node* current = NULL;
    node* head = NULL;
    node* tail = NULL;

    // initialize context
    context* C = (context*)malloc(sizeof(context));
    C->frame_type = FRAME;
    C->count = 0;
    C->start = false;
    C->v = -1;

    // parse CSV
    FILE *file = fopen(file_path, "r");
    if (file == NULL) {
        perror("Error loading input file");
        return 1;
    }

    // ignore header
    if (fgets(line, 256, file) == NULL) {
        perror("Error reading CSV header");
        fclose(file);
        return 1;
    }

    // FIXME: CSV generator creates some more tuples than expected, ok because guarantees windowing correctness? DELTA frames generator are wrongly sized: after first frame, it generates 1 tuple more than expected in the frame
    while (fgets(line, 256, file) != NULL) {
        char *token = strtok(line, ",");
        int count = 0;

        // read tuple from file and store attributes
        tuple data;
        while (token != NULL) {
            if (count == 0) {
                data.timestamp = strtol(token, NULL, 10);
            } else if (count == 1) {
                data.key = strtol(token, NULL, 10);
            } else if (count == 2) {
                data.A = strtod(token, NULL);
            }
            token = strtok(NULL, ",");
            count++;
        }

        // process tuple
        if (close_pred(data, C)) close(data, C, current);
        if (update_pred(data, C)) update(data, C, &tail, current);
        if (open_pred(data, C)) {
            open(data, C, &tail);
            current = tail;
        }
        if (num_tuples == 0) head = current; // save the head of the buffer
        num_tuples++;
    }
    // evict last frame if possible
    if (current != NULL){
        if ((C->frame_type != 0) || (C->count > MIN_COUNT)){
            evict(current);
        }
    }

    printf("--------------------\nBuffer: ");
    print_buffer(head);
    free(head);

    fclose(file);

    return 0;
}

// Open a new frame and insert the current processed tuple.
void open(tuple tuple, context *C, node **buffer) {
    switch (C->frame_type) {
        case 0:
            C->count++;
            enqueue(buffer, tuple);
            break;
        case 1:
            C->start = true;
            C->v = tuple.A;
            enqueue(buffer, tuple);
            break;
        case 2:
            C->v = tuple.A; // aggregation function on a single value corresponds to that value
            C->start = true;
            enqueue(buffer, tuple);
            break;
    }
}

// Update the current frame, extends it to include the current processed tuple.
void update(tuple tuple, context *C, node **buffer, node *list) {
    switch (C->frame_type) {
        case 0:
            C->count++;
            enqueue(buffer, tuple);
            break;
        case 1:
            enqueue(buffer, tuple);
            break;
        case 2:
            enqueue(buffer, tuple);
            C->v = aggregation(AGGREGATE, list);
            break;
    }
}

// Close the current frame and check for global conditions to eventually evict.
// Removes the evicted/discarded frame from the buffer.
void close(tuple tuple, context *C, node* buffer) {
    switch (C->frame_type) {
        case 0:
            if (C->count > MIN_COUNT) {
                evict(buffer);
            }
            C->count = 0;
            break;
        case 1:
            evict(buffer);
            C->start = false;
            break;
        case 2:
            evict(buffer);
            C->start = false;
            break;
    }
}

bool open_pred(tuple tuple, context *C) {
    switch (C->frame_type) {
        case 0: return (tuple.A >= THRESHOLD && C->count == 0);
        case 1: return (!(C->start));
        case 2: return (!(C->start));
    }
}

bool update_pred(tuple tuple, context *C) {
    switch (C->frame_type) {
        case 0: return (tuple.A >= THRESHOLD && C->count > 0);
        case 1: return (fabs(C->v - tuple.A) < DELTA && C->start);
        case 2: return (C->v < AGGREGATE_THRESHOLD && C->start);
    }
}

bool close_pred(tuple tuple, context *C) {
    switch (C->frame_type) {
        case 0: return (tuple.A < THRESHOLD && C->count > 0);
        case 1: return (fabs(C->v - tuple.A) >= DELTA && C->start);
        case 2: return (C->v >= AGGREGATE_THRESHOLD && C->start);
    }
}

double avg(node* buffer) {
    if (buffer == NULL) {
        perror("Empty buffer, impossible to compute aggregate function\n");
        return -1;
    }

    double sum = 0;
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
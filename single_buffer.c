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

#define MAX_CHARS 256 // Max admitted characters in a line representing event

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

// window definition
typedef struct Window {
    long size;
    long t_start;
    long t_end;
} window;

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
context *close(tuple tuple, context *C, node* buffer);
context *update(tuple tuple, context *C, node **buffer, node *list) ;
context *open(tuple tuple, context *C, node **buffer);
/**********************/

/** BUFFER FUNCTIONS **/
void enqueue(node** last, tuple data);
void free_buffer(node *head);
void print_buffer(node* head);
/**********************/

/** SECRET FUNCTIONS **/
bool tick(tuple data);
tuple *extract_data(window window, node *head);
/**********************/

/**
 * Perform window eviction when frame is closed
 * @param buffer pointer to the head of list representing the current frame to be evicted
 */
void evict(window window, tuple* content){
    printf("FRAME [%ld, %ld] -> ", window.t_start, window.t_end);
    for (int i = 0; i < window.size; i++){
        printf("(ts: %ld, value: %f) ", content[i].timestamp, content[i].A);
    }
    printf("\n");
}

bool tick(tuple data) {
    if (data.timestamp >= 0) return true;
    else return false;
}

/**
 * Binary search in the buffer of every value with timestamp between window.start and window.end
 * @param window struct containing timestamps and size of the window
 * @param head pointer to head of the buffer
 * @return the tuples contained in the window
 */
tuple *extract_data(window window, node *head) {
    tuple *found = (tuple *) malloc(window.size * sizeof(tuple));

    node *left = head;
    node *right = NULL;

    while (left != right) {
        node *mid = left;
        int count = 0;

        while (mid != right) {
            mid = mid->next;
            count++;
        }

        mid = left;
        for (int i = 0; i < count / 2; i++) {
            mid = mid->next;
        }

        if (mid->data.timestamp == window.t_start) { // found
            for (int j = 0; j < window.size; j++) {
                found[j] = mid->data;
                mid = mid->next;
            }
            return found;
        } else if (mid->data.timestamp < window.t_start) {
            left = mid->next;
        } else {
            right = mid;
        }
    }

    return NULL;
}

/**
 * Insert a tuple in the buffer
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

void print_buffer(node* head) {
    while (head != NULL) {
        printf("[VID: %ld, A: %f] ", head->data.timestamp, head->data.A);
        head = head->next;
    }
    printf("\n");
}

void free_buffer(node* head) {
    node* current = head;
    while (current != NULL) {
        node* next = current->next;
        free(current);
        current = next;
    }
}

int main(int argc, char *argv[]) {

    if (argc != 4) {
        perror("Usage: <input file path> <Frame type (THRESHOLD = 0 | DELTA = 1 | AGGREGATE = 2)> <Report policy (ON CLOSE = 0 | ON UPDATE = 1)>\n ");
        return 1;
    }
    char *file_path = argv[1];
    /**
    * THRESHOLD = 0 -> Detect time periods where a specified attribute is over under a specified threshold.
    * DELTA = 1 -> A new frame starts whenever the value of a particular attribute changes by more than an amount.
    * AGGREGATE = 2 -> End a frame when an aggregate of the values of a specified attribute within the frame exceeds a threshold.
     **/
    int FRAME = atoi(argv[2]);
    /**
    * ON CLOSE = 0 -> Report every time a frame is closed
    * ON UPDATE = 1 -> Report every time a frame is updated
     **/
    int REPORT_POLICY = atoi(argv[3]);

    int num_tuples = 0;
    char line[MAX_CHARS];

    // initialize buffer
    node* head_buffer = NULL;
    node* tail_buffer = NULL;
    node* buffer_iter = NULL;
    node* buffer = NULL;
    node* current = NULL;
    node* tail = NULL;
    bool head;

    // initialize context
    context* C = (context*)malloc(sizeof(context));
    C->frame_type = FRAME;
    C->count = 0;
    C->start = false;
    C->v = -1;
    tuple curr_tuple;
    int size;
    int last; // used to propagate last operation for report

    // declare SECRET variables
    window curr_window;
    tuple* content;

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

    // process stream
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

        /** Start Add **/
        enqueue(&tail_buffer, data);
        /** End Add **/

        if (num_tuples == 0) head_buffer = tail_buffer; // save pointer to first element of the list representing the buffer
        num_tuples++; // count the processed tuples

        if (tick(data)) {

            /** Start Scope **/
            // Frame Operator
            buffer_iter = head_buffer; // initialize pointer that iterates over the buffer
            head = true;
            while (buffer_iter != NULL) {
                curr_tuple = buffer_iter->data;
                // process tuple
                if (close_pred(curr_tuple, C)) {
                    C = close(curr_tuple, C, current);
                    last = 0;
                }
                if (update_pred(curr_tuple, C)) {
                    C = update(curr_tuple, C, &tail, current);
                    curr_window.size++;
                    last = 1;
                }
                if (open_pred(curr_tuple, C)) {
                    C = open(curr_tuple, C, &tail);
                    current = tail;
                    curr_window.size = 1;
                    if (head) { // save pointer to first element of the list representing the buffer
                        buffer = tail;
                        head = false;
                    }
                    last = 2;
                    curr_window.t_start = curr_tuple.timestamp;
                }
                curr_window.t_end = curr_tuple.timestamp;
                buffer_iter = buffer_iter->next;
            }
            /** End Scope **/ // result is curr_window

            // clean context
            free_buffer(buffer);
            C->frame_type = FRAME;
            C->count = 0;
            C->start = false;
            C->v = -1;
            current = NULL;
            tail = NULL;

            /** Start Content **/
            buffer_iter = head_buffer; // initialize pointer that iterates over the buffer
            content = extract_data(curr_window, buffer_iter);
            /** End Content **/

            /** Start Report **/
            if (REPORT_POLICY == last) { /// NOTE: to generalize the process it is possible to compare the current with the previous content
            /** End Report **/

                /** Start Evict **/
                evict(curr_window, content);
                /** End Evict **/

            }
            free(content);
        }

    }
    fclose(file); // close input file stream

    printf("--------------------\nBuffer: ");
    print_buffer(head_buffer);

    // free up memory
    free_buffer(head_buffer);
    free(C);

    return 0;
}

// Open a new frame and insert the current processed tuple.
context *open(tuple tuple, context *C, node **buffer) {
    /** Add **/
    enqueue(buffer, tuple);
    /*********/
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
    }
}

// Update the current frame, extends it to include the current processed tuple.
context *update(tuple tuple, context *C, node **buffer, node *list) {
    /** Add **/
    enqueue(buffer, tuple);
    /*********/
    switch (C->frame_type) {
        case 0:
            C->count++;
            return C;
        case 1:
            return C;
        case 2:
            C->v = aggregation(AGGREGATE, list);
            return C;
    }
}

// Close the current frame and check for global conditions to eventually evict.
// Removes the evicted/discarded frame from the buffer.
context *close(tuple tuple, context *C, node* buffer) {
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

double aggregation(int agg_function, node* buffer){
    switch (agg_function) {
        case 0: return avg(buffer);
        case 1: return sum(buffer);
        default:
            perror("Error while selecting aggregation function");
            return -1;
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

#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <math.h>
#include <string.h>
#include <time.h>
#include <limits.h>

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
#define MAX_FRAMES 35000 // Max admitted size of multi-buffer, pay attention to choose an evict policy that does not cause overflow

#define DEBUG false

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
    node* current;
    node* tail;
    context* context;
    int index;
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
void insertInOrder(node** head, tuple data);
void free_buffer(node *head);
void print_buffer(node* head);
/**********************/

/** SECRET FUNCTIONS **/
bool tick(tuple data);
node *extract_data(long t_start, node *head);
/**********************/

/**
 * Perform window eviction when frame is closed
 * @param buffer pointer to the head of list representing the current frame to be evicted
 */
void evict(window window, node* content){
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
node *extract_data(long t_start, node *head) {

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

        if (mid->data.timestamp == t_start) { // found
            return mid;
        } else if (mid->data.timestamp < t_start) {
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

void insertInOrder(node** head, tuple data) {
    node* new_node = (node*)malloc(sizeof(node));
    new_node->data = data;
    new_node->next = NULL;

    if (*head == NULL || data.timestamp <= (*head)->data.timestamp) {
        new_node->next = *head;
        *head = new_node;
    } else {
        node* current = *head;
        while (current->next != NULL && current->next->data.timestamp <= data.timestamp) {
            current = current->next;
        }
        new_node->next = current->next;
        current->next = new_node;
    }
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

    if (argc != 6) {
        perror("Usage: <input file path> <Frame type (THRESHOLD = 0 | DELTA = 1 | AGGREGATE = 2)> <Report policy (ON CLOSE = 0 | ON UPDATE = 1)> <Order policy (IN ORDER = 0 | OUT OF ORDER = 1)> <Buffer type (0 = SINGLE BUFFER | 1 = MULTI BUFFER)\n ");
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
    /**
    * IN ORDER = 0 -> Every tuple is expected to arrive with increasing timestamp
    * OUT OF ORDER = 1 -> Tuple can arrive with out of order timestamps
     **/
    int ORDER_POLICY = atoi(argv[4]);
    /**
    * SINGLE BUFFER = 0
    * MULTI BUFFER = 1
     **/
    int BUFFER_TYPE = atoi(argv[5]);

    // TUPLES
    int num_tuples = 0;
    char line[MAX_CHARS];

    // BUFFER
    node* head_buffer = NULL;
    node* tail_buffer = NULL;
    node* buffer_iter = NULL;
    node* buffer = NULL;
    node* current = NULL;
    node* tail = NULL;
    bool head;

    // CONTEXT
    context* C = (context*)malloc(sizeof(context));
    C->frame_type = FRAME;
    C->count = 0;
    C->start = false;
    C->v = -1;
    tuple curr_tuple;
    window frames[MAX_FRAMES]; // multi buffer data structure
    int frames_count = 0;

    // SECRET
    window curr_window;
    window report_window;
    node* content;
    int last; // used to propagate last operation for report

    // BENCHMARKING
    clock_t begin_add;
    clock_t begin_scope;
    clock_t begin_content;
    clock_t end_add;
    clock_t end_scope;
    clock_t end_content;
    double time_spent_add;
    double time_spent_scope;
    double time_spent_content;
    printf("action,time,n\n");

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

        if (BUFFER_TYPE == 0){ // Single Buffer
            /** Start Add **/
            begin_add = clock();
            if (ORDER_POLICY == 0) { // In-Order
                enqueue(&tail_buffer, data);
                if (num_tuples == 0) head_buffer = tail_buffer; // save pointer to first element of the list representing the buffer
            }
            else if (ORDER_POLICY == 1) { // Out-of-Order (eager sort)
                insertInOrder(&head_buffer, data);
            }
            num_tuples++; // count the processed tuples
            end_add = clock();
            time_spent_add = (double) (end_add - begin_add) / CLOCKS_PER_SEC;
            //printf("add,%f,%d\n", time_spent_add,num_tuples);
            /** End Add **/
        }

        if (tick(data)) {

            if (BUFFER_TYPE == 0){ // Single Buffer
                /** Start Scope **/
                begin_scope = clock();
                // TODO: lazy sort single buffer: prima di iniziare il frame operator faccio un sorting del buffer
                // Frame Operator
                buffer_iter = head_buffer; // initialize pointer that iterates over the buffer
                head = true;
                curr_window.size = 0;
                while (buffer_iter != NULL) {
                    curr_tuple = buffer_iter->data;
                    // process tuple
                    if (close_pred(curr_tuple, C)) {
                        C = close(curr_tuple, C, current);
                        last = 0;
                        report_window.t_start = curr_window.t_start;
                    }
                    if (update_pred(curr_tuple, C)) {
                        C = update(curr_tuple, C, &tail, current);
                        curr_window.size++;
                        last = 1;
                        report_window.t_start = curr_window.t_start;
                    }
                    if (open_pred(curr_tuple, C)) {
                        C = open(curr_tuple, C, &tail);
                        current = tail;
                        curr_window.size = 1;
                        if (head) { // save pointer to first element of the list representing the buffer
                            buffer = tail;
                            head = false;
                        }
                        curr_window.t_start = curr_tuple.timestamp;
                        last = 2;
                    }
                    curr_window.t_end = curr_tuple.timestamp;
                    if (buffer_iter->next != NULL && buffer_iter->next->data.timestamp <= data.timestamp) buffer_iter = buffer_iter->next; // create frames until the processed tuple is found
                    // TODO in this way when ooo we don't find ts end, compute the whole frame, pay attention to return the pointer and the size of the frame
                    else buffer_iter = NULL;
                }

                end_scope = clock();
                time_spent_scope = (double)(end_scope - begin_scope) / CLOCKS_PER_SEC;
                //printf("scope,%f,%d\n", time_spent_scope,num_tuples);
                /** End Scope **/
            } else if (BUFFER_TYPE == 1){ // Multi Buffer
                /** Start Scope **/
                begin_scope = clock();
                for (int i = 0; i < frames_count; i++){ // TODO make frames[] a circular array coupled with the eviction policy: evict after X frames are created with X < MAX_FRAMES
                    if (data.timestamp >= frames[i].t_start && (data.timestamp <= frames[i].t_end || frames[i].t_end == -1)){
                        curr_window.index = i;
                        current = frames[i].current;
                        tail = frames[i].tail;
                    }
                }
                if (frames_count == 0) {
                    current = NULL;
                    tail = NULL;
                    curr_window.index = -1;
                }
                end_scope = clock();
                time_spent_scope = (double)(end_scope - begin_scope) / CLOCKS_PER_SEC;
                //printf("scope,%f,%d\n", time_spent_scope,num_tuples);
                /** End Scope **/

                /** Start Add **/
                begin_add = clock();

                curr_tuple = data;

                if (close_pred(curr_tuple, C)) {
                    C = close(curr_tuple, C, current);
                    last = 0;

                    frames[curr_window.index].t_end = curr_tuple.timestamp;
                    report_window.index = curr_window.index;
                }
                if (update_pred(curr_tuple, C)) {
                    C = update(curr_tuple, C, &tail, current);
                    last = 1;

                    frames[curr_window.index].size++;
                    frames[curr_window.index].tail = tail;
                    report_window.index = curr_window.index;
                }
                if (open_pred(curr_tuple, C)) {
                    C = open(curr_tuple, C, &tail);
                    current = tail;
                    last = 2;

                    curr_window.index++;
                    frames[curr_window.index].size = 1;
                    frames[curr_window.index].current = current;
                    frames[curr_window.index].tail = tail;
                    frames[curr_window.index].t_start = curr_tuple.timestamp;
                    frames[curr_window.index].t_end = -1;
                    frames_count++;
                    if (frames_count == MAX_FRAMES) frames_count = 1;
                }

                end_add = clock();
                time_spent_add = (double) (end_add - begin_add) / CLOCKS_PER_SEC;
                //printf("add,%f,%d\n", time_spent_add,num_tuples);
                /** End Add **/

            } else printf("Buffer type %d not recognized", BUFFER_TYPE);



            /** DEBUG CODE **/
            // print content every time is extracted
            if (DEBUG) {
                if (BUFFER_TYPE == 0) {
                    int print_count = 0;
                    printf("frame [%ld, %ld] (size %ld) -> ", curr_window.t_start, curr_window.t_end, curr_window.size);
                    while (content != NULL && print_count < curr_window.size) {
                        printf("(ts: %ld, value: %f) ", content->data.timestamp, content->data.A);
                        content = content->next;
                        print_count++;
                    }
                }
                if (BUFFER_TYPE == 1) {
                    printf("\ntot frames: %d, curr frame size: %ld\n", frames_count, frames[curr_window.index].size);
                    int print_count = 0;
                    printf("frame [%ld, %ld] -> ", frames[curr_window.index].t_start, frames[curr_window.index].t_end);
                    node *iterator = frames[curr_window.index].current;
                    while (iterator != NULL && print_count < frames[curr_window.index].size) {
                        printf("(ts: %ld, value: %f) ", iterator->data.timestamp,
                               iterator->data.A);
                        iterator = iterator->next;
                        print_count++;
                    }
                }
                printf("\n");
            }
            /** END DEBUG CODE **/



            /** Start Report **/
            if (REPORT_POLICY == last) {
            /** End Report **/

                /** Start Content **/
                begin_content = clock();
                buffer_iter = head_buffer; // initialize pointer that iterates over the buffer
                if (BUFFER_TYPE == 0) content = extract_data(report_window.t_start, buffer_iter); // binary search
                if (BUFFER_TYPE == 1) content = frames[report_window.index].current;
                end_content = clock();
                time_spent_content = (double)(end_content - begin_content) / CLOCKS_PER_SEC;
                //printf("content,%f,%d\n", time_spent_content,num_tuples);
                /** End Content **/

                /** Start Evict **/
                evict(curr_window, content); // TODO: implement
                                             // Policy: after X frames are created, evict first Y frames
                                             // SINGLE BUFFER: you need to find a tuple in the buffer that is the pointer to the head of the buffer after eviction
                                             // pointer to head always available O(1), pointer to head after eviction found in O(logn)
                                             // free buffer between the two pointer (not counted in the complexity measure)
                                             // MULTI BUFFER: all frames available by their index, first Y frames are the array with index 0 to Y
                                             // array access in O(1)
                /** End Evict **/


            }
            if (BUFFER_TYPE == 0) free(content);
        }

    }
    fclose(file); // close input file stream

    // free up memory
    free_buffer(head_buffer);
    free(C);

    return 0;
}

// Open a new frame and insert the current processed tuple.
context *open(tuple tuple, context *C, node **buffer) {
    enqueue(buffer, tuple);
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
    enqueue(buffer, tuple);
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

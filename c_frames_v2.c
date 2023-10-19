#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <math.h>
#include <string.h>
#include <time.h>

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

#define MAX_CHARS 256 // Max admitted characters in a line representing event
#define MAX_FRAMES 120000 // Max admitted size of multi-buffer, pay attention to choose an evict policy that does not cause overflow
#define MAX_TUPLES 200000

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
    long index;
} window;

int delay_execution(int ms){
    const struct timespec ts = {
            ms / 1000, /* seconds */
            (ms % 1000) * 1000 * 1000 /* nano seconds */
    };
    return nanosleep(&ts, NULL);
}


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
void insertInOrder(window frames[], long W, window newWindow, int startIdx) ;
void free_buffer(window frames[], int num_tuples);
void free_temp_buffer(node* head);
void print_buffer(node* head);
/**********************/

/** SECRET FUNCTIONS **/
bool tick(tuple data);
int extract_data(window frames[], int startIdx, int W, long targetTimestamp);
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

int compareTimestamp(const void* a, const void* b) {
    const node* nodeA = ((window*)a)->current;
    const node* nodeB = ((window*)b)->current;
    return (nodeA->data.timestamp - nodeB->data.timestamp);
}

int customBinarySearch(window frames[], int startIdx, int endIdx, long targetTimestamp) {
    while (startIdx <= endIdx) {

        int mid = startIdx + (endIdx - startIdx) / 2;

        delay_execution(10);

        if (frames[mid].current->data.timestamp == targetTimestamp) {
            return mid; // Element found
        }

        if (frames[mid].current->data.timestamp < targetTimestamp) {
            startIdx = mid + 1;
        } else {
            endIdx = mid - 1;
        }
    }

    return -1; // Element not found
}


int extract_data(window frames[], int startIdx, int W, long targetTimestamp) {

    window key;
    key.current = (node*)malloc(sizeof(node));
    key.current->data.timestamp = targetTimestamp;

    window* result = (window*)bsearch(&key, &frames[startIdx], W - startIdx, sizeof(window), compareTimestamp);

    free(key.current);

    if (result != NULL) {
        return result - frames;
    }

    return -1;

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

void insertInOrder(window frames[], long W, window newWindow, int startIdx) {

    if (W >= MAX_FRAMES) {
        printf("Error: Array is full, cannot insert.\n");
        return;
    }

    int insertIndex = 0;
    while (insertIndex < W && frames[insertIndex].current->data.timestamp < newWindow.current->data.timestamp) {
        insertIndex++;
    }

    // move the elements
    for (long i = W; i > insertIndex; i--) {
        frames[i] = frames[i - 1];
    }

    frames[insertIndex] = newWindow;
}

void heapify(window arr[], int n, int i) {
    int largest = i;
    int left = 2 * i + 1;
    int right = 2 * i + 2;

    if (left < n && compareTimestamp(&arr[left], &arr[largest]) > 0) {
        largest = left;
    }

    if (right < n && compareTimestamp(&arr[right], &arr[largest]) > 0) {
        largest = right;
    }

    if (largest != i) {
        window temp = arr[i];
        arr[i] = arr[largest];
        arr[largest] = temp;

        heapify(arr, n, largest);
    }
}

void heapSort(window arr[], int n) {
    for (int i = n / 2 - 1; i >= 0; i--) {
        heapify(arr, n, i);
    }

    for (int i = n - 1; i > 0; i--) {
        window temp = arr[0];
        arr[0] = arr[i];
        arr[i] = temp;

        heapify(arr, i, 0);
    }
}

node* newNode(tuple data){
    node* new_node = (node*)malloc(sizeof(node));
    new_node->data = data;
    new_node->next = NULL;
    return new_node;
}

void free_buffer(window frames[], int num_tuples) {
    for(int i = 0; i < num_tuples; i++){
        free(frames[i].current);
    }
}

void free_temp_buffer(node* head) {
    node* current = head;
    while (current != NULL) {
        node* next = current->next;
        free(current);
        current = next;
    }
}


int main(int argc, char *argv[]) {

    if (argc != 8) {
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
    * OUT OF ORDER (eager sort) = 1 -> Tuple can arrive with out of order timestamps, sorting is executed eagerly
    * OUT OF ORDER (lazy sort) = 2 -> Tuple can arrive with out of order timestamps, sorting is executed lazily
     **/
    int ORDER_POLICY = atoi(argv[4]);
    /**
    * SINGLE BUFFER = 0
    * MULTI BUFFER = 1
     **/
    int BUFFER_TYPE = atoi(argv[5]);
    /**
     * EVICTION POLICY PARAMETERS
     */
    // SINGLE BUFFER: after X frames created / MULTI BUFFER: after X ms passed
    // evict first Y frames
    // Condition: SB Y < X, MB: in X ms no more than Y frames created
    int X = atoi(argv[6]);
    int Y = atoi(argv[7]);


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
    tuple tuples[MAX_TUPLES];
    long frames_count = 1;
    long W = 0;
    long evict_head;
    node *new_buffer_head;
    long multi_buffer_head = 0;
    long single_buffer_head = 0;
    long frames_count_iterator = 0;
    long multi_buffer_head_iterator = 0;
    int buffer_size = 0;
    bool first =true;

    // SECRET
    window curr_window;
    window report_window;
    node* content = NULL;
    int last=-1; // used to propagate last operation for report

    // BENCHMARKING
    clock_t begin_add;
    clock_t begin_scope;
    clock_t begin_content;
    clock_t begin_evict;
    clock_t end_add;
    clock_t end_scope;
    clock_t end_content;
    clock_t end_evict;
    double time_spent_add;
    double time_spent_scope;
    double time_spent_content;
    double time_spent_evict;
    if (BUFFER_TYPE == 0) printf("add,scope,content,evict,n\n");
    if (BUFFER_TYPE == 1) printf("scope,add,content,evict,n\n");

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
            if (ORDER_POLICY == 0 || ORDER_POLICY == 2) { // In-Order & Out-of-Order (lazy sort)
                frames[num_tuples].current = newNode(data);
            }
            else if (ORDER_POLICY == 1) { // Out-of-Order (eager sort)
                window new_window;
                new_window.current = newNode(data);
                insertInOrder(frames, num_tuples, new_window, single_buffer_head);
            }
            end_add = clock();
            time_spent_add = (double) (end_add - begin_add) *1000 / CLOCKS_PER_SEC;
            printf("%f,", time_spent_add);
            /** End Add **/
        }
        num_tuples++; // index of the tail of the buffer

        if (tick(data)) {

            if (BUFFER_TYPE == 0){ // Single Buffer
                /** Start Scope **/
                begin_scope = clock();
                if (ORDER_POLICY == 2){ // Out-of-Order (lazy sort)
                    heapSort(frames + single_buffer_head, num_tuples - single_buffer_head);
                }
                // Frame Operator
                head = true;
                curr_window.size = 0;
                buffer = NULL;
                tail = NULL;
                long frames_count_tmp = 0;
                long iterator = single_buffer_head;
                while (iterator < num_tuples) {
                    curr_tuple = frames[iterator].current->data;
                    // process tuple
                    last = -1;
                    if (close_pred(curr_tuple, C)) {
                        C = close(curr_tuple, C, current);
                        last = 0;
                        if (data.timestamp >= curr_window.t_start){
                            report_window.t_start = curr_window.t_start;
                            //report_window.t_end = curr_tuple.timestamp;
                            report_window.size = curr_window.size;
                        }
                    }
                    if (update_pred(curr_tuple, C)) {
                        C = update(curr_tuple, C, &tail, current);
                        curr_window.size++;
                        last = 1;
                        report_window.t_start = curr_window.t_start;
                        report_window.t_end = curr_tuple.timestamp;
                        report_window.size = curr_window.size;
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
                        frames_count_tmp++;
                        if (frames_count_tmp == Y+1) {
                            evict_head = curr_window.t_start;
                        }
                    }
                    //curr_window.t_end = curr_tuple.timestamp;
                    iterator++;
                }
                frames_count = frames_count_tmp;
                C->count = 0;
                C->start = false;
                C->v = -1;
                end_scope = clock();
                time_spent_scope = (double)(end_scope - begin_scope) *1000 / CLOCKS_PER_SEC;
                printf("%f,", time_spent_scope);
                /** End Scope **/
            } else if (BUFFER_TYPE == 1){ // Multi Buffer
                /** Start Scope **/
                begin_scope = clock();

                if (frames_count == 0 && first) {
                    current = NULL;
                    tail = NULL;
                    curr_window.index = -1;
                    first = false;
                } else {
                    if (multi_buffer_head < frames_count) {
                        for (long i = multi_buffer_head; i < frames_count; i++) {
                            if (data.timestamp >= frames[i].t_start &&
                                (data.timestamp <= frames[i].t_end || frames[i].t_end == -1)) {
                                curr_window.index = i;
                                current = frames[i].current;
                                tail = frames[i].tail;
                            }
                        }
                    } else if (multi_buffer_head >= frames_count) {
                        for (long i = multi_buffer_head; i < MAX_FRAMES; i++) {
                            if (data.timestamp >= frames[i].t_start &&
                                (data.timestamp <= frames[i].t_end || frames[i].t_end == -1)) {
                                curr_window.index = i;
                                current = frames[i].current;
                                tail = frames[i].tail;
                            }
                        }
                        for (long i = 0; i < frames_count; i++) {
                            if (data.timestamp >= frames[i].t_start &&
                                (data.timestamp <= frames[i].t_end || frames[i].t_end == -1)) {
                                curr_window.index = i;
                                current = frames[i].current;
                                tail = frames[i].tail;
                            }
                        }
                    }
                }


                end_scope = clock();
                time_spent_scope = (double)(end_scope - begin_scope) *1000 / CLOCKS_PER_SEC;
                printf("%f,", time_spent_scope);
                /** End Scope **/

                /** Start Add **/
                begin_add = clock();

                curr_tuple = data;

                last = -1;
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

                    if (frames_count == MAX_FRAMES) {
                        frames_count = 0;
                        curr_window.index = -1;
                    }
                    curr_window.index++;
                    frames[curr_window.index].size = 1;
                    frames[curr_window.index].current = current;
                    frames[curr_window.index].tail = tail;
                    frames[curr_window.index].t_start = curr_tuple.timestamp;
                    frames[curr_window.index].t_end = -1;
                    frames_count++;
                    W++;
                }

                end_add = clock();
                time_spent_add = (double) (end_add - begin_add) *1000 / CLOCKS_PER_SEC;
                printf("%f,", time_spent_add);
                /** End Add **/

            } else printf("Buffer type %d not recognized", BUFFER_TYPE);

            /** Start Report **/
            if (REPORT_POLICY == last) {
            /** End Report **/

                /** Start Content **/
                begin_content = clock();
                int content_index;
                if (BUFFER_TYPE == 0) content_index = customBinarySearch(frames, single_buffer_head, num_tuples-1, report_window.t_start); // binary search
                if (BUFFER_TYPE == 1) content = frames[report_window.index].current;
                end_content = clock();
                time_spent_content = (double)(end_content - begin_content) *1000 / CLOCKS_PER_SEC;
                printf("%f,", time_spent_content);
                /** End Content **/


                /** DEBUG CODE **/
                // print content
                if (DEBUG) {
                    if (BUFFER_TYPE == 0) {
                        printf("\n %ld: [%ld, %ld] (size %ld) {content index = %d} -> ", frames_count, report_window.t_start, report_window.t_end, report_window.size, content_index);
                        for (int i = content_index; i < report_window.size + content_index; i++){
                            if (i == MAX_FRAMES) i = 0;
                            printf("(ts: %ld, value: %f) ", frames[i].current->data.timestamp, frames[i].current->data.A);
                        }
                    }
                    if (BUFFER_TYPE == 1) {
                        printf("\ntot frames: %ld, curr frame size: %ld\n", ((frames_count - multi_buffer_head)%MAX_FRAMES+MAX_FRAMES)%MAX_FRAMES, frames[report_window.index].size);
                        int print_count = 0;
                        printf("frame [%ld, %ld] -> ", frames[report_window.index].t_start, frames[report_window.index].t_end);
                        //node *iterator = frames[curr_window.index].current;
                        while (content != NULL && print_count < frames[report_window.index].size) {
                            printf("(ts: %ld, value: %f) ", content->data.timestamp,
                                   content->data.A);
                            content = content->next;
                            print_count++;
                        }
                    }
                    printf("\n");

                }
                /** END DEBUG CODE **/


            } else printf("-1,");
            if (BUFFER_TYPE == 0) free_temp_buffer(buffer);

            if (X != -1) {
                if (BUFFER_TYPE == 0) { // SINGLE BUFFER: find a tuple in the buffer that is the pointer to the head of the buffer after eviction
                    if (frames_count % X == 0) { // check eviction policy
                        // Policy: after X frames are created, evict first Y frames
                        /** Start Evict **/
                        begin_evict = clock();
                        single_buffer_head = customBinarySearch(frames, single_buffer_head, num_tuples, evict_head);
                        // todo: implement binary search on circular array to get infinite available memory
                        frames_count = X - Y;
                        end_evict = clock();
                        time_spent_evict = (double)(end_evict - begin_evict) *1000 / CLOCKS_PER_SEC;
                        printf("%f,%d\n", time_spent_evict,num_tuples);
                        /** End Evict **/

                    } else printf("-1,%d\n", num_tuples);
                }
                if (BUFFER_TYPE == 1) {
                    if (data.timestamp % X == 0) { // check eviction policy
                        begin_evict = clock();
                        /** Start Evict **/
                        multi_buffer_head = (multi_buffer_head + Y) % MAX_FRAMES;
                        end_evict = clock();
                        time_spent_evict = (double)(end_evict - begin_evict) *1000 / CLOCKS_PER_SEC;
                        printf("%f,%ld\n", time_spent_evict,W);
                        /** End Evict **/
                    } else printf("-1,%ld\n", W);
                }
            }
            else if (BUFFER_TYPE == 1) printf("-1,%ld\n", W);
            else if (BUFFER_TYPE == 0) printf("-1,%d\n", num_tuples);
        }

    }
    fclose(file); // close input file stream

    // free up memory
    if (BUFFER_TYPE == 0) free_buffer(frames, num_tuples);
    if (BUFFER_TYPE == 1) free_temp_buffer(frames[0].current);
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

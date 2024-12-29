#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <time.h>
#include <stdint.h>

#define max_tuples 200000// buffer size

#define DEBUG false

int delay_execution(int ms){
    const struct timespec ts = {
            ms / 1000, /* seconds */
            (ms % 1000) * 1000 * 1000 /* nano seconds */
    };
    return nanosleep(&ts, NULL);
}

typedef struct Tuple {
    long timestamp;
    long key;
    double value;
} tuple;

typedef struct Window {
    long start;
    long end;
} window;

bool tick();

void insertOrderedByTimestamp(tuple buffer[], int size, int *currentSize, tuple newTuple);

void heapSort(tuple arr[], int n);

void heapify(tuple arr[], int n, int i);

void appendTuple(tuple buffer[], int *currentSize, int maxSize, tuple newTuple);

window* compute_windows(long timestamp, int d, int s, int k);

int binarySearchByTimestamp(tuple buffer[], int left, int right, long targetTimestamp);

int main(int argc, char *argv[]) {

    if (argc != 5) {
        fprintf(stderr, "Usage: %s input_file mode d s\n", argv[0]);
        return 1;
    }

    int D = -1;
    int S = 0;

    char line[256];
    char *file_path = argv[1];
    char *mode = argv[2];
    D = atoi(argv[3]);
    S = atoi(argv[4]);

    bool in_order = false, lazy_sort = false, eager_sort = false;

    if (strcmp(mode, "--in-order") == 0) {
        in_order = true;
    } else if (strcmp(mode, "--lazy-sort") == 0) {
        lazy_sort = true;
    } else if (strcmp(mode, "--eager-sort") == 0) {
        eager_sort = true;
    } else {
        fprintf(stderr, "Error: Invalid mode. Choose --in-order, --lazy-sort, or --eager-sort.\n");
        return 1;
    }
    if (lazy_sort + eager_sort + in_order > 1) {
        fprintf(stderr, "Error: Only one mode can be selected at a time.\n");
        return 1; // Exit with error code
    }

    tuple buffer[max_tuples];
    int buffer_size = 0;
    int append_index = 0;
    tuple content;
    int content_index;

    for (int i = 2; i < argc; i++) {
        if (strcmp(argv[i], "--in-order") == 0) {
            in_order = true;
        } else if (strcmp(argv[i], "--lazy-sort") == 0) {
            lazy_sort = true;
        } else if (strcmp(argv[i], "--eager-sort") == 0) {
            eager_sort = true;
        } else if (strcmp(argv[i], "-D") == 0) {
            if (i + 1 < argc) {
                D = atoi(argv[++i]);
            }
        } else if (strcmp(argv[i], "-S") == 0) {
            if (i + 1 < argc) { //
                S = atoi(argv[++i]);
            }
        }
    }
    if (lazy_sort + eager_sort > 1) {
        fprintf(stderr, "Error: Only one sort option (--lazy-sort or --eager-sort) can be selected at a time.\n");
        return 1; // Exit with error code
    }
    int k = D / S;


    srand(time(0));

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
    printf("add,scope,content,n\n");

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

    while (fgets(line, 256, file) != NULL) {
        char *token = strtok(line, ",");
        int count = 0;

        tuple data;
        data.timestamp = -1;
        data.value = -1;
        while (token != NULL) {
            if (count == 0) {
                data.timestamp = strtol(token, NULL, 10);
            } else if (count == 1) {
                data.key = strtol(token, NULL, 10);
            } else if (count == 2) {
                data.value = strtod(token, NULL);
            }
            token = strtok(NULL, ",");
            count++;
        }

        /// ADD
        begin_add = clock();
        if (in_order || lazy_sort) {
            appendTuple(buffer, &buffer_size, max_tuples, data);
        } else if (eager_sort){
            insertOrderedByTimestamp(buffer, max_tuples, &buffer_size, data);
        }
        end_add = clock();
        time_spent_add = (double)(end_add - begin_add) *1000 / CLOCKS_PER_SEC;
        printf("%f,", time_spent_add);

        /// SCOPE
        begin_scope = clock();
        if (lazy_sort) {
            heapSort(buffer, buffer_size);
        }
        window* scope = compute_windows(data.timestamp, D, S, k);
        end_scope = clock();
        time_spent_scope = (double)(end_scope - begin_scope) *1000 / CLOCKS_PER_SEC;
        printf("%f,", time_spent_scope);

        /// CONTENT
        if (tick() == 1) {
            begin_content = clock();
            content_index = binarySearchByTimestamp(buffer, 0, buffer_size - 1, scope[0].start);
            content = buffer[content_index];
            end_content = clock();
            time_spent_content = (double)(end_content - begin_content) *1000 / CLOCKS_PER_SEC;
            printf("%f,%d\n", time_spent_content, buffer_size);
        } else printf("-1,%d\n", buffer_size);

        int index = data.timestamp / S;
        int iter = k;
        while (iter > 0) {
            int start = scope[iter - 1].start;
            int end = scope[iter - 1].end;
            if (start > end){
                exit(255);
            }
            index--;
            index >= 0 ? iter-- : (iter = 0);
        }
        free(scope);

        if (DEBUG) printf("ts: %ld, buffer size: %d\n", content.timestamp, buffer_size);

    }
    fclose(file);

    return 0;

}

int binarySearchByTimestamp(tuple buffer[], int left, int right, long targetTimestamp) {
    while (left <= right) {
        int mid = left + (right - left) / 2;

        //delay_execution(5);

        // Check if the targetTimestamp is present at mid
        if (buffer[mid].timestamp == targetTimestamp)
            return mid;

        // If targetTimestamp is greater, ignore left half
        if (buffer[mid].timestamp < targetTimestamp)
            left = mid + 1;
            // If targetTimestamp is smaller, ignore right half
        else
            right = mid - 1;
    }

    // If we reach here, then the element was not present
    return -1;
}

window* compute_windows(long timestamp, int d, int s, int k) {
    window* result;
    long start, end;

    result = (window*) malloc (k * sizeof(window));

    start = timestamp - timestamp % s;
    end = timestamp + (d - timestamp % s);

    int i = k-1;
    while(i >= 0) {
        result[i].start = start;
        result[i].end = end;
        start -= s;
        end -= s;
        start >= 0 ? i-- : (i = -1);
    }

    return result;
}

void appendTuple(tuple buffer[], int *currentSize, int maxSize, tuple newTuple) {
    if (*currentSize >= maxSize) {
        // The array is full, cannot append a new tuple
        printf("Unable to append tuple: the array is full.\n");
        return;
    }

    // Append the new tuple at the end of the array
    buffer[*currentSize] = newTuple;

    // Increment the current size of the array
    (*currentSize)++;
}

void heapify(tuple arr[], int n, int i) {
    int largest = i; // Initialize largest as root
    int left = 2*i + 1; // left = 2*i + 1
    int right = 2*i + 2; // right = 2*i + 2

    // If left child is larger than root
    if (left < n && arr[left].timestamp > arr[largest].timestamp)
        largest = left;

    // If right child is larger than largest so far
    if (right < n && arr[right].timestamp > arr[largest].timestamp)
        largest = right;

    // If largest is not root
    if (largest != i) {
        tuple swap = arr[i];
        arr[i] = arr[largest];
        arr[largest] = swap;

        // Recursively heapify the affected sub-tree
        heapify(arr, n, largest);
    }
}

// Main function to do heap sort
void heapSort(tuple arr[], int n) {
    // Build heap (rearrange array)
    for (int i = n / 2 - 1; i >= 0; i--)
        heapify(arr, n, i);

    // One by one extract an element from heap
    for (int i=n-1; i>0; i--) {
        // Move current root to end
        tuple temp = arr[0];
        arr[0] = arr[i];
        arr[i] = temp;

        // call max heapify on the reduced heap
        heapify(arr, i, 0);
    }
}

void insertOrderedByTimestamp(tuple buffer[], int size, int *currentSize, tuple newTuple) {
    if (*currentSize >= size) {
        printf("Buffer is full, cannot insert new tuple.\n");
        return;
    }

    int pos;
    // Find the position where to insert the new tuple
    for (pos = 0; pos < *currentSize; pos++) {
        if (buffer[pos].timestamp > newTuple.timestamp) {
            break;
        }
    }

    // Shift elements to the right to make space for the new element
    for (int i = *currentSize; i > pos; i--) {
        buffer[i] = buffer[i - 1];
    }

    // Insert the new tuple
    buffer[pos] = newTuple;
    (*currentSize)++;
}

bool tick() {
    //return rand() % 5;
    return 0;
}

#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <time.h>
#include <math.h>

// #define D 200000 // window dimension
// #define S 2000 // slide
#define size 250000 // buffer size

#define DEBUG false
#define FREE true

typedef struct Tuple {
    long timestamp;
    long key;
    double value;
} tuple;

typedef struct Window_Node {
    tuple element;
    struct Window_Node* next;
} window_node;

typedef struct Window {
    long start;
    long end;
    window_node* content;
    window_node* last_content;
} window;

bool tick();

/**
 * Insert a tuple in the buffer
 * @param last pointer to last element of the buffer
 * @param data the tuple to be appended
 */
void add(window_node** last, tuple data) {
    window_node* new_node = (window_node*)malloc(sizeof(window_node));
    new_node->element = data;
    new_node->next = NULL;

    if (*last == NULL) {
        *last = new_node;
        return;
    }

    window_node* last_node = *last;
    last_node->next = new_node;
    *last = new_node;
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

int main(int argc, char *argv[]) {

    int D = atoi(argv[3]);
    int S = atoi(argv[4]);

    int k = D / S;

    char line[256];
    char *file_path = argv[1];

    int index;
    window buffer[size] = {0};

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
    printf("scope,add,content\n");

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

        /*** SCOPE ***/
        begin_scope = clock();
        window* scope = compute_windows(data.timestamp, D, S, k);
        end_scope = clock();
        time_spent_scope = (double)(end_scope - begin_scope) *1000 / CLOCKS_PER_SEC;
        printf("%f,", time_spent_scope);

        index = ceil(data.timestamp / S);
        int iter = k;
        end_add = 0;
        while (iter > 0) {
            buffer[index].start = scope[iter - 1].start;
            buffer[index].end = scope[iter - 1].end;

            /*** ADD ***/
            begin_add = clock();
            add(&buffer[index].last_content, data);
            end_add += clock() - begin_add;

            if (buffer[index].content == NULL){
                buffer[index].content = buffer[index].last_content;
            }

            index--;
            index >= 0 ? iter-- : (iter = 0);
        }
        time_spent_add = (double) (end_add) *1000 / CLOCKS_PER_SEC;
        printf("%f,", time_spent_add);

        if (tick()) {
            int rand_index = (rand() % (index+k));
            /*** CONTENT ***/
            begin_content = clock();
            window content = buffer[rand_index];
            end_content = clock();
            time_spent_content = (double)(end_content - begin_content) *1000 / CLOCKS_PER_SEC;
            printf("%f\n", time_spent_content);
            if (index > k && content.content == NULL) {
                printf("error while performing content");
                exit(-1);
            }

            if (DEBUG) {
                printf("content at ts %ld, index %d: [%ld, %ld] -> ", data.timestamp, rand_index, content.start,
                       content.end);
                window_node *content_iter = content.content;
                while (content_iter != content.last_content) {
                    printf(" (ts: %ld, value: %f) ", content_iter->element.timestamp, content_iter->element.value);
                    content_iter = content_iter->next;
                }
                printf("\n");
            }
        }
        free(scope);
    }
    fclose(file);

    if (FREE && !DEBUG) {
        window_node *content_iter = buffer[0].content;
        while (content_iter != NULL) {
            window_node * next = content_iter->next;
            free(content_iter);
            content_iter = next;
        }
    }

    return 0;

}

bool tick() {
    return true;
    // return rand()>(RAND_MAX/2);
}

#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <time.h>

#define D 12000 // window dimension
#define S 3000 // slide
#define size 34000// buffer size

#define DEBUG false

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

    int k = D / S;

    char line[256];
    char *file_path = argv[1];

    int index;
    window buffer[size] = {0};

    srand(time(0));

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
        window* scope = compute_windows(data.timestamp, D, S, k);

        index = data.timestamp / S;
        int iter = k;
        while (iter > 0) {
            buffer[index].start = scope[iter - 1].start;
            buffer[index].end = scope[iter - 1].end;

            /*** ADD ***/
            add(&buffer[index].last_content, data);
            if (buffer[index].content == NULL){
                buffer[index].content = buffer[index].last_content;
            }

            index--;
            index >= 0 ? iter-- : (iter = 0);
        }

        if (tick()) {
            int rand_index = (rand() % (index+k));
            /*** CONTENT ***/
            window content = buffer[rand_index];
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

    return 0;

}

bool tick() {
    return rand()>(RAND_MAX/2);
}

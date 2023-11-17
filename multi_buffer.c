#include "frames.h"
#include "stdbool.h"
#include "stdlib.h"
#include "stdio.h"
#include "string.h"
#include "time.h"

#define DEBUG true

int main(int argc, char *argv[]) {

    char *file_path = argv[1];
    /**
    * THRESHOLD = 0 -> Detect time periods where a specified attribute is over under a specified threshold.
    * DELTA = 1 -> A new frame starts whenever the value of a particular attribute changes by more than an amount.
    * AGGREGATE = 2 -> End a frame when an aggregate of the values of a specified attribute within the frame exceeds a threshold.
     **/
    int FRAME = atoi(argv[2]);

    char line[256];

    context* C;
    context C_backup;
    long last_timestamp = 0;

    frame* frames_head = NULL;
    frame* frames_tail;
    frame* frames_iter;
    frame *last_frame;
    frame* new_frames_tail;
    frame* old_frames_head;
    long frames_count = 0;

    node* buffer_currentframe;

    node* tuples_head;
    node* tuples_tail = NULL;
    node* tuples_iter;
    long tuples_count = 0;

    frame* content = NULL;

    /// Benchmark
    clock_t begin_add;
    clock_t begin_scope;
    clock_t begin_split;
    clock_t begin_merge;
    clock_t end_add;
    clock_t end_scope;
    clock_t end_split;
    clock_t end_merge;
    double time_spent_add;
    double time_spent_scope;
    double time_spent_split = -1;
    double time_spent_merge = -1;
    printf("add,scope,split,merge,w,n,nw\n");

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

    C = (context*)malloc(sizeof(context));
    C->frame_type = FRAME;
    C->count = 0;
    C->start = false;
    C->v = -1;

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

        /// In-Order
        if (data.timestamp >= last_timestamp){
            // if ooo but inside current opened frame, just order the tuples buffer, no recompute
            // TUPLES BUFFER
            /** START ADD **/
            begin_add = clock();
            enqueue(&tuples_tail, data);
            if (tuples_count == 0) tuples_head = tuples_tail;
            tuples_count++;
            last_timestamp = data.timestamp;
            end_add = clock();
            time_spent_add = (double) (end_add - begin_add) *1000 / CLOCKS_PER_SEC;
            /** END ADD **/

            tuple curr_tuple = data;
            /** SCOPE START **/
            begin_scope = clock();
            frame* frames_tail_iter = frames_head;
            while (frames_tail_iter != NULL) {
                if (frames_tail_iter->t_start <= curr_tuple.timestamp && frames_tail_iter->t_end >= curr_tuple.timestamp){
                    frames_tail = frames_tail_iter;
                }
                frames_tail_iter = frames_tail_iter->next;
            }
            end_scope = clock();
            time_spent_scope = (double)(end_scope - begin_scope) *1000 / CLOCKS_PER_SEC;
            /** SCOPE END **/

            // FRAMES BUFFER
            if (close_pred(curr_tuple, C)) {
                C = close(curr_tuple, C);

                // multi-buffer management
                frames_tail->t_end = curr_tuple.timestamp;
            }
            if (update_pred(curr_tuple, C)) {
                C = update(curr_tuple, C, buffer_currentframe, frames_tail->size);

                // multi-buffer management
                frames_tail->t_end = curr_tuple.timestamp;
                frames_tail->size++;
                frames_tail->end = tuples_tail;
            }
            if (open_pred(curr_tuple, C)) {
                C = open(curr_tuple, C);
                buffer_currentframe = tuples_tail;
                frames_count++;

                // multi-buffer management
                frame* new_frame = (frame*) malloc(sizeof(frame));
                new_frame->t_start = curr_tuple.timestamp;
                new_frame->t_end = curr_tuple.timestamp;
                new_frame->size = 1;
                new_frame->count = frames_count;
                new_frame->start = tuples_tail;
                new_frame->end = tuples_tail;
                new_frame->next = NULL;

                if (frames_head == NULL) {
                    frames_head = new_frame;
                } else {
                    frames_tail->next = new_frame;
                }
                frames_tail = new_frame;
            }
            C_backup.start = C->start;
            C_backup.count = C->count;
            C_backup.v = C->v;
            time_spent_split = -1;
            time_spent_merge = -1;
        }

        /// Out-of-Order
        else {
            bool found = false;
            /** START SCOPE **/
            begin_scope = clock();
            frames_iter = frames_head;
            last_frame = NULL;
            while (frames_iter != NULL) {
                if (data.timestamp > frames_iter->t_start) {
                    if (frames_iter->next != NULL){
                        if (FRAME == 0){
                            if (data.timestamp <= frames_iter->t_end){
                                old_frames_head = frames_iter;
                                tuples_iter = frames_iter->start;
                            } else if (data.timestamp <= frames_iter->next->t_start && data.timestamp > frames_iter->t_end){
                                old_frames_head = frames_iter->next;
                                tuples_iter = frames_iter->end->next;

                                /** START ADD **/
                                begin_add = clock();
                                node *tmp = tuples_iter->next;
                                tuples_iter->next = (node *) malloc(sizeof(node));
                                tuples_iter->next->data = data;
                                tuples_iter->next->next = tmp;
                                tuples_count++;
                                found = true;
                                end_add = clock();
                                time_spent_add = (double) (end_add - begin_add) *1000 / CLOCKS_PER_SEC;
                                /** END ADD **/

                                old_frames_head->start = tuples_iter->next;
                                old_frames_head->t_start =  tuples_iter->next->data.timestamp;
                            }
                        } else if (data.timestamp <= frames_iter->next->t_start){
                            old_frames_head = frames_iter; // puntatore al frame di appartenenza nella lista originale
                            tuples_iter = frames_iter->start;
                        }
                    } else {
                        old_frames_head = frames_iter;
                        tuples_iter = frames_iter->start;
                    }
                }
                last_frame = frames_iter;
                frames_iter = frames_iter->next;
            }
            end_scope = clock();
            time_spent_scope = (double)(end_scope - begin_scope) *1000 / CLOCKS_PER_SEC;
            /** END SCOPE **/

            content = old_frames_head;

            // RECOMPUTE FRAMES

            C->count = 1;
            C->start = true;
            C->v = tuples_iter->data.A;

            long old_frames_head_t_end, old_frames_head_size;
            node* old_frames_head_end;
            // di old_frames_head modifichiamo la size, il t_end e l'end
            old_frames_head_t_end = old_frames_head->t_end;
            old_frames_head_end = old_frames_head->end;
            old_frames_head_size = old_frames_head->size;

            new_frames_tail = old_frames_head;
            new_frames_tail->size = 1;

            buffer_currentframe = tuples_iter; // testa del frame al quale la tupla dovrebbe appartenere

            if (tuples_iter->next != NULL) {
                /** START ADD **/
                begin_add = clock();
                if (tuples_iter->next->data.timestamp > data.timestamp && !found) { // found the out of order tuple, in-order insert into original frame
                    node *tmp = tuples_iter->next;
                    tuples_iter->next = (node *) malloc(sizeof(node));
                    tuples_iter->next->data = data;
                    tuples_iter->next->next = tmp;
                    tuples_count++;
                    found = true;
                    old_frames_head_size++;
                    if (tuples_iter->next->data.timestamp > old_frames_head_t_end){
                        old_frames_head_t_end = tuples_iter->next->data.timestamp;
                        old_frames_head_end = tuples_iter->next;
                    }
                }
                end_add = clock();
                time_spent_add = (double) (end_add - begin_add) *1000 / CLOCKS_PER_SEC;
                /** END ADD **/
                tuples_iter = tuples_iter->next;
            }

            bool recompute;
            bool bench_split = true;
            while (tuples_iter != NULL){
                if (bench_split) {
                    /** START SPLIT **/
                    begin_split = clock();
                    bench_split = false;
                }
                recompute = false;
                tuple curr_tuple = tuples_iter->data;

                if (close_pred(curr_tuple, C)) {
                    C = close(curr_tuple, C);

                    if (FRAME == 0) new_frames_tail->t_end = curr_tuple.timestamp;
                    if (new_frames_tail->t_end < old_frames_head_t_end) {
                        // split
                        old_frames_head->end = new_frames_tail->end;
                        old_frames_head->size = new_frames_tail->size;

                        frame* new_frame = (frame*) malloc(sizeof (frame));
                        if (FRAME!=0) new_frame->start = tuples_iter;
                        else new_frame->start = tuples_iter->next;
                        new_frame->end = old_frames_head_end; /// IF WE WANT TO FORCE THE COMPLEXITY WE CAN ITERATE UNTIL WE FIND THE POINTER
                        new_frame->next = old_frames_head->next;
                        new_frame->t_start = curr_tuple.timestamp;
                        new_frame->t_end = new_frame->end->data.timestamp;
                        new_frame->size = (old_frames_head_size - new_frames_tail->size);
                        frames_count++;

                        old_frames_head->next = new_frame;
                        old_frames_head = old_frames_head->next;

                        C->count = old_frames_head->size;
                        C->start = true;
                        C->v = old_frames_head->start->data.A;

                        new_frames_tail = old_frames_head;
                        end_split = clock();
                        bench_split = true;
                        time_spent_split += (double) (end_split - begin_split) *1000 / CLOCKS_PER_SEC;
                        /** END SPLIT **/

                        if (FRAME != 0) {
                            /** START MERGE **/
                            begin_merge = clock();
                            // merge
                            while (old_frames_head->end->next != NULL && old_frames_head->next != NULL &&
                                   old_frames_head->end->data.timestamp <= old_frames_head->next->end->data.timestamp) {
                                old_frames_head->end = old_frames_head->end->next;
                                old_frames_head->size++;
                            }
                            if (old_frames_head->next != NULL) {
                                frame* tmp = old_frames_head->next;
                                old_frames_head->next = old_frames_head->next->next;
                                free(tmp);
                            }
                            old_frames_head->t_end = old_frames_head->end->data.timestamp;
                            frames_count--;
                            end_merge = clock();
                            time_spent_merge += (double) (end_merge - begin_merge) *1000 / CLOCKS_PER_SEC;
                            /** END MERGE **/

                            old_frames_head_t_end = old_frames_head->t_end;
                            old_frames_head_end = old_frames_head->end;
                            old_frames_head_size = old_frames_head->size;

                            tuples_iter = old_frames_head->start;
                            new_frames_tail = old_frames_head;
                            buffer_currentframe = tuples_iter;

                            new_frames_tail->size=1;
                            new_frames_tail->end = tuples_iter;
                            new_frames_tail->t_end = tuples_iter->data.timestamp;

                            C->v = new_frames_tail->start->data.A;
                            recompute = true;

                        } else {
                            break;
                        }

                    } else {
                        if (old_frames_head->next != NULL) {
                            C->count = 1;
                            C->start = true;
                            C->v = tuples_iter->data.A;
                            new_frames_tail = frames_tail;
                        }
                        break;
                    }
                }
                if (!recompute) {
                    if (update_pred(curr_tuple, C)) {
                        C = update(curr_tuple, C, buffer_currentframe, new_frames_tail->size);
                        // multi-buffer management
                        new_frames_tail->t_end = curr_tuple.timestamp;
                        new_frames_tail->size++;
                        new_frames_tail->end = tuples_iter;
                    }

                    /** START ADD **/
                    begin_add = clock();
                    if (tuples_iter->next != NULL && tuples_iter->next->data.timestamp >= data.timestamp &&
                        !found) { // found the out of order tuple
                        node *tmp = tuples_iter->next;
                        tuples_iter->next = (node *) malloc(sizeof(node));
                        tuples_iter->next->data = data;
                        tuples_iter->next->next = tmp;
                        tuples_count++;
                        found = true;
                        old_frames_head_size++;
                        if (tuples_iter->next->data.timestamp > old_frames_head_t_end){
                            old_frames_head_t_end = tuples_iter->next->data.timestamp;
                            old_frames_head_end = tuples_iter->next;
                        }
                    }
                    end_add = clock();
                    time_spent_add = (double) (end_add - begin_add) *1000 / CLOCKS_PER_SEC;
                    /** END ADD **/
                }
                tuples_iter = tuples_iter->next;
            }

            frames_tail = new_frames_tail;

        }

        /** START CONTENT/EVICT **/
        if (content != NULL) { // check content/eviction policy (tick)
            if (DEBUG) printf("CONTENT/EVICT: [%ld, %ld] size %ld \n", content->t_start, content->t_end, content->size); // do content/evict
            content = NULL;
        }
        /** END CONTENT/EVICT **/

        // add,scope,split,merge,w,n,wn //fixme
        printf("%f,", time_spent_add);
        printf("%f,", time_spent_scope);
        printf("%f,", time_spent_split);
        printf("%f,", time_spent_merge);
        printf("%ld,%ld,",frames_count,tuples_count);
        printf("%ld\n", frames_count*tuples_count);
        time_spent_split = 0;
        time_spent_merge = 0;

    }
    fclose(file); // close input file stream

    if (DEBUG) {
        frame *frames_head_debug = frames_head;
        printf("FINAL CONTEXT\n");
        while (frames_head_debug != NULL) {
            printf("[%ld, %ld] size %ld -> ", frames_head_debug->t_start, frames_head_debug->t_end,
                   frames_head_debug->size);
            int counter = 0;
            node *frames_head_debug_iter = frames_head_debug->start;
            while (frames_head_debug_iter != NULL && counter < frames_head_debug->size) {
                printf("(ts: %ld, value : %f) ", frames_head_debug_iter->data.timestamp,
                       frames_head_debug_iter->data.A);
                frames_head_debug_iter = frames_head_debug_iter->next;
                counter++;
            }
            printf("\n");
            frames_head_debug = frames_head_debug->next;
        }
        printf("\n");
    }

    // free memory
    node* current = tuples_head;
    while (current != NULL) {
        node* next = current->next;
        free(current);
        current = next;
    }
    frame* currentf = frames_head;
    while (currentf != NULL) {
        frame* nextf = currentf->next;
        free(currentf);
        currentf = nextf;
    }
    free(C);


}
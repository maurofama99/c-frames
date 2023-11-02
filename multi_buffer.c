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
        printf("processed tuple: (ts: %ld, value: %f)\n", data.timestamp, data.A);

        /// In-Order
        if (data.timestamp >= last_timestamp){
            // if ooo but inside current opened frame, just order the tuples buffer, no recompute
            // TUPLES BUFFER
            enqueue(&tuples_tail, data);
            if (tuples_count == 0) tuples_head = tuples_tail;
            tuples_count++;
            last_timestamp = data.timestamp;

            // todo update frames_tail
            // FRAMES BUFFER
            tuple curr_tuple = data;
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

                // multi-buffer management
                frame* new_frame = (frame*) malloc(sizeof(frame));
                new_frame->t_start = curr_tuple.timestamp;
                new_frame->t_end = curr_tuple.timestamp;
                new_frame->size = 1;
                new_frame->count = frames_count + 1;
                new_frame->start = tuples_tail;
                new_frame->end = tuples_tail;
                new_frame->next = NULL;

                if (frames_head == NULL) {
                    frames_head = new_frame;
                } else {
                    frames_tail->next = new_frame;
                }
                frames_tail = new_frame;

                frames_count++;
            }
            C_backup.start = C->start;
            C_backup.count = C->count;
            C_backup.v = C->v;
        }

        /// Out-of-Order
        else {
            bool found = false;
            // scope: find the correct window
            frames_iter = frames_head;
            last_frame = NULL;
            while (frames_iter != NULL) {
                if (data.timestamp > frames_iter->t_start) {
                    if (frames_iter->next != NULL){
                        if (FRAME == 0){
                            if (data.timestamp <= frames_iter->t_end){
                                old_frames_head = frames_iter;
                                tuples_iter = frames_iter->start;
                                frames_count = frames_iter->count;
                            } else if (data.timestamp <= frames_iter->next->t_start && data.timestamp > frames_iter->t_end){
                                old_frames_head = frames_iter->next;
                                frames_count = frames_iter->next->count;
                                tuples_iter = frames_iter->end->next;

                                node *tmp = tuples_iter->next;
                                tuples_iter->next = (node *) malloc(sizeof(node));
                                tuples_iter->next->data = data;
                                tuples_iter->next->next = tmp;
                                tuples_count++;
                                found = true;

                                old_frames_head->start = tuples_iter->next;
                                old_frames_head->t_start =  tuples_iter->next->data.timestamp;
                            }
                        } else if (data.timestamp <= frames_iter->next->t_start){
                            old_frames_head = frames_iter; // puntatore al frame di appartenenza nella lista originale
                            tuples_iter = frames_iter->start;
                            frames_count = frames_iter->count;
                        }
                    } else {
                        old_frames_head = frames_iter;
                        tuples_iter = frames_iter->start;
                        frames_count = frames_iter->count;
                    }
                }
                last_frame = frames_iter;
                frames_iter = frames_iter->next;
            }

            printf("scope: [%ld, %ld], tuples iter: (ts: %ld, value: %f)\n", old_frames_head->t_start, old_frames_head->t_end, tuples_iter->data.timestamp, tuples_iter->data.A);



            // RECOMPUTE FRAMES

            C->count = 0;
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
                if (tuples_iter->next->data.timestamp >= data.timestamp && !found) { // found the out of order tuple
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
                tuples_iter = tuples_iter->next;
            }

            bool recompute;
            long merged_frame_size;
            while (tuples_iter != NULL){
                recompute = false;
                tuple curr_tuple = tuples_iter->data;

                if (close_pred(curr_tuple, C)) {
                    C = close(curr_tuple, C);

                    if (FRAME==0) new_frames_tail->t_end = curr_tuple.timestamp;
                    if (new_frames_tail->t_end < old_frames_head_t_end) {
                        // split
                        old_frames_head->end = new_frames_tail->end;
                        printf("pre split, sta m di size e` %ld\n", new_frames_tail->size);
                        old_frames_head->size = new_frames_tail->size;

                        frame* new_frame = (frame*) malloc(sizeof (frame));
                        if (FRAME!=0) new_frame->start = tuples_iter;
                        else new_frame->start = tuples_iter->next; // skip the tuple that ends the frame
                        new_frame->end = old_frames_head_end;
                        new_frame->next = old_frames_head->next;
                        if (FRAME!=0) new_frame->t_start = curr_tuple.timestamp;
                        else new_frame->t_start = new_frame->start->data.timestamp;  // skip the tuple that ends the frame
                        new_frame->t_end = new_frame->end->data.timestamp;
                        new_frame->size = (old_frames_head_size - new_frames_tail->size);

                        old_frames_head->next = new_frame;
                        old_frames_head = old_frames_head->next;

                        C->count = old_frames_head->size;
                        C->start = true;
                        C->v = old_frames_head->start->data.A;

                        if (DEBUG) {
                            frame *frames_head_debug = frames_head;
                            printf("AFTER SPLIT CONTEXT\n");
                            while (frames_head_debug != NULL) {
                                printf("[%ld, %ld] size %ld, end ts: %ld -> ", frames_head_debug->t_start, frames_head_debug->t_end,
                                       frames_head_debug->size, frames_head_debug->end->data.timestamp);
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

                        new_frames_tail = old_frames_head;

                        if (FRAME != 0) {
                            // merge
                            while (old_frames_head->end->next != NULL &&
                                   old_frames_head->end->data.timestamp <= old_frames_head->next->end->data.timestamp) {
                                old_frames_head->end = old_frames_head->end->next;
                                old_frames_head->size++;
                            }
                            if (old_frames_head->next != NULL) {
                                old_frames_head->next = old_frames_head->next->next;
                            }
                            old_frames_head->t_end = old_frames_head->end->data.timestamp;

                            old_frames_head_t_end = old_frames_head->t_end;
                            old_frames_head_end = old_frames_head->end;
                            old_frames_head_size = old_frames_head->size;

                            tuples_iter = old_frames_head->start;
                            new_frames_tail = old_frames_head;
                            buffer_currentframe = tuples_iter;

                            if (DEBUG) {
                                frame *frames_head_debug = frames_head;
                                printf("AFTER MERGE CONTEXT\n");
                                while (frames_head_debug != NULL) {
                                    printf("[%ld, %ld] size %ld, start: %ld, end: %ld -> ", frames_head_debug->t_start,
                                           frames_head_debug->t_end,
                                           frames_head_debug->size, frames_head_debug->start->data.timestamp,
                                           frames_head_debug->end->data.timestamp);
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

                            new_frames_tail->size=1;
                            new_frames_tail->end = tuples_iter;
                            new_frames_tail->t_end = tuples_iter->data.timestamp;
                            printf("after merge, sta m di size e` %ld\n", new_frames_tail->size);

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
                            old_frames_head = old_frames_head->next;
                            new_frames_tail = old_frames_head;
                        }
                        break;
                    }
                }
                if (!recompute) {
                    if (update_pred(curr_tuple, C)) {
                        C = update(curr_tuple, C, buffer_currentframe, new_frames_tail->size);
                        printf("impazzisco\n");
                        // multi-buffer management
                        new_frames_tail->t_end = curr_tuple.timestamp;
                        new_frames_tail->size++;
                        new_frames_tail->end = tuples_iter;
                    }

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
                }
                tuples_iter = tuples_iter->next;
            }

            frames_tail = new_frames_tail;

        }

        if (DEBUG) {
            frame *frames_head_debug = frames_head;
            printf("CONTEXT AFTER INSERT\n");
            while (frames_head_debug != NULL) {
                printf("[%ld, %ld] size %ld, end ts: %ld -> ", frames_head_debug->t_start, frames_head_debug->t_end,
                       frames_head_debug->size, frames_head_debug->end->data.timestamp);
                int counter = 0;
                node *frames_head_debug_iter = frames_head_debug->start;
                while (counter < frames_head_debug->size) {
                    if (frames_head_debug_iter == NULL) printf(" (null) ");
                    else {
                        printf("(ts: %ld, value : %f) ", frames_head_debug_iter->data.timestamp, frames_head_debug_iter->data.A);
                        frames_head_debug_iter = frames_head_debug_iter->next;
                    }
                    counter++;
                }
                printf("\n");
                frames_head_debug = frames_head_debug->next;
            }
            printf("\n");
        }

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
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
    long last_timestamp = 0;

    frame* frames_head = NULL;
    frame* frames_tail;
    frame* frames_iter;
    frame *last_frame;
    frame* new_frames_head;
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

        /// In-Order
        if (data.timestamp >= last_timestamp){
            // if ooo but inside current opened frame, just order the tuples buffer, no recompute
            // TUPLES BUFFER
            enqueue(&tuples_tail, data);
            if (tuples_count == 0) tuples_head = tuples_tail;
            tuples_count++;
            last_timestamp = data.timestamp;

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
        }

        /// Out-of-Order
        else {

            frames_iter = frames_head;
            last_frame = NULL;
            while (frames_iter != NULL){
                if (data.timestamp > frames_iter->t_start) {
                    if (frames_iter->next != NULL){
                        if (data.timestamp <= frames_iter->next->t_start){
                            old_frames_head = last_frame;
                            tuples_iter = frames_iter->start;
                            frames_count = frames_iter->count;
                        }
                    } else {
                        old_frames_head = last_frame;
                        tuples_iter = frames_iter->start;
                        frames_count = frames_iter->count;
                    }
                }
                last_frame = frames_iter;
                frames_iter = frames_iter->next;
            }


            // RECOMPUTE FRAMES

            C->count = 1;
            C->start = true;
            C->v = tuples_iter->data.A;
            new_frames_head = (frame*) malloc(sizeof (frame));
            new_frames_head->count = frames_count;
            new_frames_head->t_end = tuples_iter->data.timestamp;
            new_frames_head->t_start = tuples_iter->data.timestamp;
            new_frames_head->size = 1;
            new_frames_head->start = tuples_iter;
            new_frames_head->end = tuples_iter;
            new_frames_head->next = NULL;
            new_frames_tail = new_frames_head;
            buffer_currentframe = tuples_iter;

            bool found = false;
            if (tuples_iter->next->data.timestamp >= data.timestamp) { // found the out of order tuple
                node* tmp = tuples_iter->next;
                tuples_iter->next = (node*)malloc(sizeof(node));
                tuples_iter->next->data = data;
                tuples_iter->next->next = tmp;
                tuples_count++;
                found = true;
            }
            tuples_iter = tuples_iter->next;

            while (tuples_iter != NULL){

                tuples_tail = tuples_iter;
                tuple curr_tuple = tuples_iter->data;

                if (close_pred(curr_tuple, C)) {
                    C = close(curr_tuple, C);

                    // multi-buffer management
                    new_frames_tail->t_end = curr_tuple.timestamp;
                }
                if (update_pred(curr_tuple, C)) {
                    C = update(curr_tuple, C, buffer_currentframe, new_frames_tail->size);

                    // multi-buffer management
                    new_frames_tail->t_end = curr_tuple.timestamp;
                    new_frames_tail->size++;
                    new_frames_tail->end = tuples_tail;
                }
                if (open_pred(curr_tuple, C)) {
                    /// start split & merge X
                    C = open(curr_tuple, C);
                    buffer_currentframe = tuples_tail;

                    // multi-buffer management
                    frame* new_frame = (frame*) malloc(sizeof(frame));
                    new_frame->t_start = curr_tuple.timestamp;
                    new_frame->t_end = curr_tuple.timestamp;
                    new_frame->size = 1;
                    new_frame->count = frames_count + 1;
                    new_frame->start = buffer_currentframe;
                    new_frame->end = buffer_currentframe;
                    new_frame->next = NULL;

                    new_frames_tail->next = new_frame;
                    new_frames_tail = new_frame;

                    frames_count++;
                }

                if (tuples_iter->next != NULL && tuples_iter->next->data.timestamp >= data.timestamp && !found){ // found the out of order tuple
                    node* tmp = tuples_iter->next;
                    tuples_iter->next = (node*)malloc(sizeof(node));
                    tuples_iter->next->data = data;
                    tuples_iter->next->next = tmp;
                    tuples_count++;
                    found = true;
                }
                tuples_iter = tuples_iter->next;
            }

            if (old_frames_head == NULL) frames_head = new_frames_head;
            else old_frames_head->next = new_frames_head;
            frames_tail = new_frames_tail;
        }

    }


    if (DEBUG) {
        while (frames_head != NULL) {
            printf("[%ld, %ld] size %ld -> ", frames_head->t_start, frames_head->t_end, frames_head->size);
            int counter = 0;
            while (frames_head->start != NULL && counter < frames_head->size) {
                printf("(ts: %ld, value : %f) ", frames_head->start->data.timestamp,
                       frames_head->start->data.A);
                frames_head->start = frames_head->start->next;
                counter++;
            }
            printf("\n");
            frames_head = frames_head->next;
        }
    }

}
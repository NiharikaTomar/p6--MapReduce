#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <sys/stat.h>
#include <semaphore.h>
#include "mapreduce.h"

// Data Structure: Linked List of Linked Lists

struct pair {
    char *key;
    char *value;
    struct pair *next;
};

struct reduce_stuff {
    char *key;
    Getter get_func;
    int partition_number;
};

struct partition_stuff {
    struct pair *head;
};

struct partition_stuff *p;

// Gets the next value that will be used in user Reduce
char* get_next(char *key, int partition_number){
    struct pair *current_pair;
    current_pair = p[partition_number].head;

    while (current_pair != NULL) {
        // Return the value with the key found
        if (current_pair->key == key) {
            if (current_pair->next != NULL) {
                return current_pair->next->value;
            }
        }
        current_pair = current_pair->next;
    }
    return NULL;
}

void MR_Emit(char *key, char *value){
    
}


// void reducer_run(struct reducer_arg) {
//     reducer_arg.reduce(reducer_arg.key, )
// }


void MR_Run(int argc, char *argv[], 
        Mapper map, int num_mappers, 
        Reducer reduce, int num_reducers, 
        Partitioner partition, int num_partitions) {

    // Compare number of files with number of mappers
    if (argc - 1 < num_mappers) {
        num_mappers = argc - 1;
    }
    
    pthread_t mapper_thread[num_mappers];
    pthread_t reducer_thread[num_reducers];

    // To create mapper threads
    for (int i = 0; i < num_mappers; i++) {
        pthread_create(&mapper_thread[i], NULL, map, argv[i]);
    }
    // To join mapper threads
    for (int i = 0; i < num_mappers; i++) {
        pthread_join(&mapper_thread[i], NULL);
    }

    // struct reduce_stuff;

    // To create reducer threads
    for (int i = 0; i < num_reducers; i++) {
        pthread_create(&reducer_thread[i], NULL, reduce, NULL);
        // pthread_create(&reducer_thread[i], NULL, reducer_run, reducer_args);
    }
    // To join reducer threads
    for (int i = 0; i < num_reducers; i++) {
        pthread_join(&reducer_thread[i], NULL);
    }
}

unsigned long MR_SortedPartition(char *key, int num_partitions) {
    return 0;
}

unsigned long MR_DefaultHashPartition(char *key, int num_partitions) {
    unsigned long hash = 5381;
    int c;
    while ((c = *key++) != '\0')
        hash = hash * 33 + c;
    return hash % num_partitions;
}
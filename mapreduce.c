#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <sys/stat.h>
#include <semaphore.h>
#include "mapreduce.h"

struct pair {
    char *key;
    char *value;
    struct pair *next;
    int proc_thread = 0; // to keep track if processed
};

struct reduce_stuff {
    char *key;
    Getter get_func;
    int partition_number;
};

struct partition_stuff {
    struct pair *head;
};

Partioner p;
int NUM_PART;

partition_stuff *partition_array;


// Gets the next value that will be used in user Reduce
char* get_next(char *key, int partition_number){
    struct pair *current_pair;
    current_pair = p[partition_number].head;

    while (current_pair != NULL) {
        if (current_pair->proc_thread == 0) {
            // Return the value with the key found
            if (current_pair->key == key) {
                if (current_pair->next != NULL) {                
                    // set it to processed
                    current_pair->proc_thread = 1;
                    return current_pair->next->value;
                }
            }
        }
        current_pair = current_pair->next;
    }
    return NULL;
}

void MR_Emit(char *key, char *value){
    
}


// void reducer_run(*partition) {
//
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
    p = partition;
    NUM_PART = num_partitions;
    
    partition_array = malloc(num_partitions * sizeof(partition_stuff));

    for (int i = 0; i < num_partitions; i++) {
        partition_array[i].head = NULL;
    }

    // To create mapper threads
    for (int i = 0; i < num_mappers; i++) {
        pthread_create(&mapper_thread[i], NULL, map, argv[i]);
    }
    // To join mapper threads
    for (int i = 0; i < num_mappers; i++) {
        pthread_join(&mapper_thread[i], NULL);
    }

    // MERGE SORT or qsort or any sort with nlogn complexity

    // struct reduce_info;

    // To create reducer threads
    for (int i = 0; i < num_reducers; i++) {
        pthread_create(&reducer_thread[i], NULL, reduce, NULL);
        // pthread_create(&reducer_thread[i], NULL, reducer_run, reduce_info);
    }
    // To join reducer threads
    for (int i = 0; i < num_reducers; i++) {
        pthread_join(&reducer_thread[i], NULL);
    }

    // freeee me !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
}

unsigned long MR_SortedPartition(char *key, int num_partitions) {
    char sort[4];
    char *pointer;

    strncpy(sort, key, 4);

    // Change strtoul !!!!!!!!!!!!!!!!!
    return strtoul(sort, &pointer, 36);
}

unsigned long MR_DefaultHashPartition(char *key, int num_partitions) {
    unsigned long hash = 5381;
    int c;
    while ((c = *key++) != '\0')
        hash = hash * 33 + c;
    return hash % num_partitions;
}
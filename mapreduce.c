#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <sys/stat.h>
#include <semaphore.h>
#include "mapreduce.h"

// Locks
pthread_mutex_t f_lock;
pthread_mutex_t lock;

struct map_keyvals {
    char *key;
    char *value;
};

struct files {
    char *file_name;
};

struct partition {
    int processed_times; // number of times mapped_keyvals accessed(processed) in partition
    int *mapped_p; // mapped_keyvals allocated in partition
    int *mapped_keyvals_amount; // count mapped_keyvals in partition
} partition;

struct files *file_array;
struct map_keyvals **partition_array;

int NUM_PART;
int total_files;
int amt_files;
int incrementer;

Partitioner part;
Mapper mapp;
Reducer red;

// Gets the next value that will be used in user Reduce
char* get_next(char *key, int partition_number){
    if (incrementer >= partition.processed_times) {
        // Compare the key in our hash table and the given key
        if (strcmp(partition_array[partition_number][partition.processed_times].key, key) == 0) {
            // Increment the processed times
            partition.processed_times = partition.processed_times + 1;
            // Return the value
            return partition_array[partition_number][partition.processed_times].value;
        }
    }
    return NULL;
}

void MR_Emit(char *key, char *value){
    pthread_mutex_lock(&lock);

    // Partition number based on user's choice: MR_SortedPartition, 
    // MR_DefaultHashPartition, or anything else user provides.
    unsigned long p_num = part(key, NUM_PART);

    partition.mapped_keyvals_amount[p_num] = partition.mapped_keyvals_amount[p_num] + 1;
    incrementer++;

    // Check if reallocation is needed
    if (partition.mapped_p[p_num] < partition.mapped_keyvals_amount[p_num]) {
        partition.mapped_p[p_num] = partition.mapped_p[p_num] * 2; // Double the size of the hash table
        partition_array[p_num] = realloc(partition_array[p_num], partition.mapped_p[p_num] * sizeof(struct map_keyvals)); // Reallocating the hash table size
    }

    // Allocating space in the hash table for key value pair
    partition_array[p_num][partition.mapped_keyvals_amount[p_num]].key = (char *) malloc(strlen(key) * 1);
    partition_array[p_num][partition.mapped_keyvals_amount[p_num]].value = (char *) malloc(strlen(value) * 1);

    // Putting key and value pair in the hash table
    strcpy(partition_array[p_num][partition.mapped_keyvals_amount[p_num]].key, key);
    strcpy(partition_array[p_num][partition.mapped_keyvals_amount[p_num]].value, value);

    pthread_mutex_unlock(&lock);
}

// Wrapper for Mapper
void* mapper_run() {
    for (int i = amt_files; i < total_files; i++) {
        pthread_mutex_lock(&f_lock);

        char *cf = file_array[amt_files].file_name;

        amt_files = amt_files + 1;

        pthread_mutex_unlock(&f_lock);
        // Call mapper on current file
        mapp(cf);
    }
}

// Wrapper for Reducer
void* reducer_run(void *start) {
    int i = 0;
    while (i < partition.mapped_keyvals_amount[*(int *) start]) {
        // if (partition.processed_times[*(int *) start] == i) {
            // Call reducer on the partitions
            red(partition_array[*(int *) start][i].key, get_next, *(int *)  start);
        // }
        i++;
    }
    return start;
}

// Sort keys in alphabetical order
int compare_qsort(const void *key_one, const void *key_two) {
    // Get the values at given addresses 
    struct map_keyvals *k_one = (struct map_keyvals *) key_one;
    struct map_keyvals *k_two = (struct map_keyvals *) key_two;

    // Compare keys for key_one and key_two
    int compare_key = strcmp(k_one->key, k_two->key);

    if (compare_key != 0) {
        return compare_key;
    } else {
        int compare_value = strcmp(k_one->value, k_two->value);
        return compare_value;
    }
}

void MR_Run(int argc, char *argv[], 
        Mapper map, int num_mappers, 
        Reducer reduce, int num_reducers, 
        Partitioner partition, int num_partitions) {

    // Compare number of files with number of mappers
    if (argc - 1 < num_mappers) {
        num_mappers = argc - 1;
    }
    struct partition p;
    p.mapped_keyvals_amount = malloc(num_partitions * sizeof(int));
    p.mapped_p = malloc(num_partitions * sizeof(int));

    partition_array = malloc(num_partitions * sizeof(struct map_keyvals));
    file_array = malloc((argc-1) * sizeof (struct files));

    int reducer_array[num_reducers];
    
    part = partition;
    mapp = map;
    red = reduce;
    NUM_PART = num_partitions;
    total_files = argc - 1;
    amt_files = 0;
    p.processed_times = 0;

    for (int i = 0; i < num_reducers; i++) {
        reducer_array[i] = i;
        partition_array[i] = malloc(sizeof(struct map_keyvals) * 512);
        p.mapped_keyvals_amount[i] = 0;
        p.mapped_p[i] = 512;
    }   

    pthread_t mapper_thread[num_mappers];
    pthread_t reducer_thread[num_reducers];
    
    // To create mapper threads
    for (int i = 0; i < num_mappers; i++) {
        pthread_create(&mapper_thread[i], NULL, mapper_run, NULL);
    }
    // To join mapper threads
    for (int i = 0; i < num_mappers; i++) {
        pthread_join(mapper_thread[i], NULL);
    }

    // MERGE SORT or qsort or any sort with nlogn complexity
    for (int i = 0; i < num_partitions; i++) {
        qsort(partition_array[i], p.mapped_keyvals_amount[i], sizeof(struct map_keyvals), compare_qsort);
    }

    // To create reducer threads
    for (int i = 0; i < num_reducers; i++) {
        pthread_create(&reducer_thread[i], NULL, reducer_run, &reducer_array[i]);
    }
    // To join reducer threads
    for (int i = 0; i < num_reducers; i++) {
        pthread_join(reducer_thread[i], NULL);
    }

    // Free dinamically allocated memory
    free(partition_array);
    free(file_array);
}

unsigned long MR_SortedPartition(char *key, int num_partitions) {

    if (num_partitions == 1) {
        return 0;
    }

    char sort[4];
    char *pointer;

    strncpy(sort, key, 4);

    

    return strtoul(sort, &pointer, 36);
}

unsigned long MR_DefaultHashPartition(char *key, int num_partitions) {
    unsigned long hash = 5381;
    int c;
    while ((c = *key++) != '\0')
        hash = hash * 33 + c;
    return hash % num_partitions;
}
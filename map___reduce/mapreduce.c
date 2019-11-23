// Copyright 2019 Elizaveta Stepanova and Niharika Tomar
// Site soures we used:
// 1.pthread_key_t, pthread_setspecific, pthread_getspecific, pthread_key_create
// https://www.ibm.com/support/knowledgecenter/en/SSLTBW_2.1.0/com.ibm.zos.v2r1
// .bpxbd00/ptkycre.htm
#include <pthread.h>
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include "mapreduce.h"

pthread_mutex_t f_lock;
pthread_mutex_t lock;
// Specified the link on top of the file (right after Copyright) with
// explanation of usage (found online).
pthread_key_t thread_key;

struct k_v_pair {
    char *key;
    char *value;
    struct k_v_pair *next;
};

typedef struct partition_info {
    struct k_v_pair *head;
} partition_info;
struct partition_info *partitions;

struct files {
    char *file_name;
};
struct files *file_array;

int total_files;
int check_next;
int p_num;
int files_processed;

Partitioner p;
Reducer r;
Mapper m;

// Gets the next value that will be used in user Reduce
char* get_next(char *key, int partition_number) {
    pthread_mutex_lock(&lock);
    // Initialize key value pair to the head of the partition
    struct k_v_pair *pair = partitions[partition_number].head;
    // Check if it is null
    if (pair != NULL) {
        if (strcmp(pair->key, key) == 0) {
            partitions[partition_number].head = pair->next;
            pthread_mutex_unlock(&lock);
            return pair->value;
        }
    }
    pthread_mutex_unlock(&lock);
    return NULL;
}

// Takes key value pairs from the many different mappers and stores them in a
// partition such that later reducers can access them
void MR_Emit(char *key, char *value) {
    // Initialize k_v_pair struct
    struct k_v_pair *pair_to_create = malloc(sizeof(struct k_v_pair));
    // Malloc space for key of the new pair
    pair_to_create->key = malloc((strlen(key) + 1) * sizeof(char));
    // pair_to_create->value = malloc((strlen(value)) * sizeof(char));
    strcpy(pair_to_create->key, key);
    // strcpy(pair_to_create->value, value);
    // pair_to_create->key = key;
    pair_to_create->value = value;
    pair_to_create->next = NULL;

    // Start sorting hash table to place key value pairs in the correct spot
    struct k_v_pair *pair = partitions[p(key, p_num)].head;

    // If the hash table is empty, add pair as head
    if (pair == NULL) {
        partitions[p(key, p_num)].head = pair_to_create;
        return;
    } else {  // Sort
        pthread_mutex_lock(&lock);
        struct k_v_pair *prev = NULL;
        while (pair != NULL) {
            if (strcmp(pair->key, key) > 0) {
                    pair_to_create->next = pair;
                if (prev != NULL) {
                    prev->next = pair_to_create;
                } else {  // first iteration of the loop
                    partitions[p(key, p_num)].head = pair_to_create;
                }
                pthread_mutex_unlock(&lock);
                return;
            }
            // set prev to current pair
            prev = pair;
            // set current pair to next pair
            pair = pair->next;
        }
        prev->next = pair_to_create;
        pthread_mutex_unlock(&lock);
    }
}

// Wrapper for Mapper
void* mapper_run() {
    for (;;) {
        // Check if need to process more files
        if (total_files > files_processed) {
            pthread_mutex_lock(&f_lock);
            // Get the name of the file to be processed
            char *cf = file_array[files_processed].file_name;
            // Increment global variable of files processed
            files_processed++;
            pthread_mutex_unlock(&f_lock);
            // map the file
            m(cf);
        } else {
            return NULL;
        }
    }
}

// Wrapper for Reducer
void* reducer_run() {
    for (;;) {
        pthread_mutex_lock(&f_lock);

        if (p_num > check_next) {
            struct k_v_pair *pair = partitions[check_next].head;
            int *pointer = malloc(sizeof(int));
            *pointer = check_next;
            // Specified the link on top of the file (right after Copyright)
            // with explanation of usage (found online).
            pthread_setspecific(thread_key, (void *)pointer);
            check_next++;
            pthread_mutex_unlock(&f_lock);

            // Specified the link on top of the file (right after Copyright)
            // with explanation of usage (found online).
            while (pair != NULL) {
                r(pair->key, get_next, *(int *)pthread_getspecific(thread_key));
                pair = partitions[*(int *)pthread_getspecific(thread_key)].head;
            }
            // Free dynamically allocated memory
            free(pointer);
        } else {
          pthread_mutex_unlock(&f_lock);
          return NULL;
        }
    }
}

// This function is passed that argv array and assumes that argv[1]...argv[n-1]
// all contain file names that will be passed to the mappers.
void MR_Run(int argc, char *argv[],
        Mapper map, int num_mappers,
        Reducer reduce, int num_reducers,
        Partitioner partition, int num_partitions) {
    // Created only for cpplint camelCase error
    const int numMappers = num_mappers;
    const int numReducers = num_reducers;

    pthread_t mapper_thread[numMappers];
    pthread_t reducer_thread[numReducers];

    // Initialize global Partitioner, Mapper, and Reducer
    p = partition;
    m = map;
    r = reduce;

    // Initialize global variables
    p_num = num_partitions;
    total_files = argc - 1;

    // Malloc space for partitions
    partitions = malloc(num_partitions * sizeof(struct partition_info));

    // Malloc files_array and file names
    file_array = malloc(total_files * sizeof (struct files));
    for (int i = 0; i < total_files; i++) {
        file_array[i].file_name = malloc(strlen(argv[i+1] + 1) * sizeof(char));
        strcpy(file_array[i].file_name, argv[i+1]);
    }

    // THREADING

    // Mapper thread creation
    for (int i = 0; i < num_mappers; i++) {
        pthread_create(&mapper_thread[i], NULL, mapper_run, NULL);
    }

    // Mapper thread joining
    for (int i = 0; i < num_mappers; i++) {
        pthread_join(mapper_thread[i], NULL);
    }

    // Specified the link on top of the file (right after Copyright) with
    // explanation of usage (found online).
    pthread_key_create(&thread_key, NULL);

    // Reducer thread creation
    for (int i = 0; i < num_reducers; i++) {
        pthread_create(&reducer_thread[i], NULL, reducer_run, NULL);
    }

    // Reducer thread joining
    for (int i = 0; i < num_reducers; i++) {
        pthread_join(reducer_thread[i], NULL);
    }

    // Free dynamically allocated memory
    for (int i = 0; i < num_partitions; i++) {
        struct k_v_pair *pair = partitions[i].head;
        while (pair != NULL) {
          pair->next = NULL;
          pair->key = NULL;
          pair->value = NULL;
        }
        free(pair);
    }
    free(partitions);
}

// Ensures that keys are in a sorted order across the partitions. That is, keys
// are not hashed into random partions as in the MR_DefaultHashPartition().
unsigned long MR_SortedPartition(char *key, int num_partitions) {
    if (num_partitions == 1) {
        return 0;
    }
    // VERSION 1 -----------------------------------------------
    // char sort[4];
    // char *pointer;
    // strncpy(sort, key, 4);
    // return strtoul(sort, &pointer, 36);
    // VERSION 2 -----------------------------------------------
    // sorted = sorted & 0x0FFFFFFFF;// masking
    // int num_bits_needed;
    // num_bits_needed = log2(num_partitions);
    // int bits = 32 - num_bits_needed;
    // sorted = sorted >> (bits); // shifting msb
    // return sorted;
    // WORKING VERSION ----------------------------------------


    // Got help during discussion
    // Recommended the use of atoi() instead of stroul() and math.h
    long sorted = atoi(key);

    int index = 0;
    for (int i = num_partitions; i >= 2; i/=2) {
        index++;
    }
    int bits_needed = 32 - index;
    return (unsigned)sorted >> bits_needed;
}

unsigned long MR_DefaultHashPartition(char *key, int num_partitions) {
    unsigned long hash = 5381;
    int c;
    while ((c = *key++) != '\0')
        hash = hash * 33 + c;
    return hash % num_partitions;
}

#include <pthread.h>
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include "mapreduce.h"

pthread_mutex_t f_lock;
pthread_mutex_t lock;
pthread_key_t thread_key;

typedef struct partition_info {
    struct k_v_pair *head;
} partition_info;

struct k_v_pair {
    char *key;
    char *value;
    struct k_v_pair *next;
};

struct partition_info *partitions;

// struct files {
//     char *file_name;
// };
// struct files *file_array;

char** file_array;

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
    struct k_v_pair *pair = partitions[partition_number].head;
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


void MR_Emit(char *key, char *value) {
    // Initialize k_v_pair struct
    struct k_v_pair *pair_to_create = malloc(sizeof(struct k_v_pair));
    // Malloc space for key and value of the new pair
    pair_to_create->key = malloc((strlen(key) + 1) * sizeof(char));
    // pair_to_create->value = malloc((strlen(value) + 1) * sizeof(char));
    strcpy(pair_to_create->key, key);
    // strcpy(pair_to_create->value, value);
    // pair_to_create->key = key;
    pair_to_create->value = value;
    pair_to_create->next = NULL;

    // Start sorting hash table to place key value pairs in correct spot
    struct k_v_pair *pair =  partitions[p(key, p_num)].head;

    // If the hash table is empty, add pair as head
    if (pair == NULL){
        partitions[p(key, p_num)].head = pair_to_create;
        return;
    } else {
        pthread_mutex_lock(&lock);
        MR_SortedPartition(key, p_num);
        struct k_v_pair *prev = NULL;
        while(pair != NULL) {
            if(strcmp(pair->key, key) > 0) {
                    pair_to_create->next = pair;
                if (prev != NULL){
                    prev->next = pair_to_create;
                    // pair_to_create->next = pair;
                    // pthread_mutex_unlock(&lock);
                    // return;
                } else {
                    partitions[p(key, p_num)].head = pair_to_create;
                    
                }
                pthread_mutex_unlock(&lock);
                    return;
            }
            prev = pair;
            pair = pair->next;
        }
        prev->next = pair_to_create;
        // pair_to_create->next = NULL;
        pthread_mutex_unlock(&lock);
    }
}

// Wrapper for Mapper
void* mapper_run() {
    for (;;) {
        if (total_files > files_processed) {
            // char* cf;
            pthread_mutex_lock(&f_lock);
            char *cf = file_array[files_processed];
            // char *cf = file_array[files_processed].file_name;
            files_processed++;
            pthread_mutex_unlock(&f_lock);
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
          struct k_v_pair *iterator =  partitions[check_next].head;
          int *p = malloc(sizeof(int));
          *p = check_next;
          pthread_setspecific(thread_key, p);
          // void *pointer = malloc(sizeof(long));
          // pointer = &check_next;
          // pthread_setspecific(thread_key, pointer);
          check_next++;
          pthread_mutex_unlock(&f_lock);

          int* glob_spec_var = pthread_getspecific(thread_key);
          while(iterator != NULL) {
              r(iterator->key, get_next, *glob_spec_var);
              iterator = partitions[*glob_spec_var].head;
          }
        } else {
          pthread_mutex_unlock(&f_lock);
          return NULL;
        }
    }
}

void MR_Run(int argc, char *argv[],
        Mapper map, int num_mappers,
        Reducer reduce, int num_reducers,
        Partitioner partition, int num_partitions) {

    pthread_t mapper_thread[num_mappers];
    pthread_t reducer_thread[num_reducers];

    p = partition;
    m = map;
    r = reduce;

    p_num = num_partitions;
    total_files = argc - 1;
    partitions = malloc(num_partitions * sizeof(struct partition_info));

    file_array = &argv[1];
    // file_array = malloc(total_files * sizeof (struct files));
    // for (int i = 0; i < total_files; i++) {
    //     file_array[i].file_name = malloc(strlen(argv[i+1] + 1) * sizeof(char));
    //     strcpy(file_array[i].file_name, argv[i+1]);
    // }

    for (int i = 0; i < num_mappers; i++) {
        pthread_create(&mapper_thread[i], NULL, mapper_run, NULL);
    }

    for (int i = 0; i < num_mappers; i++) {
        pthread_join(mapper_thread[i], NULL);
    }

    pthread_key_create(&thread_key,NULL);

    for (int i = 0; i < num_reducers; i++) {
        pthread_create(&reducer_thread[i], NULL, reducer_run, NULL);
    }

    for (int i = 0; i < num_reducers; i++) {
        pthread_join(reducer_thread[i], NULL);
    }
}

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
    long sorted = atoi(key);

    int index = 0;
    for(int i = num_partitions; i >= 2; i/=2){
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

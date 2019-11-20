#include <pthread.h>
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include "mapreduce.h"

pthread_mutex_t f_lock;
pthread_mutex_t lock;

typedef struct partition_info {
    struct k_v_pair *head;
} partition_info;

struct k_v_pair {
    char *key;
    char *value;
    struct k_v_pair *next;
};

Partitioner p;
Reducer r;
Mapper m;

int *isNextKeyDifferent;
int NUM_FILES;
int NEXT_PARTITION;
int NUM_PARTITIONS;
int CURR_FILE;

char** FILES;
struct partition_info *partitions;

pthread_key_t glob_var_key; // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

// Gets the next value that will be used in user Reduce
char* get_next(char *key, int partition_number) {

    struct k_v_pair *curr_partition = partitions[partition_number].head;
    // need to check if next value is different

    pthread_mutex_lock(&lock);
    if (isNextKeyDifferent[partition_number] == 1) {
        // reset key to 0
        isNextKeyDifferent[partition_number] = 0;
        pthread_mutex_unlock(&lock);
        return NULL;
    }

    if (curr_partition != NULL) {
        if (strcmp(curr_partition->key, key) == 0) {
            partitions[partition_number].head = curr_partition->next;
            if (curr_partition->next != NULL) {
                // if not 0, we have a new value
                if (strcmp(curr_partition->next->key, key) != 0){
                    // set flag to 1
                    isNextKeyDifferent[partition_number] = 1;
                    pthread_mutex_unlock(&lock);
                    return curr_partition->value;
                }
                pthread_mutex_unlock(&lock);
                return curr_partition->value;

            }else {
                pthread_mutex_unlock(&lock);
                return curr_partition->value;
            }
        }
    }
    pthread_mutex_unlock(&lock);
    return NULL;
}


void MR_Emit(char *key, char *value) {
    if (strlen(key) == 0) {
        return;
    }

    int hashIndex = p(key, NUM_PARTITIONS);
    struct k_v_pair *curr_partition =  partitions[hashIndex].head;
    struct k_v_pair *new = malloc(sizeof(struct k_v_pair));
    new->key = malloc(sizeof(char)*(strlen(key) + 1));
    strcpy(new->key, key);
    new->value = value;

    struct k_v_pair *iterator = partitions[hashIndex].head;

    if (iterator == NULL){
        partitions[hashIndex].head = new;
        new->next = NULL;
        return;
    }

    pthread_mutex_lock(&lock);
    MR_SortedPartition(key, NUM_PARTITIONS);
    struct k_v_pair *prev = NULL;
    while(iterator != NULL) {
        if(strcmp(iterator->key, key) > 0) {
            if (prev == NULL){
                new->next = iterator;
                partitions[hashIndex].head = new;
                pthread_mutex_unlock(&lock);
                return;
            } else {
                prev->next = new;
                new->next = iterator;
                pthread_mutex_unlock(&lock);
                return;
            }

        }
        prev = iterator;
        iterator = iterator->next;
    }
    prev->next = new;
    new->next = NULL;
    pthread_mutex_unlock(&lock);
}

// Wrapper for Mapper
void* mapper_run() {
    while (1)
    {
        char* curr_filename;
        pthread_mutex_lock(&f_lock);

        if(NUM_FILES <= CURR_FILE){
            pthread_mutex_unlock(&f_lock);
            return NULL;
        }
        curr_filename = FILES[CURR_FILE];
        CURR_FILE++;
        pthread_mutex_unlock(&f_lock);
        m(curr_filename);
    }
}

// Wrapper for Reducer
// void* reducer_run(void *st) {

//     pthread_mutex_lock(&f_lock);
//     int i = 0;
//     while (i < NEXT_PARTITION) {
//         // struct k_v_pair *iterator =  partitions[NEXT_PARTITION].head;
//         // int *p = malloc(sizeof(int));
//         // *p = NEXT_PARTITION;
//         // pthread_setspecific(glob_var_key, p);
//         NEXT_PARTITION++;

//         // int* glob_spec_var = pthread_getspecific(glob_var_key);
//         // while(iterator != NULL) {
//             // r(iterator->key, get_next, *glob_spec_var);
//             r(partitions[*(int *)st].head->key, get_next, *(int*)st);
//             // iterator = partitions[*(glob_spec_var)].head;
//         // }
//         i++;
//     }
//     pthread_mutex_unlock(&f_lock);
//     return st;

void* reducer_run() {
    for (;;) {
        pthread_mutex_lock(&f_lock);
        if (NUM_PARTITIONS <= NEXT_PARTITION){
            pthread_mutex_unlock(&f_lock);
            return NULL;
        }


        struct k_v_pair *iterator =  partitions[NEXT_PARTITION].head;
        int *p = malloc(sizeof(int));
        *p = NEXT_PARTITION;
        pthread_setspecific(glob_var_key, p);
        NEXT_PARTITION++;
        pthread_mutex_unlock(&f_lock);

        int* glob_spec_var = pthread_getspecific(glob_var_key);
        while(iterator != NULL)
        {
            r(iterator->key, get_next, *glob_spec_var);
            iterator = partitions[*glob_spec_var].head;
        }
    }
}



void MR_Run(int argc, char *argv[],
        Mapper map, int num_mappers,
        Reducer reduce, int num_reducers,
        Partitioner partition, int num_partitions) {

    p = partition;
    m = map;
    r = reduce;

    NUM_PARTITIONS = num_partitions;
    NUM_FILES = argc - 1;
    partitions = malloc((num_partitions) * sizeof(struct partition_info));
    isNextKeyDifferent = calloc(num_partitions, sizeof(int) );
    FILES = &argv[1];

    pthread_t mapper_thread[num_mappers];
    pthread_t reducer_thread[num_reducers];

    int reducer_array[num_reducers];

    for (int i = 0; i < num_reducers; i++) {
        reducer_array[i] = i;
    }

    for (int i = 0; i < num_mappers; i++) {
        pthread_create(&mapper_thread[i], NULL, mapper_run, NULL);
    }

    for (int i = 0; i < num_mappers; i++) {
        pthread_join(mapper_thread[i], NULL);
    }

    pthread_key_create(&glob_var_key,NULL);
    for (int i = 0; i < num_reducers; i++) {
        pthread_create(&reducer_thread[i], NULL, reducer_run, NULL);
        // pthread_create(&reducer_thread[i], NULL, reducer_run, &reducer_array[i]);
    }

    for (int i = 0; i < num_reducers; i++) {
        pthread_join(reducer_thread[i], NULL);
    }
}

unsigned long MR_SortedPartition(char *key, int num_partitions) {


    if (num_partitions == 1 || strlen(key) == 0) {
        return 0;
    }

    char sort[4];
    char *pointer;

    strncpy(sort, key, 4);



    return strtoul(sort, &pointer, 36);

    // int i;
    // for(i = num_partitions; i >= 2; i--){
    //     num_partitions /= 2;
    // }

    // int bit_off = i;
    // unsigned long bits_to_comp = 0;

    // // looking at first 4 bits of key
    // for(int j = 0; j < 4; j++) {
    //     // left bit shifting
	// 	bits_to_comp = bits_to_comp << 8;
    //     // appending the next character to key to compare
	// 	bits_to_comp = key[j] + bits_to_comp;
    // }

    // int k;
    // for(k = bits_to_comp; k >= 2; k--){
    //     bits_to_comp /= 2;
    // }

    // int bit_off2 = k;
    // int result = bit_off2 - (bit_off - 1);

    // return bits_to_comp >> result;
}


unsigned long MR_DefaultHashPartition(char *key, int num_partitions)
{
    unsigned long hash = 5381;
    int c;
    while ((c = *key++) != '\0')
        hash = hash * 33 + c;
    return hash % num_partitions;
}


#include <pthread.h>
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include "mapreduce.h"
#include <math.h>
#include "map.h"

pthread_mutex_t f_lock;
pthread_mutex_t lock;
pthread_key_t glob_var_key;

typedef struct partition_info {
    struct k_v_pair *head;
    int *next;
    // int total_files;
    // int check_next;
    // int p_num;
    // int files_processed;
} partition_info;

struct k_v_pair {
    char *key;
    char *value;
    struct k_v_pair *next;
};

// typedef struct files {
  int total_files;
  int check_next;
  int p_num;
  int files_processed;
// } files;

Partitioner p;
Reducer r;
Mapper m;

char** FILES;
// struct files *fileName;
struct partition_info *partitions;


// Gets the next value that will be used in user Reduce
char* get_next(char *key, int partition_number) {
    struct k_v_pair *curr_partition = partitions[partition_number].head;
    // need to check if next value is different

    pthread_mutex_lock(&lock);
    if (partitions->next[partition_number] == 1) {
        // reset key to 0
        partitions->next[partition_number] = 0;
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
                    partitions->next[partition_number] = 1;
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

    int hashIndex = p(key, p_num);
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
    MR_SortedPartition(key, p_num);
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

        if(total_files <= files_processed){
            pthread_mutex_unlock(&f_lock);
            return NULL;
        }
        curr_filename = FILES[files_processed];
        files_processed++;
        pthread_mutex_unlock(&f_lock);
        m(curr_filename);
    }
}

// Wrapper for Reducer
// void* reducer_run(void *st) {
//
//     char **args = (char **) st;
//     while (1) {
//         pthread_mutex_lock(&f_lock);
//         // files.check_next++;
//         int x;
//         if(files.files_processed<=files.total_files){
//             x=files.files_processed;
//             files.files_processed++;
//         }
//         pthread_mutex_unlock(&f_lock);
//
//         r(args[x], get_next, files.p_num);
//     }
//
//     return st;
// void* reducer_run(void *start) {
//     int i = 0;
//     while (i < partition.mapped_keyvals_amount[*(int *) start]) {
//         // if (partition.processed_times[*(int *) start] == i) {
//             // Call reducer on the partitions
//             r(partition_array[*(int *) start][i].key, get_next, *(int *)  start);
//         // }
//         i++;
//     }
//     return start;
void* reducer_run() {
    for (;;) {
        pthread_mutex_lock(&f_lock);
        if (p_num <= check_next){
            pthread_mutex_unlock(&f_lock);
            return NULL;
        }


        struct k_v_pair *iterator =  partitions[check_next].head;
        int *p = malloc(sizeof(int));
        *p = check_next;
        pthread_setspecific(glob_var_key, p);
        check_next++;
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

    p_num = num_partitions;
    total_files = argc - 1;
    partitions = malloc(num_partitions * sizeof(struct partition_info));
    partitions->next = malloc(num_partitions* sizeof(int));

    for (int i = 0; i < num_partitions; i++) {
      partitions->next[i] = 0;
    }
    FILES = &argv[1];

    pthread_t mapper_thread[num_mappers];
    pthread_t reducer_thread[num_reducers];

    // int reducer_array[num_reducers];
    //
    // for (int i = 0; i < num_reducers; i++) {
    //     reducer_array[i] = i;
    // }

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
        // pthread_create(&reducer_thread[i], NULL, reducer_run, (void *) i);
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


    // unsigned long sorted;
    // sorted =
    return strtoul(sort, &pointer, 36);
    // sorted = sorted & 0x0FFFFFFFF;// masking
    // // int num_bits_needed;
    // // num_bits_needed = log2(num_partitions);
    // //int bits = 32 - num_bits_needed;
    //
    // sorted = sorted >> (bits); // shifting msb
    //
    // return sorted;
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


#include <iostream>
#include <fstream>
#include <algorithm>
#include <random>
#include <thread>
#include <cstdint>
#include <cmath>
#include <cstdlib>
#include <algorithm>

#define CACHELINE_SIZE 64

using namespace std;


inline void BUG_ON(bool cond) {
    if (cond)
        abort();
}

uint64_t rotr64(uint64_t x, uint64_t n)
{
    return ((x >> n) | (x << (64 - n)));
}

uint64_t key_hash(uint64_t key)
{
    uint64_t local_rand = key;
    uint64_t local_rand_hash = 8;
    local_rand_hash = 40343 * local_rand_hash + ((local_rand) & 0xFFFF);
    local_rand_hash = 40343 * local_rand_hash + ((local_rand >> 16) & 0xFFFF);
    local_rand_hash = 40343 * local_rand_hash + ((local_rand >> 32) & 0xFFFF);
    local_rand_hash = 40343 * local_rand_hash + (local_rand >> 48);
    local_rand_hash = 40343 * local_rand_hash;
    return rotr64(local_rand_hash, 43);
}

uint64_t pad_alignment(uint64_t size, uint64_t alignment)
{
    return ((size + alignment - 1) / alignment) * alignment;
}

uint64_t get_record_size(uint64_t key_size, uint64_t value_size)
{
    // alignof(RecordInfo): 8
    // alignof(key_t): 8
    // alignof(value_t): 8 (regardless of the value size)
    // if key_size = 8 and value_size = 8, then record_size should be 24
    constexpr uint64_t record_info_size = 8;
    return pad_alignment(value_size
                         + pad_alignment(key_size
                                         + pad_alignment(record_info_size, 8), 8), 8);
}

/*
 * Simulate per-page access distribution based on memory layout
 *
 * Assumptions:
 * + Objects are randomly distributed in the log
 *   (Not necessarily follow the same pattern as pmem_bench)
 * + Hash buckets never overflow
 * + Treat multiple consecutive accesses in one cacheline as one access
 * + Only consider major memory accesses in the lookup/update path
 */

void run(uint64_t   num_keys,
         double    *obj_pmf,
         uint64_t   key_size,
         uint64_t   value_size,
         uint64_t   page_size,
         uint64_t   cpu_cache_size,
         unsigned   seed,
         double   **out_ht_exp,
         uint64_t  *out_ht_num_pages,
         double   **out_log_exp,
         uint64_t  *out_log_num_pages)
{
    uint64_t num_buckets = 1UL << ((uint64_t) ceil(log2(num_keys / 2)));
    uint64_t record_size = get_record_size(key_size, value_size);

    // Each bucket is 64 B
    uint64_t hash_bucket_size = 64;
    uint64_t hash_table_size = num_buckets * hash_bucket_size;

    uint64_t log_size = 0;
    uint64_t *log_mapping = (uint64_t *) malloc(num_keys * sizeof(*log_mapping));
    BUG_ON(log_mapping == NULL);
    // Each log slot is 32 MB
    uint64_t log_slot_size = 32 * (1UL << 20);

    fprintf(stderr, "Simulating loading...\n");
    uint64_t *obj_load_idx = (uint64_t *) malloc(num_keys * sizeof(*obj_load_idx));
    BUG_ON(obj_load_idx == NULL);
    for (uint64_t i = 0; i < num_keys; ++i) {
        obj_load_idx[i] = i;
    }
    std::shuffle(obj_load_idx, obj_load_idx + num_keys, std::default_random_engine(seed));
    for (uint64_t i = 0; i < num_keys; ++i) {
        uint64_t obj_idx = obj_load_idx[i];
        uint64_t local_slot_offset = log_size % log_slot_size;
        if (local_slot_offset + record_size > log_slot_size) {
            /* No enough free space in the current log slot,
             * so move on to the next log slot
             */
            log_size += log_slot_size - local_slot_offset;
        }
        log_mapping[i] = log_size;
        log_size += record_size;
        if (i % 100000 == 0) {
            fprintf(stderr, "\tProgress: %lld/%lld (%.2f%%)\r", i + 1, num_keys,
                    ((double) (100 * (i + 1))) / (double) num_keys);
        }
    }
    fprintf(stderr, "\tProgress: %lld/%lld (100.00%%)\n", num_keys, num_keys);

    const uint64_t lines_per_page = page_size / CACHELINE_SIZE;

    uint64_t hash_table_num_lines = (hash_table_size + CACHELINE_SIZE - 1) / CACHELINE_SIZE;
    double *ht_exp_line = (double *) malloc(hash_table_num_lines * sizeof(*ht_exp_line));
    BUG_ON(ht_exp_line == NULL);
    memset(ht_exp_line, 0, hash_table_num_lines * sizeof(*ht_exp_line));

    uint64_t hash_table_num_pages = (hash_table_size + page_size - 1) / page_size;
    *out_ht_exp = (double *) malloc(hash_table_num_pages * sizeof(**out_ht_exp));
    BUG_ON(*out_ht_exp == NULL);
    memset(*out_ht_exp, 0, hash_table_num_pages * sizeof(**out_ht_exp));

    uint64_t log_num_lines = (log_size + CACHELINE_SIZE - 1) / CACHELINE_SIZE;
    double *log_exp_line = (double *) malloc(log_num_lines * sizeof(*log_exp_line));
    BUG_ON(log_exp_line == NULL);
    memset(log_exp_line, 0, log_num_lines * sizeof(*log_exp_line));

    uint64_t log_num_pages = (log_size + page_size - 1) / page_size;
    *out_log_exp = (double *) malloc(log_num_pages * sizeof(**out_log_exp));
    BUG_ON(*out_log_exp == NULL);
    memset(*out_log_exp, 0, log_num_pages * sizeof(**out_log_exp));

    fprintf(stderr, "Calculating cacheline-level distribution...\n");
    for (uint64_t i = 0; i < num_keys; ++i) {
        double obj_pm = obj_pmf[i];
        uint64_t hash_bucket = key_hash(i) % num_buckets;
        uint64_t hash_table_line = (hash_bucket * hash_bucket_size) / CACHELINE_SIZE;
        ht_exp_line[hash_table_line] += obj_pm;

        uint64_t log_addr = log_mapping[i];
        for (uint64_t j = log_addr / CACHELINE_SIZE;
             j <= (log_addr + record_size - 1) / CACHELINE_SIZE;
             ++j)
        {
            log_exp_line[j] += obj_pm;
        }
        if (i % 100000 == 0) {
            fprintf(stderr, "\tProgress: %lld/%lld (%.2f%%)\r", i + 1, num_keys,
                    ((double) (100 * (i + 1))) / (double) num_keys);
        }
    }
    fprintf(stderr, "\tProgress: %lld/%lld (100.00%%)\n", num_keys, num_keys);

    fprintf(stderr, "Simulating CPU cache...\n");
    uint64_t *line_index_arr = (uint64_t *) malloc((hash_table_num_lines + log_num_lines) * sizeof(*line_index_arr));
    BUG_ON(line_index_arr == NULL);
    iota(line_index_arr, line_index_arr + (hash_table_num_lines + log_num_lines), 0);
    stable_sort(line_index_arr, line_index_arr + (hash_table_num_lines + log_num_lines),
    [&](const uint64_t &i, const uint64_t &j){
        double val_i, val_j;
        if (i >= hash_table_num_lines) {
            val_i = log_exp_line[i - hash_table_num_lines];
        } else {
            val_i = ht_exp_line[i];
        }
        if (j >= hash_table_num_lines) {
            val_j = log_exp_line[j - hash_table_num_lines];
        } else {
            val_j = ht_exp_line[j];
        }
        return val_i < val_j;
    });
    for (uint64_t i = 0; i < cpu_cache_size / CACHELINE_SIZE; ++i) {
        uint64_t line_index = line_index_arr[hash_table_num_lines + log_num_lines - i - 1];
        if (line_index >= hash_table_num_lines) {
            log_exp_line[line_index - hash_table_num_lines] = 0;
        } else {
            ht_exp_line[line_index] = 0;
        }
    }

    fprintf(stderr, "Calculating page-level distribution...\n");
    for (uint64_t i = 0; i < hash_table_num_lines; ++i) {
        (*out_ht_exp)[i / lines_per_page] += ht_exp_line[i];
    }
    for (uint64_t i = 0; i < log_num_lines; ++i) {
        (*out_log_exp)[i / lines_per_page] += log_exp_line[i];
    }

    *out_ht_num_pages = hash_table_num_pages;
    *out_log_num_pages = log_num_pages;

    free(line_index_arr);
    free(ht_exp_line);
    free(log_exp_line);
    free(obj_load_idx);
    free(log_mapping);
}

int main(int argc, char *argv[])
{
    if (argc != 7) {
        fprintf(stderr, "Usage: %s <num_keys> <key_size> <value_size> <page_size> <cpu cache size> <a>\n", argv[0]);
        exit(1);
    }
    uint64_t num_keys = atol(argv[1]);
    uint64_t key_size = atol(argv[2]);
    uint64_t value_size = atol(argv[3]);
    uint64_t page_size = atol(argv[4]);
    uint64_t cpu_cache_size = atol(argv[5]);
    unsigned pmf_seed = 0xBAD;
    unsigned seed = 0xBEEF;
    double a = atof(argv[6]);

    double *obj_pmf = (double *) malloc(num_keys * sizeof(*obj_pmf));
    BUG_ON(obj_pmf == NULL);
    for (uint64_t i = 0; i < num_keys; ++i) {
        if (a == 0) {
            obj_pmf[i] = 1.0L / ((double) num_keys);
        } else {
            obj_pmf[i] = 1.0L / pow((double) (i + 1), (double) a);
        }
    }
    if (a != 0) {
        std::shuffle(obj_pmf, obj_pmf + num_keys, std::default_random_engine(pmf_seed));
    }

    double *out_ht_exp = NULL;
    uint64_t out_ht_num_pages;
    double *out_log_exp = NULL;
    uint64_t out_log_num_pages;
    run(num_keys, obj_pmf, key_size, value_size, page_size, cpu_cache_size, seed,
        &out_ht_exp, &out_ht_num_pages, &out_log_exp, &out_log_num_pages);
    BUG_ON(out_ht_exp == NULL);
    BUG_ON(out_log_exp == NULL);

    printf("%lld\n", out_ht_num_pages);
    printf("%lld\n", out_log_num_pages);
    for (uint64_t i = 0; i < out_ht_num_pages; ++i) {
        printf("%.32f\n", out_ht_exp[i]);
    }
    for (uint64_t i = 0; i < out_log_num_pages; ++i) {
        printf("%.32f\n", out_log_exp[i]);
    }

    free(out_ht_exp);
    free(out_log_exp);
    free(obj_pmf);
    return 0;
}

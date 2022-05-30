#include <iostream>
#include <fstream>
#include <algorithm>
#include <random>
#include <thread>
#include <cstdint>
#include <cmath>
#include <cstdlib>
#include <algorithm>
#include <unordered_map>
#include <vector>
#include <cstdlib>
#include <queue>

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

void mrc_run(uint64_t   num_keys,
             double    *obj_pmf,
             uint64_t   key_size,
             uint64_t   value_size,
             uint64_t   page_size,
             uint64_t   cpu_cache_size,
             unsigned   seed,
             double   **out_ht_exp,
             uint64_t  *out_ht_num_lines,
             double   **out_log_exp,
             uint64_t  *out_log_num_lines,
             double    *out_cache_exp,
             uint64_t **out_log_mapping)
{
    uint64_t num_buckets = 1UL << ((uint64_t) ceil(log2(num_keys / 2)));
    uint64_t record_size = get_record_size(key_size, value_size);

    // Each bucket is 64 B
    uint64_t hash_bucket_size = 64;
    uint64_t hash_table_size = num_buckets * hash_bucket_size;

    uint64_t log_size = 0;
    uint64_t *log_mapping = (uint64_t *) malloc(num_keys * sizeof(*log_mapping));
    BUG_ON(log_mapping == NULL);
    *out_log_mapping = log_mapping;
    // Each log slot is 32 MB
    uint64_t log_slot_size = 32 * (1UL << 20);

    fprintf(stderr, "Simulating loading...\n");
    uint64_t *obj_load_idx = (uint64_t *) malloc(num_keys * sizeof(*obj_load_idx));
    BUG_ON(obj_load_idx == NULL);
    iota(obj_load_idx, obj_load_idx + num_keys, 0);
    shuffle(obj_load_idx, obj_load_idx + num_keys, default_random_engine(seed));
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
    *out_ht_exp = ht_exp_line;
    *out_ht_num_lines = hash_table_num_lines;
    BUG_ON(ht_exp_line == NULL);
    memset(ht_exp_line, 0, hash_table_num_lines * sizeof(*ht_exp_line));

    uint64_t log_num_lines = (log_size + CACHELINE_SIZE - 1) / CACHELINE_SIZE;
    double *log_exp_line = (double *) malloc(log_num_lines * sizeof(*log_exp_line));
    *out_log_exp = log_exp_line;
    *out_log_num_lines = log_num_lines;
    BUG_ON(log_exp_line == NULL);
    memset(log_exp_line, 0, log_num_lines * sizeof(*log_exp_line));

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
    priority_queue<uint64_t, vector<uint64_t>, function<bool(uint64_t, uint64_t)>> queue(
    [&](uint64_t i, uint64_t j){
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
        return val_i > val_j;
    });

    uint64_t num_lines_in_cpu_cache = cpu_cache_size / CACHELINE_SIZE;
    for (uint64_t i = 0; i < hash_table_num_lines + log_num_lines; ++i) {
        queue.push(i);
        if (queue.size() > num_lines_in_cpu_cache) {
            queue.pop();
        }
        BUG_ON(queue.size() > num_lines_in_cpu_cache);
        if (i % 100000 == 0) {
            fprintf(stderr, "\tProgress: %lld/%lld (%.2f%%)\r", i + 1,
                    hash_table_num_lines + log_num_lines,
                    ((double) (100 * (i + 1))) / (double) (hash_table_num_lines + log_num_lines));
        }
    }
    fprintf(stderr, "\tProgress: %lld/%lld (100.00%%)\n",
            hash_table_num_lines + log_num_lines, hash_table_num_lines + log_num_lines);
    *out_cache_exp = 0;
    while (!queue.empty()) {
        uint64_t line_index = queue.top();
        queue.pop();
        if (line_index >= hash_table_num_lines) {
            *out_cache_exp += log_exp_line[line_index - hash_table_num_lines];
            log_exp_line[line_index - hash_table_num_lines] = 0;
        } else {
            *out_cache_exp += ht_exp_line[line_index];
            ht_exp_line[line_index] = 0;
        }
    }

    free(obj_load_idx);
}

int main(int argc, char *argv[])
{
    if (argc != 10) {
        fprintf(stderr, "Usage: %s <num_keys> <key_size> <value_size> <page_size> <cpu_cache_size> <a> <dram_size> <map_seed> <max_num_threads>\n", argv[0]);
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
    uint64_t dram_size = atol(argv[7]);
    BUG_ON(dram_size % page_size != 0);
    unsigned int map_seed = atoi(argv[8]);
    int max_num_threads = atoi(argv[9]);
    BUG_ON(max_num_threads < 1);

    double *obj_pmf = (double *) malloc(num_keys * sizeof(*obj_pmf));
    BUG_ON(obj_pmf == NULL);
    double obj_pmf_sum = 0;
    for (uint64_t i = 0; i < num_keys; ++i) {
        if (a == 0) {
            obj_pmf[i] = 1.0L / ((double) num_keys);
        } else {
            obj_pmf[i] = 1.0L / pow((double) (i + 1), (double) a);
            obj_pmf_sum += obj_pmf[i];
        }
    }
    if (a != 0) {
        shuffle(obj_pmf, obj_pmf + num_keys, default_random_engine(pmf_seed));
        for (uint64_t i = 0; i < num_keys; ++i) {
            obj_pmf[i] = obj_pmf[i] / obj_pmf_sum;
        }
    }

    double *out_ht_exp = NULL;
    uint64_t out_ht_num_lines;
    double *out_log_exp = NULL;
    uint64_t out_log_num_lines;
    double out_cache_exp;
    uint64_t *log_mapping;
    mrc_run(num_keys, obj_pmf, key_size, value_size, page_size, cpu_cache_size, seed,
            &out_ht_exp, &out_ht_num_lines, &out_log_exp, &out_log_num_lines,
            &out_cache_exp, &log_mapping);
    BUG_ON(out_ht_exp == NULL);
    BUG_ON(out_log_exp == NULL);

    const uint64_t lines_per_page = page_size / CACHELINE_SIZE;
    uint64_t num_buckets = 1UL << ((uint64_t) ceil(log2(num_keys / 2)));
    uint64_t record_size = get_record_size(key_size, value_size);
    uint64_t hash_bucket_size = 64;

    // Generate mapping between PMEM pages and DRAM pages
    uint64_t ht_num_pages = (out_ht_num_lines * CACHELINE_SIZE + page_size - 1) / page_size;
    uint64_t log_num_pages = (out_log_num_lines * CACHELINE_SIZE + page_size - 1) / page_size;
 
    uint64_t num_dram_pages = dram_size / page_size;
    BUG_ON(8 * num_dram_pages < ht_num_pages + log_num_pages);
    uint64_t *dram_page_arr = (uint64_t *) malloc(num_dram_pages * sizeof(*dram_page_arr));
    BUG_ON(dram_page_arr == NULL);
    iota(dram_page_arr, dram_page_arr + num_dram_pages, 0);
    // shuffle(dram_page_arr, dram_page_arr + num_dram_pages, default_random_engine(rand_r(&map_seed)));

    uint64_t *ht_page_map_arr = (uint64_t *) malloc(ht_num_pages * sizeof(*ht_page_map_arr));
    BUG_ON(ht_page_map_arr == NULL);
    uint64_t *log_page_map_arr = (uint64_t *) malloc(log_num_pages * sizeof(*log_page_map_arr));
    BUG_ON(log_page_map_arr == NULL);

    uint64_t dram_page_index = 0;
    for (uint64_t i = 0; i < ht_num_pages; ++i) {
        ht_page_map_arr[i] = dram_page_arr[dram_page_index++];
        if (dram_page_index == num_dram_pages) {
            // shuffle(dram_page_arr, dram_page_arr + num_dram_pages, default_random_engine(rand_r(&map_seed)));
            dram_page_index = 0;
        }
    }
    for (uint64_t i = 0; i < log_num_pages; ++i) {
        log_page_map_arr[i] = dram_page_arr[dram_page_index++];
        if (dram_page_index == num_dram_pages) {
            // shuffle(dram_page_arr, dram_page_arr + num_dram_pages, default_random_engine(rand_r(&map_seed)));
            dram_page_index = 0;
        }
    }

    // Calculate unnormalized hit prob and the normalization factor
    uint64_t num_dram_lines = num_dram_pages * lines_per_page;
    double *dram_line_ht_hit_arr = (double *) malloc(num_dram_lines * sizeof(*dram_line_ht_hit_arr));
    BUG_ON(dram_line_ht_hit_arr == NULL);
    memset(dram_line_ht_hit_arr, 0, num_dram_lines * sizeof(*dram_line_ht_hit_arr));

    double *dram_line_log_hit_arr = (double *) malloc(num_dram_lines * sizeof(*dram_line_log_hit_arr));
    BUG_ON(dram_line_log_hit_arr == NULL);
    memset(dram_line_log_hit_arr, 0, num_dram_lines * sizeof(*dram_line_log_hit_arr));

    double *ht_hit = (double *) malloc(out_ht_num_lines * sizeof(*ht_hit));
    BUG_ON(ht_hit == NULL);
    memset(ht_hit, 0, out_ht_num_lines * sizeof(*ht_hit));

    double *log_hit = (double *) malloc(out_log_num_lines * sizeof(*log_hit));
    BUG_ON(log_hit == NULL);
    memset(log_hit, 0, out_log_num_lines * sizeof(*log_hit));

    fprintf(stderr, "Calculating unnormalized hit probability...\n");
    for (uint64_t i = 0; i < num_keys; ++i) {
        double obj_pm = obj_pmf[i];

        uint64_t hash_bucket = key_hash(i) % num_buckets;
        uint64_t hash_table_line = (hash_bucket * hash_bucket_size) / CACHELINE_SIZE;

        uint64_t hash_table_dram_line = ht_page_map_arr[hash_table_line / lines_per_page] * lines_per_page
                                        + (hash_table_line % lines_per_page);
        ht_hit[hash_table_line] += obj_pm;
        dram_line_ht_hit_arr[hash_table_dram_line] += obj_pm;

        uint64_t log_addr = log_mapping[i];
        for (uint64_t j = log_addr / CACHELINE_SIZE;
             j <= (log_addr + record_size - 1) / CACHELINE_SIZE;
             ++j)
        {
            uint64_t log_dram_line = log_page_map_arr[j / lines_per_page] * lines_per_page
                                     + (j % lines_per_page);
            log_hit[j] += obj_pm;
            dram_line_log_hit_arr[log_dram_line] += obj_pm;
        }
        if (i % 100000 == 0) {
            fprintf(stderr, "\tProgress: %lld/%lld (%.2f%%)\r", i + 1, num_keys,
                    ((double) (100 * (i + 1))) / (double) num_keys);
        }
    }
    fprintf(stderr, "\tProgress: %lld/%lld (100.00%%)\n", num_keys, num_keys);

    // Calculate overall hit prob
    fprintf(stderr, "Calculating overall hit probability...\n");
    for (int num_threads = 1; num_threads <= max_num_threads; ++num_threads) {
        double hit_prob = 0;
        double exp_sum = 0;
        for (uint64_t i = 0; i < out_ht_num_lines; ++i) {
            uint64_t hash_table_dram_line = ht_page_map_arr[i / lines_per_page] * lines_per_page
                                            + (i % lines_per_page);
            if (dram_line_ht_hit_arr[hash_table_dram_line] == 0)
                continue;
            double cur_hit_prob = ht_hit[i] / dram_line_ht_hit_arr[hash_table_dram_line];
            double p = 1 - dram_line_ht_hit_arr[hash_table_dram_line];
            double q = 1 - dram_line_log_hit_arr[hash_table_dram_line];
            cur_hit_prob *= (1.0 - ((1.0 - pow(p, num_threads)) * (1.0 - pow(q, num_threads)))
                                   / (num_threads * (1.0 - p) * (1.0 - pow(p, num_threads) * pow(q, num_threads))));
            hit_prob += out_ht_exp[i] * cur_hit_prob;
            exp_sum += out_ht_exp[i];
        }
        for (uint64_t i = 0; i < out_log_num_lines; ++i) {
            uint64_t log_dram_line = log_page_map_arr[i / lines_per_page] * lines_per_page
                                     + (i % lines_per_page);
            if (dram_line_log_hit_arr[log_dram_line] == 0)
                continue;
            double cur_hit_prob = log_hit[i] / dram_line_log_hit_arr[log_dram_line];
            double p = 1 - dram_line_ht_hit_arr[log_dram_line];
            double q = 1 - dram_line_log_hit_arr[log_dram_line];
            cur_hit_prob *= (1.0 - ((1.0 - pow(p, num_threads)) * (1.0 - pow(q, num_threads)))
                                   / (num_threads * (1.0 - q) * (1.0 - pow(p, num_threads) * pow(q, num_threads))));
            hit_prob += out_log_exp[i] * cur_hit_prob;
            exp_sum += out_log_exp[i];
        }
        printf("Overall miss probability (%d threads): %.4f\n", num_threads, 1 - (hit_prob / exp_sum));
        fflush(stdout);
    }

    free(out_ht_exp);
    free(out_log_exp);
    free(obj_pmf);
    return 0;
}

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <sched.h>
#include <iostream>
#include <cstdlib>
#include <cstring>
#include <csignal>
#include <cmath>
#include <thread>
#include <atomic>
#include <numa.h>
#include <sys/mman.h>
#include <unistd.h>
#include <random>
#include <algorithm>
#include <fcntl.h>

#define HUGE_PAGE_SIZE (1UL << 21UL)

using namespace std;

static inline void BUG_ON(bool cond) {
  if (cond) {
    raise(SIGABRT);
  }
}

int main(int argc, char *argv[]) {
  if (argc != 2) {
    printf("Usage: %s <size of memory (GB)>\n", argv[0]);
    exit(1);
  }

  uint64_t size_GB = atol(argv[1]);
  uint64_t size = size_GB << 30UL;
  uint64_t num_pages = size / HUGE_PAGE_SIZE;

  printf("Allocating %ld 2MB pages...\n", num_pages);
  uint8_t *buffer = (uint8_t *) aligned_alloc(HUGE_PAGE_SIZE, size);
  BUG_ON(buffer == NULL);
  int ret = madvise(buffer, size, MADV_HUGEPAGE);
  BUG_ON(ret != 0);

  printf("Triggering random page faults...\n");
  uint64_t *index_arr = (uint64_t *) malloc(sizeof(*index_arr) * num_pages);
  BUG_ON(index_arr == NULL);
  iota(index_arr, index_arr + num_pages, 0);

  random_device rd;
  shuffle(index_arr, index_arr + num_pages, default_random_engine(rd()));
  for (uint64_t i = 0; i < num_pages; ++i) {
    uint64_t index = index_arr[i];
    memset(buffer + index * HUGE_PAGE_SIZE, 0, HUGE_PAGE_SIZE);
  }
}

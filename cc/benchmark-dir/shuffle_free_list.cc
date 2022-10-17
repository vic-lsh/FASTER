#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <cstdlib>
#include <cstring>
#include <csignal>
#include <cmath>
#include <thread>
#include <iostream>
#include <atomic>
#include <random>
#include <algorithm>
#include <unordered_map>
#include <sys/mman.h>
#include <unistd.h>
#include <numa.h>
#include <sched.h>
#include <fcntl.h>
#include <linux/page_coloring.h>

#define HUGE_PAGE_SHIFT 21UL
#define HUGE_PAGE_SIZE (1UL << HUGE_PAGE_SHIFT)
#define BITMAP_SIZE ((NR_COLORS + sizeof(uint64_t) * 8 - 1) / (sizeof(uint64_t) * 8))

using namespace std;

#define BUG_ON(cond)    \
  do {                  \
    if (cond) {         \
      fprintf(stdout, "BUG_ON: %s (L%d) %s\n", __FILE__, __LINE__, __FUNCTION__); \
      raise(SIGABRT);   \
    }                   \
  } while (0)


void convert_list_to_bitmap(char *list, uint64_t *bitmap) {
  char *rest_list;
  char *interval = strtok_r(list, ",", &rest_list);
  while (interval != NULL) {
    int start, end;
    if (strstr(interval, "-") != NULL) {
      char *rest_token;
      char *token = strtok_r(interval, "-", &rest_token);
      start = atoi(token);
      token = strtok_r(NULL, "-", &rest_token);
      end = atoi(token);
    } else {
      start = atoi(interval);
      end = start;
    }
    for (int i = start; i <= end; ++i) {
      int buffer_index = i / (sizeof(uint64_t) * 8);
      int bit_index = i % (sizeof(uint64_t) * 8);
      bitmap[buffer_index] = bitmap[buffer_index] | (1l << bit_index);
    }
    interval = strtok_r(NULL, ",", &rest_list);
  }
}

uint64_t addr_translate(int fd, void *ptr) {
  uint64_t virtual_addr = (uint64_t) ptr;
  uint64_t virtual_pfn = virtual_addr / 4096;
  size_t offset = virtual_pfn * sizeof(uint64_t);

  /*
   * /proc/self/pagemap doc:
   * https://www.kernel.org/doc/Documentation/vm/pagemap.txt
   */
  uint64_t page;
  int ret = pread(fd, &page, 8, offset);
  BUG_ON(ret != 8);
  BUG_ON((page & (1UL << 63UL)) == 0);  // should present
  BUG_ON((page & (1UL << 62UL)) != 0);  // shoud not be swapped
  BUG_ON((page & 0x7fffffffffffffUL) == 0);

  uint64_t physical_addr = ((page & 0x7fffffffffffffUL) * 4096)
                           + (virtual_addr % 4096);
  BUG_ON((physical_addr % 4096) != (virtual_addr % 4096));
  return physical_addr;
}

uint64_t get_page_color(uint64_t phys_addr) {
  uint64_t dram_addr = phys_addr % DRAM_SIZE_PER_NODE;
  uint64_t dram_index = dram_addr >> HUGE_PAGE_SHIFT;
  uint64_t tmp_1 = dram_index % NR_COLORS;
  uint64_t tmp_2 = dram_index / NR_COLORS;
  return (tmp_1 + tmp_2) % NR_COLORS;
}

bool color_is_set(uint64_t *colormask, uint64_t color) {
  uint64_t buffer_index = color / (sizeof(*colormask) * 8);
  uint64_t bit_index = color % (sizeof(*colormask) * 8);
  return colormask[buffer_index] & (1UL << bit_index);
}

int main(int argc, char *argv[]) {
  if (argc != 2 && argc != 5) {
    printf("Usage: %s <size of memory (GB)> <nid> <color list (e.g., 0-7,12)> <max occupancy>\n", argv[0]);
    exit(1);
  }

  uint64_t size_GB = atol(argv[1]);
  uint64_t size = size_GB << 30UL;
  uint64_t num_pages = size / HUGE_PAGE_SIZE;

  uint64_t nid;
  uint64_t colormask[BITMAP_SIZE];
  uint64_t max_occupancy;
  if (argc == 5) {
    nid = atol(argv[2]);
    memset(colormask, 0, sizeof(colormask));
    convert_list_to_bitmap(argv[3], colormask);
    max_occupancy = atol(argv[4]);
    BUG_ON(max_occupancy == 0);
  } else {
    BUG_ON(argc != 2);
  }

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

  if (argc == 2)
    return 0;

  unordered_map<uint64_t, uint64_t> map;
  int fd = open("/proc/self/pagemap", O_RDONLY);
  BUG_ON(fd < 0);
  for (uint64_t i = 0; i < num_pages; ++i) {
    uint64_t phys_addr = addr_translate(fd, buffer + i * HUGE_PAGE_SIZE);
    BUG_ON(phys_addr / (DRAM_SIZE_PER_NODE * NR_PMEM_CHUNK) != nid);

    uint64_t color = get_page_color(phys_addr);
    BUG_ON(!color_is_set(colormask, color));

    uint64_t dram_addr = phys_addr % DRAM_SIZE_PER_NODE;
    if (map.find(dram_addr) == map.end()) {
      map[dram_addr] = 0;
    }
    map[dram_addr]++;
    BUG_ON(map[dram_addr] > max_occupancy);
  }
  BUG_ON(close(fd) != 0);
  return 0;
}

// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <cstdint>
#include <cstring>
#include <cstdlib>
#include <stdexcept>
#include <string>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <numaif.h>
#include <numa.h>

#define PAGE_SIZE 4096
#define HUGE_PAGE_SIZE 2097152

#if defined(USE_OPT) && ((OPT_PAGE_SIZE) == (HUGE_PAGE_SIZE))
#define OPT_HUGE_PAGE
#endif

#ifdef USE_OPT
#define OPT_DRAM_NUMA 0
#define OPT_PMEM_NUMA 2
#endif

namespace FASTER {
namespace core {

class Utility {
 public:
  static inline uint64_t Rotr64(uint64_t x, std::size_t n) {
    return (((x) >> n) | ((x) << (64 - n)));
  }

  static inline uint64_t GetHashCode(uint64_t input) {
    uint64_t local_rand = input;
    uint64_t local_rand_hash = 8;
    local_rand_hash = 40343 * local_rand_hash + ((local_rand) & 0xFFFF);
    local_rand_hash = 40343 * local_rand_hash + ((local_rand >> 16) & 0xFFFF);
    local_rand_hash = 40343 * local_rand_hash + ((local_rand >> 32) & 0xFFFF);
    local_rand_hash = 40343 * local_rand_hash + (local_rand >> 48);
    local_rand_hash = 40343 * local_rand_hash;
    return Rotr64(local_rand_hash, 43);
    //Func<long, long> hash =
    //    e => 40343 * (40343 * (40343 * (40343 * (40343 * 8 + (long)((e) & 0xFFFF)) + (long)((e >> 16) & 0xFFFF)) + (long)((e >> 32) & 0xFFFF)) + (long)(e >> 48));
  }

  static inline uint64_t HashBytes(const uint16_t* str, size_t len) {
    // 40343 is a "magic constant" that works well,
    // 38299 is another good value.
    // Both are primes and have a good distribution of bits.
    const uint64_t kMagicNum = 40343;
    uint64_t hashState = len;

    for(size_t idx = 0; idx < len; ++idx) {
      hashState = kMagicNum * hashState + str[idx];
    }

    // The final scrambling helps with short keys that vary only on the high order bits.
    // Low order bits are not always well distributed so shift them to the high end, where they'll
    // form part of the 14-bit tag.
    return Rotr64(kMagicNum * hashState, 6);
  }

  static inline uint64_t HashBytesUint8(const uint8_t* str, size_t len) {
    // 40343 is a "magic constant" that works well,
    // 38299 is another good value.
    // Both are primes and have a good distribution of bits.
    const uint64_t kMagicNum = 40343;
    uint64_t hashState = len;

    for(size_t idx = 0; idx < len; ++idx) {
      hashState = kMagicNum * hashState + str[idx];
    }

    // The final scrambling helps with short keys that vary only on the high order bits.
    // Low order bits are not always well distributed so shift them to the high end, where they'll
    // form part of the 14-bit tag.
    return Rotr64(kMagicNum * hashState, 6);
  }

  static constexpr inline bool IsPowerOfTwo(uint64_t x) {
    return (x > 0) && ((x & (x - 1)) == 0);
  }
};

inline void BUG_ON(bool cond) {
  if (cond) {
    abort();
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
	assert((physical_addr % 4096) == (virtual_addr % 4096));
	return physical_addr;
};

void *huge_mmap(uint64_t size) {
  BUG_ON(size % HUGE_PAGE_SIZE != 0);
  void *mmap_ret = mmap(NULL, size + HUGE_PAGE_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0);
  BUG_ON(mmap_ret == MAP_FAILED);
  return (void *) (((((uint64_t) mmap_ret) + HUGE_PAGE_SIZE - 1) / HUGE_PAGE_SIZE) * HUGE_PAGE_SIZE);
}

void huge_madvise(void *addr, uint64_t size) {
  int ret = madvise(addr, size, MADV_HUGEPAGE);
  BUG_ON(ret != 0);
}

void numa_bind(void *addr, uint64_t size, uint64_t node) {
  BUG_ON(((uint64_t) addr) % PAGE_SIZE != 0);
  BUG_ON(size % PAGE_SIZE != 0);

  struct bitmask *nodes;
  nodes = numa_allocate_nodemask();
  BUG_ON(nodes == NULL);

  numa_bitmask_setbit(nodes, node);

  long mbind_ret = mbind(addr, size, MPOL_BIND, nodes->maskp, nodes->size + 1, MPOL_F_STATIC_NODES);
  BUG_ON(mbind_ret != 0);

  numa_bitmask_free(nodes);
}

void numa_remap(void *addr, uint64_t size, uint64_t node) {
  BUG_ON(((uint64_t) addr) % PAGE_SIZE != 0);
  BUG_ON(size % PAGE_SIZE != 0);
  BUG_ON(((uint64_t) addr) % OPT_PAGE_SIZE != 0);
  BUG_ON(size % OPT_PAGE_SIZE != 0);

  // Copy data into a tmp buffer
  uint8_t *tmp_buf = (uint8_t *) aligned_alloc(OPT_PAGE_SIZE, size);
  memcpy(tmp_buf, addr, size);

  // Unmap existing pages
  int ret = munmap(addr, size);
  BUG_ON(ret != 0);

  // Re-mmap new pages
  void *mmap_ret = mmap(addr, size, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS | MAP_FIXED_NOREPLACE, -1, 0);
  BUG_ON(mmap_ret == MAP_FAILED);

  // Bind to new NUMA node
  numa_bind(addr, size, node);

#ifdef OPT_HUGE_PAGE
  // Re-madvise THP
  huge_madvise(addr, size);
#endif

  // Copy data into the new pages
  memcpy(addr, tmp_buf, size);

  aligned_free(tmp_buf);
}

}
} // namespace FASTER::core

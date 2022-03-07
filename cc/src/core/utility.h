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
	BUG_ON((page & 0x7fffffffffffffUL) == 0);

	uint64_t physical_addr = ((page & 0x7fffffffffffffUL) * 4096)
	                         + (virtual_addr % 4096);
	assert((physical_addr % 4096) == (virtual_addr % 4096));
	return physical_addr;
};

}
} // namespace FASTER::core

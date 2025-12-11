// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#include <atomic>
#include <cinttypes>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cmath>
#include <random>
#include <string>
#include <numeric>
#include <algorithm>

#include "aligned_atomic.hpp"

#include "file.h"

#include "core/auto_ptr.h"
#include "core/faster.h"
#include "device/null_disk.h"

using namespace std::chrono_literals;
using namespace FASTER::core;
using namespace std;

/// Basic YCSB benchmark.

enum class Op : uint8_t {
  Insert = 0,
  Read = 1,
  Upsert = 2,
  Scan = 3,
  ReadModifyWrite = 4,
};

enum class Workload {
  A_50_50 = 0,
  B_95_5 = 1,
  C_100 = 2,
  F_50_RMW_50 = 5,
};

static constexpr uint64_t kRefreshInterval = 64;
static constexpr uint64_t kCompletePendingInterval = 1600;

static_assert(kCompletePendingInterval % kRefreshInterval == 0,
              "kCompletePendingInterval % kRefreshInterval != 0");

static constexpr uint64_t kNanosPerSecond = 1000000000;
static constexpr uint64_t kMaxKey = 268435456;

static constexpr uint64_t kNumWarmupThreads = 8;

double zipfian_constant_;
uint64_t num_records_;
uint64_t num_records_nodead_;
uint64_t num_ops_;
uint64_t num_warmup_ops_;
#ifdef USE_OPT
uint64_t dram_size_;  // GB
#endif
long max_run_time;
int64_t churn_per_hour_;
int64_t churn_activation_period_sec_;
volatile bool running = true;

struct {
  double zetan;
  double theta;
  double theta2;
  double zeta2theta;
  double alpha;
  double eta;
} zipfian_ctxt_;

std::atomic<uint64_t> total_ops_done_{ 0 };

std::atomic<uint64_t> total_duration_{ 0 };
std::atomic<uint64_t> total_reads_done_{ 0 };
std::atomic<uint64_t> total_writes_done_{ 0 };

class ReadContext;
class UpsertContext;
class RmwContext;

/// This benchmark stores 8-byte keys in key-value store.
class Key {
 public:
  Key(uint64_t key)
    : key_{ key } {
  }

  /// Methods and operators required by the (implicit) interface:
  inline static constexpr uint32_t size() {
    return static_cast<uint32_t>(sizeof(Key));
  }
  inline KeyHash GetHash() const {
    return KeyHash{ Utility::GetHashCode(key_) };
  }

  /// Comparison operators.
  inline bool operator==(const Key& other) const {
    return key_ == other.key_;
  }
  inline bool operator!=(const Key& other) const {
    return key_ != other.key_;
  }

 private:
  uint64_t key_;
};

#define VALUE_NUM_UINT64 (VALUE_SIZE / 8)

#if VALUE_NUM_UINT64 == 1
// 8B value
class Value {
 public:
  Value() {
    value_ = 0;
  }

  Value(const Value& other) {
    value_ = other.value_;
  }

  Value(uint64_t value) {
    value_ = value;
  }

  inline static constexpr uint32_t size() {
    return static_cast<uint32_t>(sizeof(Value));
  }

  friend class ReadContext;
  friend class UpsertContext;
  friend class RmwContext;

 private:
  union {
    uint64_t value_;
    std::atomic<uint64_t> atomic_value_;
  };
};

/// Class passed to store_t::Read().
class ReadContext : public IAsyncContext {
 public:
  typedef Key key_t;
  typedef Value value_t;

  ReadContext(uint64_t key)
    : key_{ key } {
  }

  /// Copy (and deep-copy) constructor.
  ReadContext(const ReadContext& other)
    : key_{ other.key_ } {
  }

  /// The implicit and explicit interfaces require a key() accessor.
  inline const Key& key() const {
    return key_;
  }

  inline void Get(const value_t& value) {
    uint64_t volatile *p = (uint64_t volatile *) &value_.value_;
    *p = value.value_;
  }

  inline void GetAtomic(const value_t& value) {
    uint64_t v = value.atomic_value_.load();
    value_.atomic_value_.store(v);
  }

 protected:
  /// The explicit interface requires a DeepCopy_Internal() implementation.
  Status DeepCopy_Internal(IAsyncContext*& context_copy) {
    return IAsyncContext::DeepCopy_Internal(*this, context_copy);
  }

 private:
  Key key_;
  Value value_;
};

/// Class passed to store_t::Upsert().
class UpsertContext : public IAsyncContext {
 public:
  typedef Key key_t;
  typedef Value value_t;

  UpsertContext(uint64_t key, uint64_t input)
    : key_{ key }
    , input_{ input } {
  }

  /// Copy (and deep-copy) constructor.
  UpsertContext(const UpsertContext& other)
    : key_{ other.key_ }
    , input_{ other.input_ } {
  }

  /// The implicit and explicit interfaces require a key() accessor.
  inline const Key& key() const {
    return key_;
  }
  inline static constexpr uint32_t value_size() {
    return sizeof(value_t);
  }

  /// Non-atomic and atomic Put() methods.
  inline void Put(value_t& value) {
    value.value_ = input_;
  }
  inline bool PutAtomic(value_t& value) {
    value.atomic_value_.store(input_);
    return true;
  }

 protected:
  /// The explicit interface requires a DeepCopy_Internal() implementation.
  Status DeepCopy_Internal(IAsyncContext*& context_copy) {
    return IAsyncContext::DeepCopy_Internal(*this, context_copy);
  }

 private:
  Key key_;
  uint64_t input_;
};

/// Class passed to store_t::RMW().
class RmwContext : public IAsyncContext {
 public:
  typedef Key key_t;
  typedef Value value_t;

  RmwContext(uint64_t key, uint64_t incr)
    : key_{ key }
    , incr_{ incr } {
  }

  /// Copy (and deep-copy) constructor.
  RmwContext(const RmwContext& other)
    : key_{ other.key_ }
    , incr_{ other.incr_ } {
  }

  /// The implicit and explicit interfaces require a key() accessor.
  const Key& key() const {
    return key_;
  }
  inline static constexpr uint32_t value_size() {
    return sizeof(value_t);
  }
  inline static constexpr uint32_t value_size(const value_t& old_value) {
    return sizeof(value_t);
  }

  /// Initial, non-atomic, and atomic RMW methods.
  inline void RmwInitial(value_t& value) {
    value.value_ = incr_;
  }
  inline void RmwCopy(const value_t& old_value, value_t& value) {
    value.value_ = old_value.value_ + incr_;
  }
  inline bool RmwAtomic(value_t& value) {
    value.atomic_value_.fetch_add(incr_);
    return true;
  }

 protected:
  /// The explicit interface requires a DeepCopy_Internal() implementation.
  Status DeepCopy_Internal(IAsyncContext*& context_copy) {
    return IAsyncContext::DeepCopy_Internal(*this, context_copy);
  }

 private:
  Key key_;
  uint64_t incr_;
};

#else

// Large value
class GenLock {
  public:
  GenLock()
    : control_{ 0 } {
  }
  GenLock(uint64_t control)
    : control_{ control } {
  }
  inline GenLock& operator=(const GenLock& other) {
    control_ = other.control_;
    return *this;
  }

  union {
      struct {
        uint64_t gen_number : 62;
        uint64_t locked : 1;
        uint64_t replaced : 1;
      };
      uint64_t control_;
    };
};
static_assert(sizeof(GenLock) == 8, "sizeof(GenLock) != 8");

class AtomicGenLock {
  public:
  AtomicGenLock()
    : control_{ 0 } {
  }
  AtomicGenLock(uint64_t control)
    : control_{ control } {
  }

  inline GenLock load() const {
    return GenLock{ control_.load() };
  }
  inline void store(GenLock desired) {
    control_.store(desired.control_);
  }

  inline bool try_lock(bool& replaced) {
    replaced = false;
    GenLock expected{ control_.load() };
    expected.locked = 0;
    expected.replaced = 0;
    GenLock desired{ expected.control_ };
    desired.locked = 1;

    if(control_.compare_exchange_strong(expected.control_, desired.control_)) {
      return true;
    }
    if(expected.replaced) {
      replaced = true;
    }
    return false;
  }
  inline void unlock(bool replaced) {
    if(!replaced) {
      // Just turn off "locked" bit and increase gen number.
      uint64_t sub_delta = ((uint64_t)1 << 62) - 1;
      control_.fetch_sub(sub_delta);
    } else {
      // Turn off "locked" bit, turn on "replaced" bit, and increase gen number
      uint64_t add_delta = ((uint64_t)1 << 63) - ((uint64_t)1 << 62) + 1;
      control_.fetch_add(add_delta);
    }
  }

  private:
  std::atomic<uint64_t> control_;
};
static_assert(sizeof(AtomicGenLock) == 8, "sizeof(AtomicGenLock) != 8");

class Value {
  public:
  Value()
    : gen_lock_{ 0 }
    , size_{ 0 }
    , length_{ 0 } {
  }

  inline uint32_t size() const {
    return size_;
  }

  friend class RmwContext;
  friend class UpsertContext;
  friend class ReadContext;

  private:
  AtomicGenLock gen_lock_;
  uint32_t size_;
  uint32_t length_;

  inline const int8_t* buffer() const {
    return reinterpret_cast<const int8_t*>(this + 1);
  }
  inline int8_t* buffer() {
    return reinterpret_cast<int8_t*>(this + 1);
  }
};

class RmwContext : public IAsyncContext {
  public:
  typedef Key key_t;
  typedef Value value_t;

  RmwContext(uint64_t key, int8_t incr, uint32_t length)
    : key_{ key }
    , incr_{ incr }
    , length_{ length } {
  }

  /// Copy (and deep-copy) constructor.
  RmwContext(const RmwContext& other)
    : key_{ other.key_ }
    , incr_{ other.incr_ }
    , length_{ other.length_ } {
  }

  /// The implicit and explicit interfaces require a key() accessor.
  inline const Key& key() const {
    return key_;
  }
  inline uint32_t value_size() const {
    return sizeof(value_t) + length_;
  }
  inline uint32_t value_size(const Value& old_value) const {
    return sizeof(value_t) + length_;
  }

  inline void RmwInitial(Value& value) {
    value.gen_lock_.store(GenLock{});
    value.size_ = sizeof(Value) + length_;
    value.length_ = length_;
    std::memset(value.buffer(), incr_, length_);
  }
  inline void RmwCopy(const Value& old_value, Value& value) {
    value.gen_lock_.store(GenLock{});
    value.size_ = sizeof(Value) + length_;
    value.length_ = length_;
    for(uint32_t idx = 0; idx < std::min(old_value.length_, length_); ++idx) {
      value.buffer()[idx] = old_value.buffer()[idx] + incr_;
    }
  }
  inline bool RmwAtomic(Value& value) {
    bool replaced;
    while(!value.gen_lock_.try_lock(replaced) && !replaced) {
      std::this_thread::yield();
    }
    if(replaced) {
      // Some other thread replaced this record.
      return false;
    }
    if(value.size_ < sizeof(Value) + length_) {
      // Current value is too small for in-place update.
      value.gen_lock_.unlock(true);
      return false;
    }
    // In-place update overwrites length and buffer, but not size.
    value.length_ = length_;
    for(uint32_t idx = 0; idx < length_; ++idx) {
      value.buffer()[idx] += incr_;
    }
    value.gen_lock_.unlock(false);
    return true;
  }

  protected:
  /// The explicit interface requires a DeepCopy_Internal() implementation.
  Status DeepCopy_Internal(IAsyncContext*& context_copy) {
    return IAsyncContext::DeepCopy_Internal(*this, context_copy);
  }

  private:
  int8_t incr_;
  uint32_t length_;
  Key key_;
};

#define CACHELINE_SIZE (64)
alignas(CACHELINE_SIZE) size_t rand_offsets[128] = {0};
void memcpy_rand_byte(char* output_arr, const char* input_arr, size_t length, size_t thread_idx) {
    size_t *offset_p = &rand_offsets[thread_idx];
    if (*offset_p >= length) {
        *offset_p = *offset_p % length;
    }
    output_arr[*offset_p] = input_arr[*offset_p];
    *offset_p += 1;
}

class ReadContext : public IAsyncContext {
  public:
  typedef Key key_t;
  typedef Value value_t;

  ReadContext(uint64_t key, size_t thread_idx)
    : key_{ key }
    , output_length{ 0 }
    , thread_idx_{ thread_idx } {
  }

  /// Copy (and deep-copy) constructor.
  ReadContext(const ReadContext& other)
    : key_{ other.key_ }
    , output_length{ 0 } {
  }

  /// The implicit and explicit interfaces require a key() accessor.
  inline const Key& key() const {
    return key_;
  }

  inline void Get(const Value& value) {
    // All reads should be atomic (from the mutable tail).
    BUG_ON(true);
  }
  inline void GetAtomic(const Value& value) {
    GenLock before, after;
    do {
      before = value.gen_lock_.load();
      output_length = value.length_;
      memcpy(output_arr, value.buffer(), output_length);
      // memcpy_rand_byte((char*)output_arr, (const char*)value.buffer(), output_length, thread_idx_);
      after = value.gen_lock_.load();
    } while(before.gen_number != after.gen_number);
  }

  protected:
  /// The explicit interface requires a DeepCopy_Internal() implementation.
  Status DeepCopy_Internal(IAsyncContext*& context_copy) {
    return IAsyncContext::DeepCopy_Internal(*this, context_copy);
  }

  private:
  Key key_;
  size_t thread_idx_;
  public:
  uint64_t output_length;
  uint64_t output_arr[VALUE_NUM_UINT64];
  // uint64_t output_arr[8]; // tmp
};

class UpsertContext : public IAsyncContext {
  public:
  typedef Key key_t;
  typedef Value value_t;

  UpsertContext(uint64_t key, uint32_t length)
    : key_{ key }
    , length_{ length } {
  }

  /// Copy (and deep-copy) constructor.
  UpsertContext(const UpsertContext& other)
    : key_{ other.key_ }
    , length_{ other.length_ } {
  }

  /// The implicit and explicit interfaces require a key() accessor.
  inline const Key& key() const {
    return key_;
  }
  inline uint32_t value_size() const {
    return sizeof(Value) + length_;
  }
  /// Non-atomic and atomic Put() methods.
  inline void Put(Value& value) {
    value.gen_lock_.store(0);
    value.size_ = sizeof(Value) + length_;
    value.length_ = length_;
    std::memset(value.buffer(), 88, length_);
  }
  inline bool PutAtomic(Value& value) {
    bool replaced;
    while(!value.gen_lock_.try_lock(replaced) && !replaced) {
      std::this_thread::yield();
    }
    if(replaced) {
      // Some other thread replaced this record.
      return false;
    }
    if(value.size_ < sizeof(Value) + length_) {
      // Current value is too small for in-place update.
      value.gen_lock_.unlock(true);
      return false;
    }
    // In-place update overwrites length and buffer, but not size.
    value.length_ = length_;
    std::memset(value.buffer(), 88, length_);
    value.gen_lock_.unlock(false);
    return true;
  }

  protected:
  /// The explicit interface requires a DeepCopy_Internal() implementation.
  Status DeepCopy_Internal(IAsyncContext*& context_copy) {
    return IAsyncContext::DeepCopy_Internal(*this, context_copy);
  }

  private:
  Key key_;
  uint32_t length_;
};
#endif

/// Key-value store, specialized to our key and value types.
typedef FASTER::device::NullDisk disk_t;
using store_t = FasterKv<Key, Value, disk_t>;

inline Op ycsb_a_50_50(std::mt19937_64& rng) {
  if(rng() % 100 < 50) {
    return Op::Read;
  } else {
    return Op::Upsert;
  }
}

inline Op ycsb_b_95_5(std::mt19937_64& rng) {
  if(rng() % 100 < 95) {
    return Op::Read;
  } else {
    return Op::Upsert;
  }
  return Op::ReadModifyWrite;
}

inline Op ycsb_c_100(std::mt19937_64& rng) {
  return Op::Read;
}

inline Op ycsb_f_50_rmw_50(std::mt19937_64& rng) {
  if(rng() % 100 < 50) {
    return Op::Read;
  } else {
    return Op::ReadModifyWrite;
  }
}

void SetThreadAffinity(size_t core) {
  cpu_set_t mask;
  CPU_ZERO(&mask);
  CPU_SET(core, &mask);
  ::sched_setaffinity(0, sizeof(mask), &mask);
}

atomic<uint64_t> *ranks;

uint64_t fnv1_64_hash(uint64_t value) {
    return ranks[value];
}

/*
uint64_t fnv1_64_hash_real(uint64_t value) {
  uint64_t hash = 14695981039346656037ul;
  uint8_t *p = (uint8_t *) &value;
  for (uint64_t i = 0; i < sizeof(uint64_t); ++i, ++p) {
    hash *= 1099511628211ul;
    hash ^= *p;
  }
  return hash;
}
*/


void init_zipfian_ctxt() {
  zipfian_ctxt_.zetan = 0;
  for (uint64_t i = 1; i < num_records_nodead_ + 1; ++i) {
    zipfian_ctxt_.zetan += 1.0 / (pow((double) i, zipfian_constant_));
  }
  zipfian_ctxt_.theta = zipfian_constant_;
  zipfian_ctxt_.zeta2theta = 0;
  for (uint64_t i = 1; i < 3; ++i) {
    zipfian_ctxt_.zeta2theta += 1.0 / (pow((double) i, zipfian_constant_));
  }
  zipfian_ctxt_.alpha = 1.0 / (1.0 - zipfian_ctxt_.theta);
  zipfian_ctxt_.eta = (1 - pow(2.0 / (double) num_records_nodead_, 1 - zipfian_ctxt_.theta))
    / (1 - (zipfian_ctxt_.zeta2theta / zipfian_ctxt_.zetan));

  zipfian_ctxt_.theta2 = 1 + pow(0.5, zipfian_ctxt_.theta);

}

void init_ranks() {
  // init ranks
  ranks = new std::atomic<uint64_t>[num_records_];
  std::vector<uint64_t> ranks_init;
  for (uint64_t i = 0; i < num_records_; i++) {
      ranks_init.push_back(i);
  }
  std::mt19937 g{123};
  std::shuffle(ranks_init.begin(), ranks_init.end(), g);
  for (uint64_t i = 0; i < num_records_; i++) {
    //ranks[i] = fnv1_64_hash_real(i) % num_records_;
    ranks[i] = ranks_init[i];
  }

}

//uint64_t next_zipfian(mt19937_64 &rand_eng, uniform_real_distribution<double> &dist, uint64_t phase) {
uint64_t next_8020(mt19937_64 &rand_eng, uniform_real_distribution<double> &dist) {
    double u = dist(rand_eng);
    if (u < 0.8) {
        // access the hot set
        double u_to_1 = u / 0.8;
        double u_to_set = u_to_1 * (num_records_nodead_ * 0.2);
        return ranks[(uint64_t)u_to_set];
    } else {
        // access the cold set
        double u_to_1 = (u - 0.8) / 0.2;
        double u_to_set = u_to_1 * (num_records_nodead_ * 0.8) + num_records_nodead_ * 0.2;
        return ranks[(uint64_t)u_to_set];
    }
}

uint64_t next_zipfian(mt19937_64 &rand_eng, uniform_real_distribution<double> &dist) {
  double u = dist(rand_eng);
  double uz = u * zipfian_ctxt_.zetan;
  uint64_t ret;
  if (uz < 1) {
    ret = 0;
  } else if (uz < zipfian_ctxt_.theta2) {
    ret = 1;
  } else {
    ret = (uint64_t) ((double)num_records_nodead_ * pow(zipfian_ctxt_.eta * u - zipfian_ctxt_.eta + 1, zipfian_ctxt_.alpha));
  }
  //ret = fnv1_64_hash(ret + num_records_nodead_ * phase) % num_records_nodead_;
  //ret = fnv1_64_hash(ret) % num_records_nodead_;
  ret = ranks[ret];
  return ret;
}

/*
uint64_t next_uniform(mt19937_64 &rand_eng, uniform_int_distribution<uint64_t> &dist) {
  return fnv1_64_hash(dist(rand_eng));
}
*/

template <Op(*FN)(std::mt19937_64&)>
void thread_warmup_store(store_t* store, size_t thread_idx, uint64_t num_ops) {
  mt19937_64 rand_eng{thread_idx + 0xBEEF};
	uniform_real_distribution<double> uniform_real_dist(0, 1);
	uniform_int_distribution<uint64_t> uniform_int_dist(0, num_records_nodead_ - 1);

  Guid guid = store->StartSession();

  for (uint64_t idx = 0; idx < num_ops; ++idx) {
    if(idx % kRefreshInterval == 0) {
      store->Refresh();
      if(idx % kCompletePendingInterval == 0) {
        store->CompletePending(false);
      }
    }
    uint64_t key;
    if (zipfian_constant_ > 0)
      //key = next_zipfian(rand_eng, uniform_real_dist, 0);
      key = next_zipfian(rand_eng, uniform_real_dist);
    else
      //key = next_uniform(rand_eng, uniform_int_dist);
      key = next_8020(rand_eng, uniform_real_dist);

    switch(FN(rand_eng)) {
    case Op::Insert:
    case Op::Upsert: {
      auto callback = [](IAsyncContext* ctxt, Status result) {
        CallbackContext<UpsertContext> context{ ctxt };
      };
#if VALUE_NUM_UINT64 == 1
      UpsertContext context{ key, 0 };
#else
      UpsertContext context{ key, VALUE_SIZE };
#endif
      Status result = store->Upsert(context, callback, 1);
      break;
    }
    case Op::Scan:
      printf("Scan currently not supported!\n");
      exit(1);
      break;
    case Op::Read: {
      auto callback = [](IAsyncContext* ctxt, Status result) {
        CallbackContext<ReadContext> context{ ctxt };
      };

      ReadContext context{ key , thread_idx };

      Status result = store->Read(context, callback, 1);
      break;
    }
    case Op::ReadModifyWrite:
      auto callback = [](IAsyncContext* ctxt, Status result) {
        CallbackContext<RmwContext> context{ ctxt };
      };
#if VALUE_NUM_UINT64 == 1
      RmwContext context{ key, 5 };
#else
      RmwContext context{ key, 5, VALUE_SIZE };
#endif
      Status result = store->Rmw(context, callback, 1);
      if(result == Status::Ok) {
      }
      break;
    }
  }

  store->CompletePending(true);
  store->StopSession();
}

template <Op(*FN)(std::mt19937_64&)>
void warmup_store(store_t* store, size_t num_threads) {
  std::deque<std::thread> threads;
  for(size_t thread_idx = 0; thread_idx < num_threads; ++thread_idx) {
    threads.emplace_back(&thread_warmup_store<FN>, store, thread_idx, num_warmup_ops_ / num_threads);
  }
  for(auto& thread : threads) {
    thread.join();
  }

  printf("Finished warming-up store.\n");
}

void thread_setup_store(store_t* store, size_t thread_idx, uint64_t start_idx, uint64_t end_idx) {
  auto callback = [](IAsyncContext* ctxt, Status result) {
    assert(result == Status::Ok);
  };
  Guid guid = store->StartSession();

  uint64_t value = 42;  // arbitrary choice
  for (uint64_t i = start_idx; i < end_idx; ++i) {
    if(i % kRefreshInterval == 0) {
      store->Refresh();
      if(i % kCompletePendingInterval == 0) {
        store->CompletePending(false);
      }
    }
#if VALUE_NUM_UINT64 == 1
    UpsertContext context{ i, value };
#else
    UpsertContext context{ i, VALUE_SIZE };
#endif
    store->Upsert(context, callback, 1);
  }

  store->CompletePending(true);
  store->StopSession();
}

void setup_store(store_t* store, size_t num_threads) {
  std::deque<std::thread> threads;
  uint64_t num_records_per_thread = (num_records_ + num_threads - 1) / num_threads;
  for(size_t thread_idx = 0; thread_idx < num_threads; ++thread_idx) {
    threads.emplace_back(&thread_setup_store, store, thread_idx,
                         thread_idx * num_records_per_thread,
                         std::min((thread_idx + 1) * num_records_per_thread, num_records_));
  }
  for(auto& thread : threads) {
    thread.join();
  }

  printf("Finished populating store: contains %ld elements.\n", num_records_);

#ifdef USE_OPT
  uint64_t record_size = store->GetRecordSize(8, VALUE_SIZE);

  uint64_t ht_size, log_size;
  store->GetMemorySize(&ht_size, &log_size);
  printf("Hash table: %ld GB, log: %ld GB\n", ht_size >> 30UL, log_size >> 30UL);

  uint64_t ht_num_pages = (ht_size + OPT_PAGE_SIZE - 1) / OPT_PAGE_SIZE;
  uint64_t log_num_pages = (log_size + OPT_PAGE_SIZE - 1) / OPT_PAGE_SIZE;

  double *page_exp = (double *) malloc((ht_num_pages + log_num_pages) * sizeof(*page_exp));
  BUG_ON(page_exp == NULL);
  memset(page_exp, 0, (ht_num_pages + log_num_pages) * sizeof(*page_exp));

  printf("Calculating page-level distribution...\n");
  auto callback = [](IAsyncContext* ctxt, Status result) {
    CallbackContext<ReadContext> context{ ctxt };
  };
  for (uint64_t i = 0; i < num_records_; ++i) {
    // From hotest to coldest
    uint64_t key_index;
    double mass;
    // Don't need to normalize mass
    if (zipfian_constant_ > 0) {
      // Zipfian distribution
      mass = 1.0L / pow((double) (i + 1), zipfian_constant_);
      key_index = fnv1_64_hash(i) % num_records_;
      print("idk what this is and/or how to convert it to now use num_records_nodead_\n");
      exit(0);
    } else {
      // Uniform distribution
      mass = 1.0L;
      key_index = i;
    }
    ReadContext context{ key_index , 0};

    uint64_t ht_addr, log_addr;
    store->GetAddr(context, callback, &ht_addr, &log_addr);
    uint64_t ht_page = ht_addr / OPT_PAGE_SIZE;
    page_exp[ht_page] += mass;

    for (uint64_t j = log_addr / OPT_PAGE_SIZE; 
         j <= (log_addr + record_size - 1) / OPT_PAGE_SIZE;
         ++j)
    {
      page_exp[ht_num_pages + j] += mass;
    }
    if (i % 100000 == 0) {
      printf("Progress: %ld/%ld (%.2f%%)\r", i + 1, num_records_,
             ((double) (100 * (i + 1))) / (double) num_records_);
    }
  }
  printf("Progress: %ld/%ld (100.00%%)\n", num_records_, num_records_);

  uint64_t *page_index = (uint64_t *) malloc((ht_num_pages + log_num_pages) * sizeof(*page_index));
  BUG_ON(page_index == NULL);

  std::iota(page_index, page_index + (ht_num_pages + log_num_pages), 0);
  std::stable_sort(page_index, page_index + (ht_num_pages + log_num_pages),
    [&](const uint64_t &i, const uint64_t &j){return page_exp[i] < page_exp[j];});

  BUG_ON(ht_num_pages + log_num_pages < 5);
  printf("Top 5 unnormalized expected number of accesses:");
  for (uint64_t i = 0; i < 5; ++i) {
    printf(" %f", page_exp[page_index[ht_num_pages + log_num_pages - 1 - i]]);
  }
  printf("\n");

  uint64_t num_dram_pages = (dram_size_ << 30UL) / OPT_PAGE_SIZE;
  if (num_dram_pages > ht_num_pages + log_num_pages) {
    num_dram_pages = ht_num_pages + log_num_pages;
  }
  uint64_t num_pmem_pages = ht_num_pages + log_num_pages - num_dram_pages;
  printf("Number of DRAM pages: %ld, number of PMEM pages: %ld\n",
    num_dram_pages, num_pmem_pages);

  for (uint64_t i = 0; i < num_pmem_pages; ++i) {
    uint64_t cur_page_index = page_index[i];
    if (cur_page_index < ht_num_pages) {
      // Hash table page
      store->MigrateHashTable(cur_page_index * OPT_PAGE_SIZE, OPT_PAGE_SIZE, OPT_PMEM_NUMA);
    } else {
      // Log page
      cur_page_index -= ht_num_pages;
      store->MigrateLog(cur_page_index * OPT_PAGE_SIZE, OPT_PAGE_SIZE, OPT_PMEM_NUMA);
    }
  }

  free(page_index);
  free(page_exp);
  printf("Finished migrating pages\n");
#endif
}

aligned_atomic<uint64_t> counts[1024];
aligned_atomic<bool> do_zipfian[1024];
aligned_atomic<uint64_t> zipfian_phase[1024];
aligned_atomic<int> phase_idx[1024];

template <Op(*FN)(std::mt19937_64&)>
void thread_run_benchmark(store_t* store, size_t thread_idx, uint64_t num_ops) {
  mt19937_64 rand_eng{thread_idx};
	uniform_real_distribution<double> uniform_real_dist(0, 1);
	uniform_int_distribution<uint64_t> uniform_int_dist(0, num_records_nodead_ - 1);
	uniform_int_distribution<uint64_t> uniform_int_dist1(0, num_records_nodead_/4 - 1);
	uniform_int_distribution<uint64_t> uniform_int_dist2(num_records_nodead_/4, 2*num_records_nodead_/4 - 1);
	uniform_int_distribution<uint64_t> uniform_int_dist3(2*num_records_nodead_/4, 3*num_records_nodead_/4 - 1);
	uniform_int_distribution<uint64_t> uniform_int_dist4(3*num_records_nodead_/4, num_records_nodead_ - 1);

  auto start_time = std::chrono::high_resolution_clock::now();

  uint64_t upsert_value = 0;
  int64_t reads_done = 0;
  int64_t writes_done = 0;

  Guid guid = store->StartSession();

  for (uint64_t idx = 0; idx < num_ops && running; ++idx) {
    if(idx % kRefreshInterval == 0) {
      store->Refresh();
      if(idx % kCompletePendingInterval == 0) {
        store->CompletePending(false);
      }
    }
    uint64_t key;
    if (zipfian_constant_ > 0 && do_zipfian[thread_idx])
      //key = next_zipfian(rand_eng, uniform_real_dist, zipfian_phase[thread_idx]);
      if (zipfian_phase[thread_idx] == 0) {
          key = next_zipfian(rand_eng, uniform_real_dist);
      } else {
          key = next_8020(rand_eng, uniform_real_dist);
      }
    else
      key = next_8020(rand_eng, uniform_real_dist);
      //key = next_uniform(rand_eng, uniform_int_dist);
    //if (phase_idx[thread_idx] == 0) key = next_uniform(rand_eng, uniform_int_dist1);
    //if (phase_idx[thread_idx] == 1) key = next_uniform(rand_eng, uniform_int_dist2);
    //if (phase_idx[thread_idx] == 2) key = next_uniform(rand_eng, uniform_int_dist3);
    //if (phase_idx[thread_idx] == 3) key = next_uniform(rand_eng, uniform_int_dist4);
    /*
    if (uniform_real_dist(rand_eng) < 0.9) {
        key = fnv1_64_hash(phase_idx[thread_idx]) % num_records_nodead_;
    } else {
        key = next_uniform(rand_eng, uniform_int_dist);
    }
    */

    switch(FN(rand_eng)) {
    case Op::Insert:
    case Op::Upsert: {
      auto callback = [](IAsyncContext* ctxt, Status result) {
        CallbackContext<UpsertContext> context{ ctxt };
      };
#if VALUE_NUM_UINT64 == 1
      UpsertContext context{ key, upsert_value };
#else
      UpsertContext context{ key, VALUE_SIZE };
#endif
      Status result = store->Upsert(context, callback, 1);
      ++writes_done;
      counts[thread_idx]++;
      break;
    }
    case Op::Scan:
      printf("Scan currently not supported!\n");
      exit(1);
      break;
    case Op::Read: {
      auto callback = [](IAsyncContext* ctxt, Status result) {
        CallbackContext<ReadContext> context{ ctxt };
      };

      ReadContext context{ key , thread_idx };

      Status result = store->Read(context, callback, 1);
      ++reads_done;
      counts[thread_idx]++;
      break;
    }
    case Op::ReadModifyWrite:
      auto callback = [](IAsyncContext* ctxt, Status result) {
        CallbackContext<RmwContext> context{ ctxt };
      };

#if VALUE_NUM_UINT64 == 1
      RmwContext context{ key, 5 };
#else
      RmwContext context{ key, 5, VALUE_SIZE };
#endif
      Status result = store->Rmw(context, callback, 1);
      if(result == Status::Ok) {
        ++writes_done;
        counts[thread_idx]++;
      }
      break;
    }
  }

  store->CompletePending(true);
  store->StopSession();

  auto end_time = std::chrono::high_resolution_clock::now();
  std::chrono::nanoseconds duration = end_time - start_time;
  total_duration_ += duration.count();
  total_reads_done_ += reads_done;
  total_writes_done_ += writes_done;
  printf("Finished thread %" PRIu64 " : %" PRIu64 " reads, %" PRIu64 " writes, in %.2f seconds.\n",
         thread_idx, reads_done, writes_done, (double)duration.count() / kNanosPerSecond);
}

void do_churn(mt19937_64 &rand_eng, uniform_int_distribution<size_t> &dist, uniform_int_distribution<size_t> &dist_nodead) {
    size_t idx1 = dist_nodead(rand_eng);
    size_t idx2 = dist_nodead(rand_eng);
    // non atomic swap, small race condition here lol
    uint64_t val1 = ranks[idx1];
    uint64_t val2 = ranks[idx2];
    ranks[idx1] = val2;
    ranks[idx2] = val1;
}

template <Op(*FN)(std::mt19937_64&)>
void run_benchmark(store_t* store, size_t num_threads) {
  total_duration_ = 0;
  total_reads_done_ = 0;
  total_writes_done_ = 0;
  std::deque<std::thread> threads;
  int num_phases = 1;
  //int num_phases = 4;
  //int phases[num_phases] = {0, -1, 0, 1, -1, 2};
  //int phases[num_phases] = {0,1,2,3,4,5};
  int phases[num_phases] = {0}; // DO WEIRD DISTRO OR NOT
  int current_phase_idx = 0;
  for(size_t thread_idx = 0; thread_idx < num_threads; ++thread_idx) {
      do_zipfian[thread_idx] = phases[current_phase_idx] >= 0;
      zipfian_phase[thread_idx] = phases[current_phase_idx];
      phase_idx[thread_idx] = current_phase_idx;
    threads.emplace_back(&thread_run_benchmark<FN>, store, thread_idx, num_ops_ / num_threads);
  }

  auto start_time = std::chrono::system_clock::now();
  //auto end_time = start_time + std::chrono::seconds(max_run_time);
  auto max_duration = std::chrono::seconds(max_run_time);
  auto next_phase_change = std::chrono::seconds(max_run_time * (current_phase_idx+1) / num_phases);
  //uint64_t churn_per_hour = 1000000000;
//#include "cph.h"
  //uint64_t churn_per_hour = CHURNS_PER_HOUR; / TODO
  //uint64_t churn_per_sec = churn_per_hour / 3600;
  //uint64_t churn_per_sec_real = churn_per_sec == 0 ? 1 : churn_per_sec;
  //uint64_t churn_every = churn_per_sec == 0 ? 3600 / churn_per_hour : 1;
  //printf("Churn: %ld every %ld seconds\n", churn_per_sec_real, churn_every);
  //uint64_t current_churn_idx = 0;
  //uint64_t num_churn_events = max_run_time / churn_every;
  //if (num_churn_events == 0) num_churn_events = 1;
  //auto next_churn = std::chrono::seconds(max_run_time * (current_churn_idx+1) / num_churn_events);
#ifdef NDEBUG
  printf("Running in Release mode\n");
#else
  printf("Running in Debug mode\n");
#endif
  mt19937_64 rand_eng{12345678};
  uniform_int_distribution<size_t> uniform_int_dist(0, num_records_ - 1);
  uniform_int_distribution<size_t> uniform_int_dist_nodead(0, num_records_nodead_ - 1);
  uint64_t churn_last = 0;
  auto target_epoch_time = std::chrono::seconds(1); // DO NOT CHANGE WITHOUT SEEING LINE BELOW
  int64_t churn_activation_period_epoch = churn_activation_period_sec_; // TODO actually convert this!
  int64_t churn_every = churn_activation_period_epoch;
  uint64_t churn_ctr = 0;
  int64_t sleep_debt = 0;
  if (max_run_time > 0) {
    while (1) {
        //std::this_thread::sleep_for(std::chrono::seconds(max_run_time));
        auto cur_time = std::chrono::system_clock::now();

        auto elapsed = cur_time - start_time;
        if (elapsed >= max_duration) {
            printf("Reached maximum run time of %ld seconds, shutting down...\n", max_run_time);
            running = false;
            break;
        } else {
            uint64_t tot_cnt = 0;
            for (size_t i = 0; i < num_threads; i++) tot_cnt += counts[i];
            auto elapsed_us = std::chrono::duration_cast<std::chrono::microseconds>(elapsed);
            printf("At time %f, total count %ld, phase[%d]=%d\n", elapsed_us.count()/1000000., tot_cnt, current_phase_idx, phases[current_phase_idx]);
            //std::cout << "At time " << std::put_time(&bt, "%H:%M:%S") << "." << std::setfill('0') << std::setw(3) << ms.count() << std::endl;
            /*
            if (elapsed >= next_phase_change) {
                current_phase_idx++;
                next_phase_change = std::chrono::seconds(max_run_time * (current_phase_idx+1) / num_phases);
                for (size_t i = 0; i < num_threads; i++) {
                    do_zipfian[i] = phases[current_phase_idx] >= 0;
                    zipfian_phase[i] = phases[current_phase_idx];
                    phase_idx[i] = current_phase_idx;
                }
            }
            */
            /*
            if (elapsed >= next_churn) {
                current_churn_idx++;
                next_churn = std::chrono::seconds(max_run_time * (current_churn_idx+1) / num_churn_events);
                printf("Churning: %lu\n", churn_per_sec_real);
                for (size_t _i = 0; _i < churn_per_sec_real; _i++) {
                    do_churn(rand_eng, uniform_int_dist);
                }
            }
            */
            if (churn_ctr == churn_every - 1) {
                if (sleep_debt != 0) {
                    printf("ERROR: churn is taking too long. turn it down!\n");
                    running = false;
                    break;
                }
                if (churn_per_hour_ == -1 || churn_per_hour_ == -2) {
                    // full shuffle
                    printf("Shuffling...\n");
                    //uint64_t size = num_records_;
                    uint64_t size = churn_per_hour_ == -1 ? num_records_nodead_ : num_records_;
                    std::shuffle(ranks, ranks + size, rand_eng);
                } else {
                    // continuous churn
                    int64_t churn_trgt = churn_per_hour_ * std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count() / 1000 / 60 / 60;
                    int64_t churn_epoch = churn_trgt - churn_last;
                    churn_last = churn_trgt;
                    if (churn_epoch != 0) {
                        printf("Churning: %lu\n", churn_epoch);
                        for (int64_t _i = 0; _i < churn_epoch; _i++) {
                            do_churn(rand_eng, uniform_int_dist, uniform_int_dist_nodead);
                        }
                    }
                }
                churn_ctr = 0;
            } else {
                churn_ctr++;
            }
        }
        auto policy_time_taken = std::chrono::system_clock::now() - cur_time;
        auto sleep_amt_ = target_epoch_time - policy_time_taken;
        int64_t sleep_amt = std::chrono::duration_cast<std::chrono::microseconds>(sleep_amt_).count();
        if (sleep_amt > 0) {
            if (sleep_debt == 0) {
                // sleep
                usleep(sleep_amt);
            } else {
                // resolve sleep debt
                sleep_debt -= sleep_amt;
                if (sleep_debt < 0) sleep_debt = 0;
            }
        } else {
            sleep_debt += -sleep_amt;
        }
        /*
        if (policy_time_taken > target_epoch_time) {
            printf("ERROR: policy time is taking TOO long. turn down the churn!\n");
            running = false;
            break;
        }
        std::this_thread::sleep_for(target_epoch_time - policy_time_taken);
        */
    }
  }

  for(auto& thread : threads) {
    thread.join();
  }

  printf("Finished benchmark: %.2f ops/second/thread\n",
         ((double)total_reads_done_ + (double)total_writes_done_) / ((double)total_duration_ /
         kNanosPerSecond));
  fflush(stdout);
}

void run(Workload workload, size_t num_load_threads, size_t num_run_threads) {
  // FASTER store has a hash table with approx. kInitCount / 2 entries and a log of size 16 GB
  size_t init_size = next_power_of_two(num_records_ / 2);
  store_t store{
    /* hash table size */ init_size,
    /* log size */ 2199023255552UL,
    /* file name */ "",
  };

  printf("Populating the store...\n");
  auto setup_start_time = std::chrono::high_resolution_clock::now();
  setup_store(&store, num_load_threads);
  auto setup_end_time = std::chrono::high_resolution_clock::now();

  std::chrono::nanoseconds setup_duration = setup_end_time - setup_start_time;
  printf("Setup time: %.2f seconds.\n", (double) setup_duration.count() / kNanosPerSecond);

  store.DumpDistribution();

  auto warmup_start_time = std::chrono::high_resolution_clock::now();
  store.WarmUp();

  printf("Configuring distribution...\n");
  init_ranks();
  if (zipfian_constant_ > 0) {
    init_zipfian_ctxt();
    printf("Zipfian data:\n");
    /*
struct {
  double zetan;
  double theta;
  double zeta2theta;
  double alpha;
  double eta;
} zipfian_ctxt_;
*/
    printf("zetan: %f\n", zipfian_ctxt_.zetan);
    printf("theta: %f\n", zipfian_ctxt_.theta);
    printf("zeta2theta: %f\n", zipfian_ctxt_.zeta2theta);
    printf("alpha: %f\n", zipfian_ctxt_.alpha);
    printf("eta: %f\n", zipfian_ctxt_.eta);
  }

  printf("Warming up the store...\n");
  if (num_warmup_ops_ > 0) {
    switch(workload) {
    case Workload::A_50_50:
      warmup_store<ycsb_a_50_50>(&store, kNumWarmupThreads);
      break;
    case Workload::B_95_5:
      warmup_store<ycsb_b_95_5>(&store, kNumWarmupThreads);
      break;
    case Workload::C_100:
      warmup_store<ycsb_c_100>(&store, kNumWarmupThreads);
      break;
    case Workload::F_50_RMW_50:
      warmup_store<ycsb_f_50_rmw_50>(&store, kNumWarmupThreads);
      break;
    default:
      printf("Unknown workload!\n");
      exit(1);
    }
  }
  auto warmup_end_time = std::chrono::high_resolution_clock::now();
  std::chrono::nanoseconds warmup_duration = warmup_end_time - warmup_start_time;
  printf("Warmup time: %.2f seconds.\n", (double) warmup_duration.count() / kNanosPerSecond);

  printf("Running benchmark on %" PRIu64 " threads...\n", num_run_threads);
  printf("Value size in uint64_ts: %d\n", VALUE_NUM_UINT64);
  fflush(stdout);
  switch(workload) {
  case Workload::A_50_50:
    run_benchmark<ycsb_a_50_50>(&store, num_run_threads);
    break;
  case Workload::B_95_5:
    run_benchmark<ycsb_b_95_5>(&store, num_run_threads);
    break;
  case Workload::C_100:
    run_benchmark<ycsb_c_100>(&store, num_run_threads);
    break;
  case Workload::F_50_RMW_50:
    run_benchmark<ycsb_f_50_rmw_50>(&store, num_run_threads);
    break;
  default:
    printf("Unknown workload!\n");
    exit(1);
  }
}

int main(int argc, char* argv[]) {
  size_t kNumArgs = 11;
#ifdef USE_OPT
  kNumArgs++;
#endif
  if(argc != kNumArgs + 1) {
    printf("Usage: %s <workload> <# load threads> <# run threads> <zipfian constant> <# records> <# ops> <# warmup ops> <max run time> <# records not dead> <churn_per_hour> <churn_activation_period_sec>", argv[0]);
#ifdef USE_OPT
    printf(" <DRAM Size (GB)>");
#endif
    printf("\n");
    exit(0);
  }

  Workload workload = static_cast<Workload>(std::atol(argv[1]));
  size_t num_load_threads = std::atol(argv[2]);
  size_t num_run_threads = std::atol(argv[3]);
  zipfian_constant_ = std::atof(argv[4]);
  num_records_ = std::atol(argv[5]);
  num_ops_ = std::atol(argv[6]);
  num_warmup_ops_ = std::atol(argv[7]);
  max_run_time = std::atol(argv[8]);
  num_records_nodead_ = std::atol(argv[9]);
  churn_per_hour_ = std::atol(argv[10]);
  churn_activation_period_sec_ = std::atol(argv[11]);
#ifdef USE_OPT
  dram_size_ = std::atol(argv[12]);
#endif

  run(workload, num_load_threads, num_run_threads);

  return 0;
}


namespace std {
    /**
     * @brief Atomically swaps the underlying values of two std::atomic objects.
     */
    void swap(std::atomic<uint64_t>& a, std::atomic<uint64_t>& b) noexcept {
        // Atomically load the value of 'a'
        //uint64_t temp = a.load(std::memory_order_relaxed); 
        uint64_t temp = a.load(std::memory_order_seq_cst); 
        
        // Atomically store the value of 'b' into 'a'
        //a.store(b.load(std::memory_order_relaxed), std::memory_order_relaxed);
        a.store(b.load(std::memory_order_seq_cst), std::memory_order_seq_cst);
        
        // Atomically store the value of the original 'a' (held in temp) into 'b'
        //b.store(temp, std::memory_order_relaxed);
        b.store(temp, std::memory_order_seq_cst);
        
        // NOTE: This entire three-step swap is NOT a single atomic operation.
        // It consists of three atomic operations. This is a thread-safe way 
        // to swap two *independent* atomics.
    }
}

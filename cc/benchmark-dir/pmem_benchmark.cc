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
  RMW_100 = 1,
  C_100 = 2,
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
uint64_t num_ops_;
uint64_t num_warmup_ops_;
#ifdef USE_OPT
uint64_t dram_size_;  // GB
#endif

struct {
  double zetan;
  double theta;
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

/// This benchmark stores a fixed-size value in the key-value store.
class Value {
 public:
  Value() {
    for (uint64_t i = 0; i < VALUE_NUM_UINT64; ++i) {
      value_[i] = 0;
    }
  }

  Value(const Value& other) {
    for (uint64_t i = 0; i < VALUE_NUM_UINT64; ++i) {
      value_[i] = other.value_[i];
    }
  }

  Value(uint64_t value) {
    for (uint64_t i = 0; i < VALUE_NUM_UINT64; ++i) {
      value_[i] = value;
    }
  }

  inline static constexpr uint32_t size() {
    return static_cast<uint32_t>(sizeof(Value));
  }

  friend class ReadContext;
  friend class UpsertContext;
  friend class RmwContext;

 private:
  union {
    uint64_t value_[VALUE_NUM_UINT64];
    std::atomic<uint64_t> atomic_value_[VALUE_NUM_UINT64];
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

  // For this benchmark, we don't copy out, so these are no-ops.
  inline void Get(const value_t& value) {
    uint64_t volatile *p = (uint64_t volatile *) value_.value_;
    for (uint64_t i = 0; i < VALUE_NUM_UINT64; ++i) {
      p[i] = value.value_[i];
    }
  }

  inline void GetAtomic(const value_t& value) {
    if (VALUE_NUM_UINT64 == 1) {
      uint64_t v = value.atomic_value_[0].load();
      value_.atomic_value_[0].store(v);
      return;
    }
    uint64_t volatile *p = (uint64_t volatile *) value_.value_;
    for (uint64_t i = 0; i < VALUE_NUM_UINT64; ++i) {
      p[i] = value.value_[i];
    }
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
    for (uint64_t i = 0; i < VALUE_NUM_UINT64; ++i) {
      value.value_[i] = input_;
    }
  }
  inline bool PutAtomic(value_t& value) {
    if (VALUE_NUM_UINT64 == 1) {
      value.atomic_value_[0].store(input_);
      return true;
    }
    for (uint64_t i = 0; i < VALUE_NUM_UINT64; ++i) {
      value.value_[i] = input_;
    }
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
    for (uint64_t i = 0; i < VALUE_NUM_UINT64; ++i) {
      value.value_[i] = incr_;
    }
  }
  inline void RmwCopy(const value_t& old_value, value_t& value) {
    for (uint64_t i = 0; i < VALUE_NUM_UINT64; ++i) {
      value.value_[i] =  old_value.value_[i] + incr_;
    }
  }
  inline bool RmwAtomic(value_t& value) {
    if (VALUE_NUM_UINT64 == 1) {
      value.atomic_value_[0].fetch_add(incr_);
      return true;
    }
    for (uint64_t i = 0; i < VALUE_NUM_UINT64; ++i) {
      value.value_[i] += incr_;
    }
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

inline Op ycsb_rmw_100(std::mt19937_64& rng) {
  return Op::ReadModifyWrite;
}

inline Op ycsb_c_100(std::mt19937_64& rng) {
  return Op::Read;
}

void SetThreadAffinity(size_t core) {
  cpu_set_t mask;
  CPU_ZERO(&mask);
  CPU_SET(core, &mask);
  ::sched_setaffinity(0, sizeof(mask), &mask);
}

uint64_t fnv1_64_hash(uint64_t value) {
  uint64_t hash = 14695981039346656037ul;
  uint8_t *p = (uint8_t *) &value;
  for (uint64_t i = 0; i < sizeof(uint64_t); ++i, ++p) {
    hash *= 1099511628211ul;
    hash ^= *p;
  }
  return hash;
}

void init_zipfian_ctxt() {
  zipfian_ctxt_.zetan = 0;
  for (uint64_t i = 1; i < num_records_ + 1; ++i) {
    zipfian_ctxt_.zetan += 1.0 / (pow((double) i, zipfian_constant_));
  }
  zipfian_ctxt_.theta = zipfian_constant_;
  zipfian_ctxt_.zeta2theta = 0;
  for (uint64_t i = 1; i < 3; ++i) {
    zipfian_ctxt_.zeta2theta += 1.0 / (pow((double) i, zipfian_constant_));
  }
  zipfian_ctxt_.alpha = 1.0 / (1.0 - zipfian_ctxt_.theta);
  zipfian_ctxt_.eta = (1 - pow(2.0 / (double) num_records_, 1 - zipfian_ctxt_.theta))
    / (1 - (zipfian_ctxt_.zeta2theta / zipfian_ctxt_.zetan));
}

uint64_t next_zipfian(mt19937_64 &rand_eng, uniform_real_distribution<double> &dist) {
  double u = dist(rand_eng);
  double uz = u * zipfian_ctxt_.zetan;
  uint64_t ret;
  if (uz < 1) {
    ret = 0;
  } else if (uz < 1 + pow(0.5, zipfian_ctxt_.theta)) {
    ret = 1;
  } else {
    ret = (uint64_t) ((double)num_records_ * pow(zipfian_ctxt_.eta * u - zipfian_ctxt_.eta + 1, zipfian_ctxt_.alpha));
  }
  ret = fnv1_64_hash(ret) % num_records_;
  return ret;
}

uint64_t next_uniform(mt19937_64 &rand_eng, uniform_int_distribution<uint64_t> &dist) {
  return dist(rand_eng);
}

template <Op(*FN)(std::mt19937_64&)>
void thread_warmup_store(store_t* store, size_t thread_idx, uint64_t num_ops) {
  SetThreadAffinity(thread_idx);

  mt19937_64 rand_eng{thread_idx + 0xBEEF};
	uniform_real_distribution<double> uniform_real_dist(0, 1);
	uniform_int_distribution<uint64_t> uniform_int_dist(0, num_records_ - 1);

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
      key = next_zipfian(rand_eng, uniform_real_dist);
    else
      key = next_uniform(rand_eng, uniform_int_dist);

    switch(FN(rand_eng)) {
    case Op::Insert:
    case Op::Upsert: {
      auto callback = [](IAsyncContext* ctxt, Status result) {
        CallbackContext<UpsertContext> context{ ctxt };
      };

      UpsertContext context{ key, 0 };
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

      ReadContext context{ key };

      Status result = store->Read(context, callback, 1);
      break;
    }
    case Op::ReadModifyWrite:
      auto callback = [](IAsyncContext* ctxt, Status result) {
        CallbackContext<RmwContext> context{ ctxt };
      };

      RmwContext context{ key, 5 };
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

  SetThreadAffinity(thread_idx);
  Guid guid = store->StartSession();

  uint64_t value = 42;  // arbitrary choice
  for (uint64_t i = start_idx; i < end_idx; ++i) {
    if(i % kRefreshInterval == 0) {
      store->Refresh();
      if(i % kCompletePendingInterval == 0) {
        store->CompletePending(false);
      }
    }
    UpsertContext context{ i, value };
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
    } else {
      // Uniform distribution
      mass = 1.0L;
      key_index = i;
    }
    ReadContext context{ key_index };

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

template <Op(*FN)(std::mt19937_64&)>
void thread_run_benchmark(store_t* store, size_t thread_idx) {
  SetThreadAffinity(thread_idx);

  mt19937_64 rand_eng{thread_idx};
	uniform_real_distribution<double> uniform_real_dist(0, 1);
	uniform_int_distribution<uint64_t> uniform_int_dist(0, num_records_ - 1);

  auto start_time = std::chrono::high_resolution_clock::now();

  uint64_t upsert_value = 0;
  int64_t reads_done = 0;
  int64_t writes_done = 0;

  Guid guid = store->StartSession();

  uint64_t idx = 0;
  while (total_ops_done_.fetch_add(1) < num_ops_) {
    if(idx % kRefreshInterval == 0) {
      store->Refresh();
      if(idx % kCompletePendingInterval == 0) {
        store->CompletePending(false);
      }
    }
    idx++;
    uint64_t key;
    if (zipfian_constant_ > 0)
      key = next_zipfian(rand_eng, uniform_real_dist);
    else
      key = next_uniform(rand_eng, uniform_int_dist);

    switch(FN(rand_eng)) {
    case Op::Insert:
    case Op::Upsert: {
      auto callback = [](IAsyncContext* ctxt, Status result) {
        CallbackContext<UpsertContext> context{ ctxt };
      };

      UpsertContext context{ key, upsert_value };
      Status result = store->Upsert(context, callback, 1);
      ++writes_done;
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

      ReadContext context{ key };

      Status result = store->Read(context, callback, 1);
      ++reads_done;
      break;
    }
    case Op::ReadModifyWrite:
      auto callback = [](IAsyncContext* ctxt, Status result) {
        CallbackContext<RmwContext> context{ ctxt };
      };

      RmwContext context{ key, 5 };
      Status result = store->Rmw(context, callback, 1);
      if(result == Status::Ok) {
        ++writes_done;
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

template <Op(*FN)(std::mt19937_64&)>
void run_benchmark(store_t* store, size_t num_threads) {
  total_duration_ = 0;
  total_reads_done_ = 0;
  total_writes_done_ = 0;
  std::deque<std::thread> threads;
  for(size_t thread_idx = 0; thread_idx < num_threads; ++thread_idx) {
    threads.emplace_back(&thread_run_benchmark<FN>, store, thread_idx);
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
  if (zipfian_constant_ > 0)
    init_zipfian_ctxt();

  printf("Warming up the store...\n");
  if (num_warmup_ops_ > 0) {
    switch(workload) {
    case Workload::A_50_50:
      warmup_store<ycsb_a_50_50>(&store, kNumWarmupThreads);
      break;
    case Workload::RMW_100:
      warmup_store<ycsb_rmw_100>(&store, kNumWarmupThreads);
      break;
    case Workload::C_100:
      warmup_store<ycsb_c_100>(&store, kNumWarmupThreads);
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
  fflush(stdout);
  switch(workload) {
  case Workload::A_50_50:
    run_benchmark<ycsb_a_50_50>(&store, num_run_threads);
    break;
  case Workload::RMW_100:
    run_benchmark<ycsb_rmw_100>(&store, num_run_threads);
    break;
  case Workload::C_100:
    run_benchmark<ycsb_c_100>(&store, num_run_threads);
    break;
  default:
    printf("Unknown workload!\n");
    exit(1);
  }
}

int main(int argc, char* argv[]) {
  size_t kNumArgs = 7;
#ifdef USE_OPT
  kNumArgs++;
#endif
  if(argc != kNumArgs + 1) {
    printf("Usage: %s <workload> <# load threads> <# run threads> <zipfian constant> <# records> <# ops> <# warmup ops>", argv[0]);
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
#ifdef USE_OPT
  dram_size_ = std::atol(argv[8]);
#endif

  run(workload, num_load_threads, num_run_threads);

  return 0;
}

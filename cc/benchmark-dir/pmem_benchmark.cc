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

#include "file.h"

#include "core/auto_ptr.h"
#include "core/faster.h"
#include "device/null_disk.h"

using namespace std::chrono_literals;
using namespace FASTER::core;

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
};

static constexpr uint64_t kRefreshInterval = 64;
static constexpr uint64_t kCompletePendingInterval = 1600;

static_assert(kCompletePendingInterval % kRefreshInterval == 0,
              "kCompletePendingInterval % kRefreshInterval != 0");

static constexpr uint64_t kNanosPerSecond = 1000000000;
static constexpr uint64_t kMaxKey = 268435456;

double zipfian_constant_;
uint64_t num_records_;
uint64_t num_ops_;

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

/// This benchmark stores an 8-byte value in the key-value store.
class Value {
 public:
  Value()
    : value_{ 0 } {
  }

  Value(const Value& other)
    : value_{ other.value_ } {
  }

  Value(uint64_t value)
    : value_{ value } {
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

  // For this benchmark, we don't copy out, so these are no-ops.
  inline void Get(const value_t& value) { }
  inline void GetAtomic(const value_t& value) { }

 protected:
  /// The explicit interface requires a DeepCopy_Internal() implementation.
  Status DeepCopy_Internal(IAsyncContext*& context_copy) {
    return IAsyncContext::DeepCopy_Internal(*this, context_copy);
  }

 private:
  Key key_;
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

/// Key-value store, specialized to our key and value types.
typedef FASTER::device::NullDisk disk_t;
using store_t = FasterKv<Key, Value, disk_t>;

inline Op ycsb_a_50_50(std::mt19937& rng) {
  if(rng() % 100 < 50) {
    return Op::Read;
  } else {
    return Op::Upsert;
  }
}

inline Op ycsb_rmw_100(std::mt19937& rng) {
  return Op::ReadModifyWrite;
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

uint64_t index_to_key(uint64_t index) {
  return fnv1_64_hash(index) % kMaxKey;
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

double next_double(unsigned int *seedp) {
  return (((uint64_t) rand_r(seedp)) | (((uint64_t) rand_r(seedp)) << 32))
         / (((uint64_t) RAND_MAX) | (((uint64_t) RAND_MAX) << 32));
}

uint64_t next_zipfian(unsigned int *seedp) {
  double u = next_double(seedp);
  double uz = u * zipfian_ctxt_.zetan;
  uint64_t ret;
  if (uz < 1) {
    ret = 0;
  } else if (uz < pow(0.5, zipfian_ctxt_.theta)) {
    ret = 1;
  } else {
    ret = (uint64_t) ((double)num_records_ * pow(zipfian_ctxt_.eta * u - zipfian_ctxt_.eta + 1, zipfian_ctxt_.alpha));
  }
  ret = ret % num_records_;
  return index_to_key(ret);
}

uint64_t next_uniform(unsigned int *seedp) {
  uint64_t ret = ((uint64_t) rand_r(seedp)) | (((uint64_t) rand_r(seedp)) << 32);
  ret = ret % num_records_;
  return index_to_key(ret);
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
    UpsertContext context{ index_to_key(i), value };
    store->Upsert(context, callback, 1);
  }

  store->CompletePending(true);
  store->StopSession();
}

void setup_store(store_t* store, size_t num_threads) {
  auto callback = [](IAsyncContext* ctxt, Status result) {
    assert(result == Status::Ok);
  };

  std::deque<std::thread> threads;
  uint64_t num_records_per_thread = (num_records_ + num_threads - 1) / num_threads;
  for(size_t thread_idx = 0; thread_idx < num_threads; ++thread_idx) {
    threads.emplace_back(&thread_setup_store, store, thread_idx,
                         thread_idx * num_records_per_thread,
                         min((thread_idx + 1) * num_records_per_thread, num_records_));
  }
  for(auto& thread : threads) {
    thread.join();
  }

  printf("Finished populating store: contains %ld elements.\n", num_records_);
}

template <Op(*FN)(std::mt19937&)>
void thread_run_benchmark(store_t* store, size_t thread_idx) {
  SetThreadAffinity(thread_idx);

  std::random_device rd{};
  std::mt19937 rng{ rd() };
  unsigned int seed = (unsigned int) thread_idx;

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
      key = next_zipfian(&seed);
    else
      key = next_uniform(&seed);

    switch(FN(rng)) {
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

template <Op(*FN)(std::mt19937&)>
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
}

void run(Workload workload, size_t num_threads) {
  // FASTER store has a hash table with approx. kInitCount / 2 entries and a log of size 16 GB
  size_t init_size = next_power_of_two(num_records_ / 2);
  store_t store{
    /* hash table size */ init_size,
    /* log size */ 17179869184,
    /* file name */ "",
  };

  printf("Populating the store...\n");

  setup_store(&store, num_threads);

  store.DumpDistribution();

  printf("Configuring distribution...\n");
  if (zipfian_constant_ > 0)
    init_zipfian_ctxt();

  printf("Running benchmark on %" PRIu64 " threads...\n", num_threads);
  switch(workload) {
  case Workload::A_50_50:
    run_benchmark<ycsb_a_50_50>(&store, num_threads);
    break;
  case Workload::RMW_100:
    run_benchmark<ycsb_rmw_100>(&store, num_threads);
    break;
  default:
    printf("Unknown workload!\n");
    exit(1);
  }
}

int main(int argc, char* argv[]) {
  constexpr size_t kNumArgs = 5;
  if(argc != kNumArgs + 1) {
    printf("Usage: %s <workload> <# threads> <zipfian constant> <# records> <# ops>\n", argv[0]);
    exit(0);
  }

  Workload workload = static_cast<Workload>(std::atol(argv[1]));
  size_t num_threads = ::atol(argv[2]);
  zipfian_constant_ = ::atof(argv[3]);
  num_records_ = ::atol(argv[4]);
  num_ops_ = ::atol(argv[5]);

  run(workload, num_threads);

  return 0;
}

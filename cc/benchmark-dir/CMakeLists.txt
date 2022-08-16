set(BENCHMARK_HEADERS
  file.h
)

set(BENCHMARK_SOURCES
)

ADD_FASTER_BENCHMARK(benchmark)
ADD_FASTER_BENCHMARK(pmem_benchmark)
ADD_FASTER_BENCHMARK(shuffle_free_list)

add_executable(process_ycsb process_ycsb.cc)

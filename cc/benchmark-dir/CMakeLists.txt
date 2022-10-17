set(BENCHMARK_HEADERS
  file.h
)

set(BENCHMARK_SOURCES
)

ADD_FASTER_BENCHMARK(benchmark)
ADD_FASTER_BENCHMARK(pmem_benchmark)
ADD_FASTER_BENCHMARK(color_remap_pmem_benchmark)
ADD_FASTER_BENCHMARK(shuffle_free_list)
ADD_FASTER_BENCHMARK(get_shm_value)
ADD_FASTER_BENCHMARK(set_shm_value)
ADD_FASTER_BENCHMARK(merge_miss)

add_executable(process_ycsb process_ycsb.cc)

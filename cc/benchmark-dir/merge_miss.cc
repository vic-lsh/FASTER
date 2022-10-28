#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <cstdlib>
#include <cstring>
#include <csignal>
#include <cmath>
#include <thread>
#include <atomic>
#include <random>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>
#include <algorithm>
#include <fcntl.h>
#include <sched.h>
#include <numa.h>
#include <unistd.h>
#include <sys/mman.h>

using namespace std;

#define FLOATING_POINT

#define BUG_ON(cond)    \
  do {                  \
    if (cond) {         \
      fprintf(stdout, "BUG_ON: %s (L%d) %s\n", __FILE__, __LINE__, __FUNCTION__); \
      raise(SIGABRT);   \
    }                   \
  } while (0)

struct entry {
  uint64_t index;
#ifdef FLOATING_POINT
  double num_l3_misses;
  double num_dram_misses;
#else
  uint64_t num_l3_misses;
  uint64_t num_dram_misses;
#endif
};


int main(int argc, char *argv[]) {
  if (argc != 4) {
    printf("Usage: %s <input file> <shift> <output file>\n", argv[0]);
    exit(1);
  }
  char *input_path = argv[1];
  int shift = atoi(argv[2]);
  BUG_ON(shift < 0);
  char *output_path = argv[3];

  printf("Reading input file...\n");
  unordered_map<uint64_t, struct entry> map;
  ifstream input_file;
  input_file.open(input_path);
  string line;
  while (getline(input_file, line)) {
    stringstream line_stream(line);
    string value;

    getline(line_stream, value, ',');
    uint64_t index = stol(value) >> shift;

    if (map.find(index) == map.end()) {
      map[index].index = index;
      map[index].num_l3_misses = 0;
      map[index].num_dram_misses = 0;
    }
    struct entry &entry = map[index];

#ifdef FLOATING_POINT
    getline(line_stream, value, ',');
    entry.num_l3_misses += stod(value);
    getline(line_stream, value, ',');
    entry.num_dram_misses += stod(value);
#else
    getline(line_stream, value, ',');
    entry.num_l3_misses += stol(value);
    getline(line_stream, value, ',');
    entry.num_dram_misses += stol(value);
#endif
  }
  input_file.close();

  printf("Sorting...\n");
  vector<struct entry> vec;
  for (auto &i : map) {
    vec.push_back(i.second);
  }
  sort(vec.begin(), vec.end(), [&](struct entry entry_1, struct entry entry_2) {
    return entry_1.num_dram_misses < entry_2.num_dram_misses;
  });

  printf("Writing to output file...\n");
  ofstream output_file;
  output_file.open(output_path);
  for (auto &entry : vec) {
    output_file << entry.index << ',' << entry.num_l3_misses << ',' << entry.num_dram_misses << '\n';
  }
  output_file.close();
}

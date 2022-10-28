#include <signal.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>

#define SHM_NAME "color_remap_pmem_benchmark"
#define PAGE_SIZE 4096UL

#define BUG_ON(cond)    \
  do {                  \
    if (cond) {         \
      fprintf(stdout, "BUG_ON: %s (L%d) %s\n", __FILE__, __LINE__, __FUNCTION__); \
      raise(SIGABRT);   \
    }                   \
  } while (0)


int main(int argc, char* argv[]) {
  BUG_ON(argc != 2);
  uint64_t indicator_value = (uint64_t) atol(argv[1]);

  int shm_fd = shm_open(SHM_NAME, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);
  BUG_ON(shm_fd < 0);

  void *shm_ptr = mmap(NULL, PAGE_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd, 0);
  BUG_ON(shm_ptr == MAP_FAILED);

  uint64_t *indicator = (uint64_t *) shm_ptr;
  __atomic_store(indicator, &indicator_value, __ATOMIC_SEQ_CST);

  return 0;
}

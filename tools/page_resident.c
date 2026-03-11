#include <stdio.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

int main(int argc, char *argv[]) {
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <file> <map_size> [chunk_size]\n", argv[0]);
        return 1;
    }

    int fd = open(argv[1], O_RDONLY);
    if (fd < 0) { perror("open"); return 1; }

    size_t map_size = (size_t)atol(argv[2]);
    size_t chunk_size = argc > 3 ? (size_t)atol(argv[3]) : (1024 * 1024);

    void *addr = mmap(NULL, map_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (addr == MAP_FAILED) { perror("mmap"); return 1; }

    size_t page_size = sysconf(_SC_PAGESIZE);
    size_t num_pages = (map_size + page_size - 1) / page_size;
    unsigned char *vec = malloc(num_pages);
    if (!vec) { perror("malloc"); return 1; }

    if (mincore(addr, map_size, vec) != 0) { perror("mincore"); return 1; }

    size_t pages_per_chunk = chunk_size / page_size;
    if (pages_per_chunk == 0) pages_per_chunk = 1;

    size_t total_resident = 0;
    for (size_t i = 0; i < num_pages; i += pages_per_chunk) {
        size_t count = 0;
        size_t end = i + pages_per_chunk;
        if (end > num_pages) end = num_pages;
        for (size_t j = i; j < end; j++) {
            if (vec[j] & 1) count++;
        }
        total_resident += count * page_size;
        printf("%zu %zu %zu\n", i * page_size, count * page_size, (end - i) * page_size);
    }
    fprintf(stderr, "total_resident: %zu bytes (%.2f MB)\n",
            total_resident, (double)total_resident / 1024 / 1024);

    munmap(addr, map_size);
    close(fd);
    free(vec);
    return 0;
}

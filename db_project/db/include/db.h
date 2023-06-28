#pragma once
#include <stdint.h>
#include <vector>

typedef uint64_t pagenum_t;

// Open an existing data file using ‘pathname’ or create a new one if it does not exi
int64_t open_table(const char* pathname);

pagenum_t db_get_root_page(int64_t table_id);

//  Insert a ‘key/value’ (i.e., record) with the given size to the data file.
int db_insert(int64_t table_id, int64_t key, const char * value, uint16_t val_size);

int db_find(int64_t table_id, int64_t key, char * ret_val, uint16_t* val_size);

int db_delete(int64_t table_id, int64_t key);

int db_scan (int64_t table_id, int64_t begin_key, int64_t end_key, 
    std::vector<int64_t> * keys, std::vector<char*> * values, 
    std::vector<uint16_t> * val_sizes);

int init_db(int num_buf);

int shutdown_db();

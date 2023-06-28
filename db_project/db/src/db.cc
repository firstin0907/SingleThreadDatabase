#include "../include/bpt.h"
#include "../include/db.h"
#include "../include/file.h"
#include "../include/buffer.h"

#include <iostream>
#include <stdint.h>
#include <cstdio>

int64_t open_table(const char* pathname)
{
    int result = buffer_manager->open_table(pathname);

    return result;
}

int db_insert(int64_t table_id, int64_t key, const char* value,
    uint16_t val_size)
{
    try
    {
        page_t header_p;

        // get header page
        auto header_bb = buffer_manager->get_block(table_id, 0, &header_p);

        // record to insert
        record new_record(key, val_size, value);

        pagenum_t root = header_p.ui64_array[3];

        // initially, checks there are already recode whose key is same with now.
        record* find_result = find_record(table_id, root, key);
        if(find_result != nullptr)
        {
            // it there exists that key, delete result
            delete find_result;

            // failed to insert
            return -1;
        }

        pagenum_t new_root = insert(table_id, root, &new_record);
        if(root != new_root)
        {
            header_p.ui64_array[3] = new_root;
            buffer_manager->write_page(header_bb, header_p);
        }

        return 0;
    }
    catch(const NoSpaceException& e)
    {
        std::cout << e.what() << std::endl;
        return -1;
    }
}

int db_find(int64_t table_id, int64_t key, char* ret_val, uint16_t* val_size)
{
    try
    {
        // get header page
        page_t header_p;
        auto header_bb = buffer_manager->get_block(table_id, 0, &header_p);

        // extract root page number from root
        pagenum_t root = header_p.ui64_array[3];

        // find recode
        record* rec = find_record(table_id, root, key);

        if(rec == nullptr)
        {
            // if failure, return -1
            return -1;
        }
        else
        {
            // set value we find
            *val_size = rec->size;
            for(int i = 0; i < rec->size; i++) ret_val[i] = rec->content[i];
            
            // deallocate rec pointer
            delete[] rec->content;
            delete rec;
            
            // successful -> return 0;
            return 0;
        }
    }
    catch(const NoSpaceException& e)
    {
        std::cout << e.what() << std::endl;
        return -1;
    }
}

int db_delete(int64_t table_id, int64_t key)
{
    try
    {
        page_t header_p;
        // get header page
        auto header_bb = buffer_manager->get_block(table_id, 0, &header_p);
        // extract root page number from root
        pagenum_t root = header_p.ui64_array[3];


        pagenum_t key_leaf = find_leaf(table_id, root, key).page_num;
        record* key_record = find_record(table_id, root, key);

        // if we find corresponding record
        if (key_record != nullptr && key_leaf != 0) {
            pagenum_t new_root = delete_entry(table_id, root, key_leaf, key, 0);
            
            // if root has been changed, write it
            if(root != new_root)
            {
                header_p.ui64_array[3] = new_root;
                buffer_manager->write_page(header_bb, header_p);
            }
            return 0;
        }
        // if we could'm find corresponding record,
        else
        {
            return -1;
        }
    }
    catch(const NoSpaceException& e)
    {
        std::cout << e.what() << std::endl;
        return -1;
    }
}

int db_scan (int64_t table_id, int64_t begin_key, int64_t end_key, 
    std::vector<int64_t> * keys, std::vector<char*> * values,  std::vector<uint16_t> * val_sizes)
{
    try
    {
        // get header page
        page_t header_p;
        auto header_bb = buffer_manager->get_block(table_id, 0, &header_p);
        // extract root page number from root
        pagenum_t root = header_p.ui64_array[3];

        find_range(table_id, root, begin_key, end_key, keys, values, val_sizes);
        return 0;
    }
    catch(const NoSpaceException& e)
    {
        std::cout << e.what() << std::endl;
        return -1;
    }
}


int init_db(int num_buf)
{
    buffer_manager = new BufferManager(num_buf);
    return 0;
}

int shutdown_db()
{
    buffer_manager->close_tables();
    return 0;
}
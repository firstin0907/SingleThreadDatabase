#pragma once

#include "file.h"
#include <stdexcept>
#include <memory>

struct BufferBlock
{
public:
    // up-to-date contents of a target page(4096 bytes)
    page_t      frame;

    // the unique id of a table(per file)
    int64_t     table_id;
    
    // the target page number within a file
    pagenum_t   page_num;

    // whether this buffer block is dirty or not
    bool        is_dirty;

    // whether this buffer block is pinned(in-use) or not
    int         is_pinned;

    // whether this is delete waited.
    bool        is_delete_waited;

    // time the page used lastly.
    uint64_t    last_used;

    // next element of NRU list
    BufferBlock* list_next;

    // previous element of NRU list
    BufferBlock* list_prev;

public:
    std::unique_ptr<page_t> make_clone()
    {
        std::unique_ptr<page_t> clone = std::make_unique<page_t>();
        for(int i = 0; i < PAGE_SIZE; i++)
        {
            clone->c_array[i] = frame.c_array[i];
        }
        return clone;
    }


    void set_whole_frame(const page_t* src)
    {
        for(size_t i = 0; i < PAGE_SIZE; i++)
        {
            frame.c_array[i] = src->c_array[i];
        }
        is_dirty = true;
    }

    void get_whole_frame(page_t* src)
    {
        for(size_t i = 0; i < PAGE_SIZE; i++)
        {
            src->c_array[i] = frame.c_array[i];
        }
    }    

	template <typename T>
    void set_content_by_offset(int offset, T value)
    {
        frame.get_pos_value<T>(offset) = value;
        is_dirty = true;
    }

    template <typename T>
    void set_content_by_index(int index, T value)
    {
        frame.get_pos_value<T>(index * sizeof(T)) = value;
        is_dirty = true;
    }

    template <typename T>
    T get_content_by_offset(int offset)
    {
        return frame.get_pos_value<T>(offset);
    }
    
    template <typename T>
    T get_content_by_index(int index)
    {
        return frame.get_pos_value<T>(index * sizeof(T));
    }

    friend struct BufferManager;
};

class NoSpaceException : public std::exception
{
public:
    virtual const char* what() const noexcept
    {
        return "There is no space in buffer pool.";
    }
};

struct BufferBlockPointer
{
    int valid;
    int64_t table_id;
    pagenum_t page_num;

    struct BufferManager* from;

    BufferBlockPointer(BufferManager* from, int64_t table_id, pagenum_t page_num);
    BufferBlockPointer(const BufferBlockPointer& other);
    BufferBlockPointer(BufferBlockPointer&& other);

    BufferBlockPointer& operator=(const BufferBlockPointer& other);
    BufferBlockPointer& operator=(BufferBlockPointer&& other);

    ~BufferBlockPointer();

};

struct BufferManager
{
public:
    // buffer pool capacity
    int             buffer_list_capacity;

    // number of in-memory blocks
    int             buffer_list_size;

    // buffer pool list that contains in-memory pages
    BufferBlock*    buffer_list_head;

    // number of calling method of this instance(used as time)
    uint64_t        calling_count;

public:
    // initialize BufferManager which can have buffered page of num_buf.
    BufferManager(int num_buf);

    int64_t open_table(const char* pathname);

    BufferBlockPointer get_block(int64_t table_id, pagenum_t page_num, page_t* content = nullptr);

    BufferBlockPointer get_new_block(int64_t table_id,
        PAGE_TYPE page_type = DEFAULT_PAGE);

    void set_delete_waited(BufferBlockPointer bbp);

    void get_page(BufferBlockPointer bbp, page_t& page);

    void write_page(BufferBlockPointer bbp, const page_t& content);

    void free_page(int64_t table_id, pagenum_t page_num);

    void pin_page(int64_t table_id, pagenum_t page_num);

    void unpin_page(int64_t table_id, pagenum_t page_num);

    void close_tables();

    void clear_pages();

private:
    BufferBlock* get_block_pointer(int64_t table_id, pagenum_t page_num);

public:

    ~BufferManager();


};


extern BufferManager* buffer_manager;
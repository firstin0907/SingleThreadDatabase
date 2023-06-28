#include "../include/buffer.h"
#include "../include/file.h"

BufferManager* buffer_manager = nullptr;

BufferBlockPointer::BufferBlockPointer(BufferManager* from, int64_t table_id, pagenum_t page_num)
: table_id(table_id), page_num(page_num), valid(1), from(from)
{
    if(valid && from) from->pin_page(table_id, page_num);
}

BufferBlockPointer::BufferBlockPointer(const BufferBlockPointer& other)
{  
    from = other.from;

    valid = other.valid;
    table_id = other.table_id;
    page_num = other.page_num;

    if(valid && from) from->pin_page(table_id, page_num);
}

BufferBlockPointer::BufferBlockPointer(BufferBlockPointer&& other)
{
    from = other.from;

    valid = other.valid;
    table_id = other.table_id;
    page_num = other.page_num;

    other.from = nullptr;
    other.valid = 0;
}

BufferBlockPointer& BufferBlockPointer::operator=(const BufferBlockPointer& other)
{    
    if(valid && from) from->unpin_page(table_id, page_num);

    this->from = other.from;
    
    valid = other.valid;
    table_id = other.table_id;
    page_num = other.page_num;

    if(valid && from) from->pin_page(table_id, page_num);

    return *this;
}

BufferBlockPointer& BufferBlockPointer::operator=(BufferBlockPointer&& other)
{
    if(valid && from) from->unpin_page(table_id, page_num);
    
    this->from = other.from;

    valid = other.valid;
    table_id = other.table_id;
    page_num = other.page_num;

    other.from = nullptr;
    other.valid = false;

    return *this;
}


BufferBlockPointer::~BufferBlockPointer()
{
    if(valid && from) from->unpin_page(table_id, page_num);
}

BufferManager::BufferManager(int num_buf) : buffer_list_capacity(num_buf)
{
    buffer_list_size = 0;
    buffer_list_head = nullptr;
    calling_count = 0;
}


void BufferManager::set_delete_waited(BufferBlockPointer bbp)
{
    BufferBlock* block = get_block_pointer(bbp.table_id, bbp.page_num);
    block->is_delete_waited = true;
}

BufferBlockPointer BufferManager::get_block(int64_t table_id, pagenum_t page_num, page_t* content)
{
    
    BufferBlock* it = buffer_list_head;
    BufferBlock* victim = nullptr;

    while(it != nullptr)
    {
        // if requested page was founded,
        if(table_id == it->table_id && page_num == it->page_num)
        {
            // return the page;
            if(content != nullptr) *content = it->frame;
            return BufferBlockPointer(this, table_id, page_num);
        }

        // if it is unpinned, it can be victim
        if(it->is_pinned == false)
        {
            // if it is the oldest page among unpinned pages,
            // promote it to victim candidate
            if(victim == nullptr || victim->last_used < it->last_used)
            {
                victim = it;
            }
        }

        // move it to next of it
        it = it->list_next;
    }

    // case: there is no requested page in buffer

    BufferBlock* new_page = nullptr;

    // if there is no empty space, but there is at least one unpinned page
    if(buffer_list_size == buffer_list_capacity && victim != nullptr)
    {
        // write victim page if dirty, and reuse victim for requested page
        if(victim->is_dirty == true)
        {
            file_write_page(victim->table_id, victim->page_num,
                &(victim->frame));
            victim->is_dirty = false;
        }
        new_page = victim;
    }
    // if there is some empty space on list,
    else if(buffer_list_size < buffer_list_capacity)
    {
        new_page = new BufferBlock;

        // put new page into the front of list
        if(buffer_list_head != nullptr) buffer_list_head->list_prev = new_page;
        new_page->list_next = buffer_list_head;
        buffer_list_head = new_page;
        ++buffer_list_size;
    }

    // if we could make new page,
    if(new_page != nullptr)
    {
        file_read_page(table_id, page_num, &(new_page->frame));

        new_page->table_id = table_id;
        new_page->page_num = page_num;
        new_page->is_dirty = false;

        // set pin count as 0, it will be increased soon
        // at the BufferBlockPointer constructure
        new_page->is_pinned = 0;
        new_page->last_used = calling_count;
        new_page->is_delete_waited = false;
        
        if(content != nullptr) *content = new_page->frame;
        // successfully made new page!
        return BufferBlockPointer(this, table_id, page_num);
    }

    // case : there is no space, and no unpinned pages, it fails.
    throw NoSpaceException();
}

BufferBlockPointer BufferManager::get_new_block(int64_t table_id, PAGE_TYPE page_type)
{
    BufferBlock* new_page;

    if(buffer_list_size < buffer_list_capacity)
    {
        new_page = new BufferBlock;

        // put new page into the front of list
        if(buffer_list_head != nullptr) buffer_list_head->list_prev = new_page;
        new_page->list_next = buffer_list_head;
        buffer_list_head = new_page;

        ++buffer_list_size;
    }
    else
    {
        BufferBlock* it = buffer_list_head;
        BufferBlock* victim = nullptr;
        while(it != nullptr)
        {
            // if it is unpinned, it can be victim
            if(it->is_pinned == false)
            {
                // if it is the oldest page among unpinned pages,
                // promote it to victim candidate
                if(victim == nullptr || victim->last_used < it->last_used)
                {
                    victim = it;
                }
            }

            // move it to next of it
            it = it->list_next;
        }

        if(victim == nullptr)
        {
            throw NoSpaceException();
        }

        // write victim page if dirty, and reuse victim for requested page
        if(victim->is_dirty == true)
        {
            file_write_page(victim->table_id, victim->page_num,
                &(victim->frame));
            victim->is_dirty = false;
        }
        new_page = victim;
    }

    // if there is some place for new page in buffer.
    new_page->frame = page_t(page_type);

    new_page->table_id = table_id;
    new_page->page_num = file_alloc_page(table_id);
    new_page->is_dirty = true;
    // set pin count as 0, it will be increased soon
    // at the BufferBlockPointer constructure
    new_page->is_pinned = 0;
    new_page->last_used = calling_count;
    new_page->is_delete_waited = false;

    return BufferBlockPointer(this, new_page->table_id, new_page->page_num);
}

int64_t BufferManager::open_table(const char* pathname)
{
    return file_open_table_file(pathname);
}

void BufferManager::get_page(BufferBlockPointer bbp, page_t& page)
{
    BufferBlock* block = get_block_pointer(bbp.table_id, bbp.page_num);
    page = block->frame;
}

void BufferManager::write_page(BufferBlockPointer bbp, const page_t& content)
{
    BufferBlock* block = get_block_pointer(bbp.table_id, bbp.page_num);
    block->frame = content;
    block->is_dirty = true;
}

void BufferManager::free_page(int64_t table_id, pagenum_t page_num)
{
    BufferBlock* block = get_block_pointer(table_id, page_num);
    file_free_page(block->table_id, block->page_num);
    
    // replace it next time
    block->last_used = 0;
}

void BufferManager::pin_page(int64_t table_id, pagenum_t page_num)
{
    BufferBlock* block = get_block_pointer(table_id, page_num);
    
    block->is_pinned++;
}

void BufferManager::unpin_page(int64_t table_id, pagenum_t page_num)
{
    BufferBlock* block = get_block_pointer(table_id, page_num);
    
    block->last_used = ++calling_count;
    block->is_pinned--;

    if(block->is_pinned == 0 && block->is_delete_waited) free_page(table_id, page_num);
}

void BufferManager::close_tables()
{
    file_close_table_files();
}

void BufferManager::clear_pages()
{
    BufferBlock* curr = buffer_list_head;

    while(curr != nullptr)
    {
        BufferBlock* nxt = curr->list_next;

        if(curr->is_dirty)
        {
            file_write_page(curr->table_id, curr->page_num, &(curr->frame));
        }
        delete curr;
        
        curr = nxt;
    }
    
    buffer_list_head = nullptr;
}



BufferBlock* BufferManager::get_block_pointer(int64_t table_id, pagenum_t page_num)
{
    BufferBlock* it = buffer_list_head;
    BufferBlock* victim = nullptr;

    while(it != nullptr)
    {
        // if requested page was founded,
        if(table_id == it->table_id && page_num == it->page_num)
        {
            // return the page;
            return it;
        }

        // if it is unpinned, it can be victim
        if(it->is_pinned == false)
        {
            // if it is the oldest page among unpinned pages,
            // promote it to victim candidate
            if(victim == nullptr || victim->last_used < it->last_used)
            {
                victim = it;
            }
        }

        // move it to next of it
        it = it->list_next;
    }
    
    return nullptr;
}


BufferManager::~BufferManager()
{
    clear_pages();
}
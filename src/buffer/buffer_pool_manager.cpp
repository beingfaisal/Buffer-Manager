//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include <list>
#include <unordered_map>

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new ClockReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) 
{
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  

  latch_.lock();
   //as page_table_ returns the iterator for end value if the element is not found 
  //so we see if the has been found then
  if(page_table_.find(page_id) != page_table_.end())
  {
    frame_id_t reqFrame = page_table_[page_id];
    //if that is found then piining it
    replacer_->Pin(reqFrame);
    pages_[reqFrame].pin_count_++;
    latch_.unlock();
    return &pages_[reqFrame];
  }
  else
  {
    frame_id_t reqFrame;
    //if its not found in the page_table then we wil find a replacement from free list first
    if(free_list_.size()!= 0)
    {

      //selecting the frame from the free list
      reqFrame = free_list_.front();
      //as the frame in free list is going to be used so we remove it
      free_list_.pop_front();
      replacer_->Pin(reqFrame);
      pages_[reqFrame].pin_count_++;
      pages_[reqFrame].ResetMemory();

      //reading the page from secondary storage
      disk_manager_->ReadPage(page_id, pages_[reqFrame].data_);

      //adding the page into the frame
      pages_[reqFrame].page_id_ = page_id;
      //since its just been added so its not dirty
      pages_[reqFrame].is_dirty_ = false;
      //now we have to add it in the page_table as well
      page_table_[page_id] = reqFrame;

      latch_.unlock();
      return &pages_[reqFrame];
    }
    else
    {
      /* if the requested page is not in free list and also not in page table
      then we look for a replacement in replacer and replace it there */
      bool evictPage = replacer_->Victim(&reqFrame);
     
      //if there is no frame available for replacement
      if(evictPage == false)
      {
        return nullptr;
      }
     
      page_id_t replacerPage = pages_[reqFrame].GetPageId();
     
      //if found replacement frame has dirty page then write onto the drive
      if(pages_[reqFrame].is_dirty_)
      {
        disk_manager_->WritePage(page_id, pages_[reqFrame].data_);
        pages_[reqFrame].is_dirty_ = false;
      }

      replacer_->Pin(reqFrame);
     
      //erasing the replacer page from table
      pages_[reqFrame].ResetMemory();
      page_table_.erase(replacerPage);
      
      //reading the page from secondary storage
      disk_manager_->ReadPage(page_id, pages_[reqFrame].data_);

      //seting the basic members of the replaced page
      pages_[reqFrame].pin_count_ += 1;
      pages_[reqFrame].page_id_ = page_id;
      pages_[reqFrame].is_dirty_ = false;
      //adding new page into the page_table
      page_table_[page_id] = reqFrame;

      latch_.unlock();
      return &pages_[reqFrame];

    }
  }
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) 
{
  latch_.lock();
  //checking whether its in the memory
  if(page_table_.find(page_id) == page_table_.end())
  {
    // if its not in the buffer pool then can't unpin it
    latch_.unlock();
    return true;
  }
  else
  {
    frame_id_t targetFrame = page_table_[page_id];
    
    //if the pin_count is -1 then  page is pinned or it is 0 and already in the replacer so we cant do anything to it
    if(pages_[targetFrame].GetPinCount() <= 0)
    {
      latch_.unlock();
      return false;
    }
    else
    {
      if(is_dirty)  {pages_[targetFrame].is_dirty_ = is_dirty;} 
      pages_[targetFrame].pin_count_--;
      
      // adding thr frame into the replacer
      if(pages_[targetFrame].GetPinCount() == 0)
      {
        replacer_->Unpin(targetFrame);
      }

      latch_.unlock();
      return true;
    }
    
  }
  
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) 
{
  // Make sure you call DiskManager::WritePage!
  if(page_table_.find(page_id)!= page_table_.end())
  {
    // this means page is in page table
    frame_id_t targetFrame = page_table_[page_id];
    
    if(pages_[targetFrame].is_dirty_)
    {
      //if the page has some unsaved data
      disk_manager_->WritePage(page_id,pages_[targetFrame].data_);
      pages_[targetFrame].is_dirty_ = false;
      return true;
    }
    else
    {
      //if its not dirty return true
      return true;
    }
    
  }
  return false;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) 
{
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.



  latch_.lock();
  //first we will look for a free frame in free list
  if(free_list_.size() != 0)
  {
    // this means there are free frames available
    frame_id_t freeFrame = free_list_.back();
    free_list_.pop_back();
    //allocating the new page
    *page_id = disk_manager_->AllocatePage();
    
    //setting the basic metadata of new frame
    pages_[freeFrame].ResetMemory();
    pages_[freeFrame].is_dirty_ = false;
    pages_[freeFrame].page_id_ = *page_id;
    pages_[freeFrame].pin_count_ = 1;

    //Adding the new page to the page table
    page_table_[*page_id] = freeFrame;
    replacer_->Pin(freeFrame);

    latch_.unlock();
    return &pages_[freeFrame];

  }
  else
  {
    /* if no frame id available in free list then we look for
    it in the replacer for a replacement */
    frame_id_t newCandidFrame;
    bool isReplaceable = replacer_->Victim(&newCandidFrame);
    if(!isReplaceable)
    {
      //as there are no replacement present 
      latch_.unlock();
      return nullptr;
    }
    
    //getting the page id
    page_id_t evictingPage = pages_[newCandidFrame].GetPageId();
    if(pages_[newCandidFrame].is_dirty_)
    {
      //if unsaved data remains then save it
      disk_manager_->WritePage(evictingPage,pages_[newCandidFrame].data_);
    }


    page_table_.erase(evictingPage);

    *page_id = disk_manager_->AllocatePage();

    //resetting all the attributes of frame
    pages_[newCandidFrame].ResetMemory();
    pages_[newCandidFrame].is_dirty_ = false;
    pages_[newCandidFrame].pin_count_ = 1;
    pages_[newCandidFrame].page_id_ = *page_id;

    //pinning the page in the replacer
    replacer_->Pin(newCandidFrame);

    //adding the new page into the page table
    page_table_[*page_id] = newCandidFrame;


    latch_.unlock();
    return &pages_[newCandidFrame]; 

  }

  return nullptr;
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  latch_.lock();
  //searching the in the page table
  if(page_table_.find(page_id) != page_table_.end())
  {
    // this means the page is present in the table
    frame_id_t targetFrame = page_table_[page_id];
    if(pages_[targetFrame].GetPageId() == 0)
    {
      // this means the page is no longer in use of anyone
      disk_manager_->DeallocatePage(page_id);
      page_table_.erase(page_id);

      //resetting the metadata of the frame
      pages_[targetFrame].ResetMemory();
      pages_[targetFrame].is_dirty_ = false;
      pages_[targetFrame].pin_count_ = 0;

      //adding this frame into free list
      free_list_.emplace_back(targetFrame);
      
      latch_.unlock();
      return true;

    }
    else
    {
      /* someone is using the page */
      latch_.unlock();
      return false;
    }

  }
  else
  {
    /* the page does not exist in the table */
    latch_.unlock();
    return true;
  }
}

void BufferPoolManager::FlushAllPagesImpl() 
{
  latch_.lock();
  for(size_t i =0; i<pool_size_;i++)
  {
    if(pages_[i].is_dirty_)
    {
      FlushPageImpl(pages_[i].GetPageId());
    }
  }

  latch_.unlock();  
}







}  // namespace bustub

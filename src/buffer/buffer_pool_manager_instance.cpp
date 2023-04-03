//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }

  //  // TODO(students): remove this line after you have implemented the buffer pool manager
  //  throw NotImplementedException(
  //      "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //      "exception line in `buffer_pool_manager_instance.cpp`.");
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

/*
 * 创建一个新页面
 */
auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  std::lock_guard<std::mutex> lck(latch_);
  // 判断freelist是否已满或所有的frame都是不可删除的
  if (this->free_list_.empty() && this->replacer_->Size() == 0) {
    return nullptr;
  }

  frame_id_t frame_id;

  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    if (!replacer_->Evict(&frame_id)) {
      return nullptr;
    }
    if (pages_[frame_id].is_dirty_) {
      std::cout << "NewPgImp: 将frame_id: " << frame_id << "写回磁盘" << std::endl;
      disk_manager_->WritePage(pages_[frame_id].page_id_, pages_[frame_id].GetData());
    }
  }

  std::cout << "NewPgImp: the frame now is : " << frame_id << std::endl;

  // 设置page的dirty为false
  pages_[frame_id].is_dirty_ = false;
  // 清空frame的数据
  pages_[frame_id].ResetMemory();
  // 移除page table
  page_table_->Remove(pages_[frame_id].page_id_);
  // 记录访问记录
  this->replacer_->RecordAccess(frame_id);
  // 设置不可移除
  this->replacer_->SetEvictable(frame_id, false);
  // 分配page id
  *page_id = AllocatePage();
  // 设置frame的page id
  pages_[frame_id].page_id_ = *page_id;
  // 插入page table
  page_table_->Insert(*page_id, frame_id);
  // pin
  pages_[frame_id].pin_count_ = 1;

  return &pages_[frame_id];
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  std::lock_guard<std::mutex> lck(latch_);
  frame_id_t frame_id;
  // pool 中能找到 page_id, 直接返回
  if (page_table_->Find(page_id, frame_id)) {
    std::cout << "FetchPgImp: pool 中能找到frame id: " << frame_id << std::endl;
    // 更新访问记录
    replacer_->RecordAccess(frame_id);
    // 设置不可淘汰
    replacer_->SetEvictable(frame_id, false);
    // 设置pin
    pages_[frame_id].pin_count_++;
    return &pages_[frame_id];
  }
  // pool中没找到，需要从磁盘中获取
  // 看freelist中有空闲的frame
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    if (!replacer_->Evict(&frame_id)) {
      return nullptr;
    }
    if (pages_[frame_id].IsDirty()) {
      disk_manager_->WritePage(pages_[frame_id].page_id_, pages_[frame_id].GetData());
      pages_[frame_id].is_dirty_ = false;
    }
    page_table_->Remove(pages_[frame_id].GetPageId());
  }

  page_table_->Insert(page_id, frame_id);
  pages_[frame_id].page_id_ = page_id;
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].pin_count_ = 1;
  disk_manager_->ReadPage(page_id, pages_[frame_id].data_);

  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);

  //  // 2. 设置page的is_dirty
  //  pages_[frame_id].is_dirty_ = false;
  //  // 3. 移除page table
  //  page_table_->Remove(pages_[frame_id].page_id_);
  //  // 4. 设置frame的page_id
  //  pages_[frame_id].page_id_ = page_id;
  //  // 5. 设置page table
  //  page_table_->Insert(page_id, frame_id);
  //  // 6. 设置pin
  //  pages_[frame_id].pin_count_ = 1;
  //  // 7. 读取磁盘 page_id 对应的数据到frame中
  //  this->disk_manager_->ReadPage(page_id, pages_[frame_id].data_);
  //  // 8. 设置访问记录
  //  replacer_->RecordAccess(frame_id);
  //  // 9. 设置不可淘汰
  //  replacer_->SetEvictable(frame_id, false);
  return &pages_[frame_id];
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::lock_guard<std::mutex> lck(latch_);
  frame_id_t frame_id;
  // page不再pool中，return false
  if (!page_table_->Find(page_id, frame_id)) {
    return false;
  }
  // page的pin已经为0, return false
  if (pages_[frame_id].pin_count_ == 0) {
    return false;
  }
  // 递减pin
  pages_[frame_id].pin_count_--;
  // 如果pin等于0, 代表只有自己使用
  // 设置该frame可以被驱逐
  //
  if (pages_[frame_id].GetPinCount() == 0) {
    replacer_->SetEvictable(frame_id, true);
  }
  // 设置dirty标记
  // 只有原本dirty是false才能改变
  if (!pages_[frame_id].is_dirty_) {
    pages_[frame_id].is_dirty_ = is_dirty;
  }
  return true;
  //  frame_id_t frame_id;
  //  if (!page_table_->Find(page_id, frame_id) || pages_[frame_id].GetPinCount() == 0) {
  //    return false;
  //  }
  //  pages_[frame_id].pin_count_--;
  //  pages_[frame_id].is_dirty_ |= is_dirty;
  //  if (pages_[frame_id].GetPinCount() == 0) {
  //    replacer_->SetEvictable(frame_id, true);
  //  }
  //  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  //  frame_id_t frame_id;
  //  // 判断page id 是否 invalid
  //  if (page_id == INVALID_PAGE_ID) {
  //    return false;
  //  }
  //  // 判断page id 是否在pool
  //  if (!page_table_->Find(page_id, frame_id)) {
  //    return false;
  //  }
  //  // 将page的数据写入到磁盘
  //  this->disk_manager_->WritePage(page_id, pages_[frame_id].GetData());
  //  // 重置dirty
  //  pages_[frame_id].is_dirty_ = false;
  //  return true;
  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    return false;
  }
  disk_manager_->WritePage(pages_[frame_id].GetPageId(), pages_[frame_id].GetData());
  //  pages_->is_dirty_ = false;
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  //  for (size_t i = 0; i < pool_size_; ++i) {
  //    FlushPgImp(pages_[i].page_id_);
  //  }
  frame_id_t tmp;
  std::scoped_lock<std::mutex> lock(latch_);
  for (size_t frame_id = 0; frame_id < pool_size_; frame_id++) {
    if (page_table_->Find(pages_[frame_id].GetPageId(), tmp)) {
      disk_manager_->WritePage(pages_[frame_id].GetPageId(), pages_[frame_id].GetData());
      pages_->is_dirty_ = false;
    }
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lck(latch_);
  //  frame_id_t frame_id;
  //  // page id 不在pool，不做任何操作，返回true
  //  if (!page_table_->Find(page_id, frame_id)) {
  //    return true;
  //  }
  //  // page pin ！= 0, 返回false
  //  if (pages_[frame_id].pin_count_ != 0) {
  //    return false;
  //  }
  //  if (pages_[frame_id].is_dirty_) {
  //    disk_manager_->WritePage(page_id, pages_[frame_id].GetData());
  //    pages_[frame_id].is_dirty_ = false;
  //  }
  //
  //  // 从page table删除page
  //  page_table_->Remove(page_id);
  //  // 替换器中remove
  //  replacer_->Remove(frame_id);
  //  // 重置该frame的内存和元数据
  //  pages_[frame_id].ResetMemory();
  //  pages_[frame_id].pin_count_ = 0;
  //  pages_[frame_id].page_id_ = INVALID_PAGE_ID;
  //  // 将其重新添加到freelist中
  //  free_list_.push_back(frame_id);
  //  // 调用DeallocatePage()释放磁盘的page
  //  DeallocatePage(page_id);
  //  return true;
  DeallocatePage(page_id);
  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    return true;
  }
  if (pages_[frame_id].GetPinCount() > 0) {
    return false;
  }
  if (pages_[frame_id].IsDirty()) {
    disk_manager_->WritePage(pages_[frame_id].GetPageId(), pages_[frame_id].GetData());
    pages_->is_dirty_ = false;
  }
  replacer_->Remove(frame_id);
  free_list_.push_back(frame_id);
  page_table_->Remove(page_id);
  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub

// #include "buffer/buffer_pool_manager_instance.h"
//
// #include "common/exception.h"
// #include "common/macros.h"
//
// namespace bustub {
//
// BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
//                                                      LogManager *log_manager)
//     : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
//   // we allocate a consecutive memory space for the buffer pool
//   pages_ = new Page[pool_size_];
//   page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
//   replacer_ = new LRUKReplacer(pool_size, replacer_k);
//
//   // Initially, every page is in the free list.
//   for (size_t i = 0; i < pool_size_; ++i) {
//     free_list_.emplace_back(static_cast<int>(i));
//   }
// }
//
// BufferPoolManagerInstance::~BufferPoolManagerInstance() {
//   delete[] pages_;
//   delete page_table_;
//   delete replacer_;
// }
//
// auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
//   frame_id_t frame_id;
//   std::scoped_lock<std::mutex> lock(latch_);
//   if (!free_list_.empty()) {
//     *page_id = AllocatePage();
//     frame_id = free_list_.front();
//     free_list_.pop_front();
//     pages_[frame_id].page_id_ = *page_id;
//     pages_[frame_id].is_dirty_ = false;
//     pages_[frame_id].pin_count_ = 1;
//     pages_[frame_id].ResetMemory();
//     replacer_->RecordAccess(frame_id);
//     replacer_->SetEvictable(frame_id, false);
//     page_table_->Insert(*page_id, frame_id);
//     return &pages_[frame_id];
//   }
//   if (replacer_->Evict(&frame_id)) {
//     if (pages_[frame_id].is_dirty_) {
//       disk_manager_->WritePage(pages_[frame_id].page_id_, pages_[frame_id].data_);
//       pages_[frame_id].is_dirty_ = false;
//     }
//     *page_id = AllocatePage();
//     page_table_->Remove(pages_[frame_id].page_id_);
//     pages_[frame_id].page_id_ = *page_id;
//     pages_[frame_id].is_dirty_ = false;
//     pages_[frame_id].pin_count_ = 1;
//     pages_[frame_id].ResetMemory();
//     replacer_->RecordAccess(frame_id);
//     replacer_->SetEvictable(frame_id, false);
//     page_table_->Insert(*page_id, frame_id);
//     return &pages_[frame_id];
//   }
//   return nullptr;
// }
//
// auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
//   frame_id_t frame_id;
//   std::scoped_lock<std::mutex> lock(latch_);
//   if (page_id == INVALID_PAGE_ID) {
//     return nullptr;
//   }
//   if (page_table_->Find(page_id, frame_id)) {
//     replacer_->RecordAccess(frame_id);
//     replacer_->SetEvictable(frame_id, false);
//     pages_[frame_id].pin_count_++;
//     return &pages_[frame_id];
//   }
//   if (!free_list_.empty()) {
//     frame_id = free_list_.front();
//     free_list_.pop_front();
//     disk_manager_->ReadPage(page_id, pages_[frame_id].data_);
//     pages_[frame_id].page_id_ = page_id;
//     pages_[frame_id].is_dirty_ = false;
//     pages_[frame_id].pin_count_ = 1;
//     replacer_->RecordAccess(frame_id);
//     replacer_->SetEvictable(frame_id, false);
//     page_table_->Insert(page_id, frame_id);
//     return &pages_[frame_id];
//   }
//   if (replacer_->Evict(&frame_id)) {
//     if (pages_[frame_id].is_dirty_) {
//       disk_manager_->WritePage(pages_[frame_id].page_id_, pages_[frame_id].data_);
//       pages_[frame_id].is_dirty_ = false;
//     }
//
//     page_table_->Remove(pages_[frame_id].page_id_);
//     disk_manager_->ReadPage(page_id, pages_[frame_id].data_);
//     pages_[frame_id].page_id_ = page_id;
//     pages_[frame_id].is_dirty_ = false;
//     pages_[frame_id].pin_count_ = 1;
//     replacer_->RecordAccess(frame_id);
//     replacer_->SetEvictable(frame_id, false);
//     page_table_->Insert(page_id, frame_id);
//     return &pages_[frame_id];
//   }
//
//   return nullptr;
// }
//
// auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
//   frame_id_t frame_id;
//   std::scoped_lock<std::mutex> lock(latch_);
//   if (page_table_->Find(page_id, frame_id)) {
//     if (pages_[frame_id].pin_count_ <= 0) {
//       return false;
//     }
//     pages_[frame_id].pin_count_--;
//     if (pages_[frame_id].pin_count_ == 0) {
//       replacer_->SetEvictable(frame_id, true);
//     }
//     if (!pages_[frame_id].is_dirty_) {
//       pages_[frame_id].is_dirty_ = is_dirty;
//     }
//
//     return true;
//   }
//
//   return false;
// }
//
// auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
//   std::scoped_lock<std::mutex> lock(latch_);
//   if (page_id == INVALID_PAGE_ID) {
//     return false;
//   }
//   frame_id_t frame_id;
//
//   if (page_table_->Find(page_id, frame_id)) {
//     disk_manager_->WritePage(page_id, pages_[frame_id].data_);
//     pages_[frame_id].is_dirty_ = false;
//     return true;
//   }
//
//   return false;
// }
//
// void BufferPoolManagerInstance::FlushAllPgsImp() {
//   for (size_t i = 0; i < pool_size_; i++) {
//     FlushPgImp(pages_[i].page_id_);
//   }
// }
//
// auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
//   frame_id_t frame_id;
//   latch_.lock();
//   if (page_table_->Find(page_id, frame_id)) {
//     if (pages_[frame_id].GetPinCount() != 0) {
//       latch_.unlock();
//       return false;
//     }
//     page_table_->Remove(page_id);
//     replacer_->Remove(frame_id);
//     pages_[frame_id].ResetMemory();
//     pages_[frame_id].page_id_ = INVALID_PAGE_ID;
//     pages_[frame_id].pin_count_ = 0;
//     pages_[frame_id].is_dirty_ = false;
//     free_list_.push_back(frame_id);
//     DeallocatePage(page_id);
//
//     latch_.unlock();
//     return true;
//   }
//   latch_.unlock();
//   return true;
// }
//
// auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }
//
// }  // namespace bustub

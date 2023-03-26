//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include <algorithm>
#include <iostream>

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  // 先淘汰history尾
  for (auto it = history_list_.begin(); it != history_list_.end(); ++it) {
    if (frame_info_[*it].is_evictable_) {
      *frame_id = *it;
      history_list_.erase(history_hash_map_[*frame_id]);
      history_hash_map_.erase(*frame_id);
      frame_info_[*frame_id].record_time_.clear();
      frame_info_[*frame_id].is_evictable_ = false;
      this->evictable_nums_--;
      return true;
    }
  }

  // 淘汰cache,优先淘汰cache头
  for (auto it = cache_list_.begin(); it != cache_list_.end(); ++it) {
    if (frame_info_[*it].is_evictable_) {
      *frame_id = *it;
      cache_list_.erase(cache_hash_map_[*frame_id]);
      cache_hash_map_.erase(*frame_id);
      frame_info_[*frame_id].record_time_.clear();
      frame_info_[*frame_id].is_evictable_ = false;
      this->evictable_nums_--;
      return true;
    }
  }

  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  // buffer 不存在这个frame_id
  // frame第一次插入
  if (frame_info_.find(frame_id) == frame_info_.end()) {
    // 创建该frame
    auto new_frame = FrameInfo();
    new_frame.frame_id_ = frame_id;
    new_frame.record_time_.push_back(current_timestamp_);
    // 更新frame_info_
    frame_info_[frame_id] = new_frame;
    // 插入history_list_
    history_list_.push_back(frame_id);
    // 更新history_hash_map_
    history_hash_map_[frame_id] = --history_list_.end();
    // 更新curr_stamp
    current_timestamp_++;
    return;
  }

  // 更新frame的访问记录
  frame_info_[frame_id].record_time_.push_back(current_timestamp_);
  current_timestamp_++;
  // bug: size() < k 时，即size == 1 时，有可能不再history里面
  if (frame_info_[frame_id].record_time_.size() < k_) {
    if (history_hash_map_.find(frame_id) == history_hash_map_.end()) {
      history_list_.push_front(frame_id);
      history_hash_map_.insert({frame_id, history_list_.begin()});
    }
    return;
  }
  if (frame_info_[frame_id].record_time_.size() == k_) {
    // 访问后，访问次数等于k
    // 删除history
    history_list_.erase(history_hash_map_[frame_id]);
    history_hash_map_.erase(frame_id);
    //    // 找到合适的位置，加入到cache
    //    for (auto it = cache_list_.begin(); it != cache_list_.end(); ++it) {
    //      if (frame_info_[*it].record_time_.front() > frame_info_[frame_id].record_time_.front()) {
    //        cache_hash_map_[frame_id] = cache_list_.insert(it, frame_id);
    //        break;
    //      }
    //    }
    //    if (cache_hash_map_.find(frame_id) == cache_hash_map_.end()) {
    //      cache_list_.push_back(frame_id);
    //      cache_hash_map_[frame_id] = (--cache_list_.end());
    //    }
    this->CacheListInsert(frame_id);
    return;
  }
  if (frame_info_[frame_id].record_time_.size() > k_) {
    // 访问后，访问次数 > k
    // 删除原本在cache_list_的元素
    cache_list_.erase(cache_hash_map_[frame_id]);
    cache_hash_map_.erase(frame_id);
    frame_info_[frame_id].record_time_.pop_front();

    // 加入到cache
    //    for (auto it = cache_list_.begin(); it != cache_list_.end(); ++it) {
    //      if (frame_info_[*it].record_time_.front() > frame_info_[frame_id].record_time_.front()) {
    //        cache_hash_map_[frame_id] = cache_list_.insert(it, frame_id);
    //        break;
    //      }
    //    }
    //    if (cache_hash_map_.find(frame_id) == cache_hash_map_.end()) {
    //      cache_list_.push_back(frame_id);
    //      cache_hash_map_[frame_id] = (--cache_list_.end());
    //    }
    this->CacheListInsert(frame_id);
    return;
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock<std::mutex> lock(latch_);
  BUSTUB_ASSERT(frame_id < static_cast<frame_id_t>(replacer_size_), "SetEvictable: frame not valid");
  BUSTUB_ASSERT(frame_info_.find(frame_id) != frame_info_.end(), "SetEvictable: frame id not in pool");

  // 判断该frame是否存在
  if (history_hash_map_.find(frame_id) == history_hash_map_.end() &&
      cache_hash_map_.find(frame_id) == cache_hash_map_.end()) {
    return;
  }

  // 存在 && 原来是 true，改成false，size--
  if (frame_info_[frame_id].is_evictable_ && !set_evictable) {
    this->evictable_nums_--;
  } else if (!frame_info_[frame_id].is_evictable_ && set_evictable) {
    this->evictable_nums_++;
  }

  frame_info_[frame_id].is_evictable_ = set_evictable;
}

// 剔除指定的frame
void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  // 没找到,直接返回
  if (history_hash_map_.find(frame_id) == history_hash_map_.end() &&
      cache_hash_map_.find(frame_id) == cache_hash_map_.end()) {
    return;
  }
  BUSTUB_ASSERT(frame_info_[frame_id].is_evictable_ == true, "Remove: frame id not evictable");
  if (history_hash_map_.find(frame_id) == history_hash_map_.end() &&
      cache_hash_map_.find(frame_id) == cache_hash_map_.end()) {
    return;
  }
  // 在history上
  if (history_hash_map_.find(frame_id) != history_hash_map_.end()) {
    history_list_.erase(history_hash_map_[frame_id]);
    history_hash_map_.erase(frame_id);
    frame_info_[frame_id].record_time_.clear();
    frame_info_[frame_id].is_evictable_ = false;
    this->evictable_nums_--;
  }
  // 在cache上
  if (cache_hash_map_.find(frame_id) != cache_hash_map_.end()) {
    cache_list_.erase(cache_hash_map_[frame_id]);
    cache_hash_map_.erase(frame_id);
    frame_info_[frame_id].record_time_.clear();
    frame_info_[frame_id].is_evictable_ = false;
    this->evictable_nums_--;
  }
}

auto LRUKReplacer::Size() -> size_t {
  std::scoped_lock<std::mutex> lock(latch_);
  return this->evictable_nums_;
}
void LRUKReplacer::PrintPool() {
  //  std::cout << "history_list"
  //            << " ";
  //  for (auto it = history_list_.rbegin(); it != history_list_.rend(); ++it) {
  //    std::cout << *it << " ";
  //  }
  //  std::cout << std::endl;
  //  std::cout << "cache_list"
  //            << " ";
  //  for (int &it : cache_list_) {
  //    std::cout << it << " ";
  //  }
  //  std::cout << std::endl;
  //
  //  std::cout << "frame_info size " << frame_info_.size() << std::endl;
  //  std::cout << "history hash size " << history_hash_map_.size() << std::endl;
  //  std::cout << "cache hash size " << cache_hash_map_.size() << std::endl;
  std::cout << "en size: " << this->evictable_nums_ << std::endl;
}
void LRUKReplacer::PrintFrameInfo(frame_id_t frame_id) {
  if (frame_info_.find(frame_id) == frame_info_.end()) {
    std::cout << "frame id not find" << std::endl;
  }
  std::cout << frame_id << " record times: " << frame_info_[frame_id].record_time_.size() << std::endl;
}
void LRUKReplacer::CacheListInsert(frame_id_t frame_id) {
  for (auto it = cache_list_.begin(); it != cache_list_.end(); ++it) {
    if (frame_info_[*it].record_time_.front() > frame_info_[frame_id].record_time_.front()) {
      cache_hash_map_[frame_id] = cache_list_.insert(it, frame_id);
      break;
    }
  }
  if (cache_hash_map_.find(frame_id) == cache_hash_map_.end()) {
    cache_list_.push_back(frame_id);
    cache_hash_map_[frame_id] = (--cache_list_.end());
  }
}

}  // namespace bustub

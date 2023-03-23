//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size) : bucket_size_(bucket_size) {
  dir_.push_back(std::make_unique<Bucket>(Bucket(bucket_size)));
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  // get dir index
  size_t dir_index = ExtendibleHashTable::IndexOf(key);
  auto bucket = this->dir_[dir_index];
  return bucket->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  // 0. get key hash code
  size_t idx = ExtendibleHashTable::IndexOf(key);
  // 1. get bucket
  std::shared_ptr<Bucket> *bucket = &this->dir_[idx];
  // 2. remove
  return bucket->get()->Remove(key);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  std::scoped_lock<std::mutex> lock(latch_);
  while (this->dir_[IndexOf(key)]->IsFull()) {
    // get index
    size_t idx = IndexOf(key);
    // get bucket
    auto bucket = dir_[idx];

    // depth 相同需要扩容
    if (bucket->GetDepth() == this->GetGlobalDepthInternal()) {
      size_t dir_size = this->dir_.size();
      this->dir_.resize(dir_size * 2);
      this->global_depth_++;
      for (auto i = dir_size; i != dir_.size(); ++i) {
        dir_[i] = dir_[i - dir_size];
      }
    }

    // 分裂bucket
    // get local depth
    int local_depth = bucket->GetDepth();
    // get mask
    int mask = 1 << local_depth;
    auto zero_bucket = std::make_shared<Bucket>(bucket_size_, local_depth + 1);
    auto one_bucket = std::make_shared<Bucket>(bucket_size_, local_depth + 1);
    // 将 bucket 的kv重分配
    for (auto item = bucket->GetItems().begin(); item != bucket->GetItems().end(); ++item) {
      if (std::hash<K>()(item->first) & mask) {
        one_bucket->Insert(item->first, item->second);
      } else {
        zero_bucket->Insert(item->first, item->second);
      }
    }

    //    if (!zero_bucket->GetItems().empty() && !one_bucket->GetItems().empty()) {
    //      num_buckets_++;
    //    }

    num_buckets_++;
    // 将目录重新指向
    for (size_t i = 0; i < dir_.size(); ++i) {
      if (dir_[i] == bucket) {
        if ((i & mask) != 0U) {
          dir_[i] = one_bucket;
        } else {
          dir_[i] = zero_bucket;
        }
      }
    }
  }

  // 找到空位置
  std::shared_ptr<Bucket> *bucket = &this->dir_[IndexOf(key)];
  bucket->get()->Insert(key, value);
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  for (const auto &p : list_) {
    if (p.first == key) {
      value = p.second;
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  for (auto it = list_.begin(); it != list_.end(); ++it) {
    if (it->first == key) {
      list_.erase(it);
      return true;
    }
  }

  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  if (Update(key, value)) {
    return true;
  }

  if (IsFull()) {
    return false;
  }

  list_.push_back({key, value});
  return true;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Update(const K &key, const V &value) -> bool {
  for (auto &p : list_) {
    if (p.first == key) {
      p.second = value;
      return true;
    }
  }
  return false;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub

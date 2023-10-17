//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      index_info_(exec_ctx->GetCatalog()->GetIndex(plan->GetIndexOid())),
      table_info_(exec_ctx->GetCatalog()->GetTable(index_info_->table_name_)),
      index_(dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(index_info_->index_.get())),
      iter_(index_->GetBeginIterator()) {}

void IndexScanExecutor::Init() { }

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if(iter_ == index_->GetEndIterator()) {
    return false;
  }
  *rid = (*iter_).second;
  auto ret = table_info_->table_->GetTuple(*rid, tuple, exec_ctx_->GetTransaction());
  ++iter_;
  return ret;
}

}  // namespace bustub

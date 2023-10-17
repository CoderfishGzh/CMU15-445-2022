//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(plan->GetAggregates(), plan->GetAggregateTypes()),
      aht_iterator_(aht_.Begin()),
      successful_(false) {}

// 初始化时，将所有的tuple插入到Hash Table中
void AggregationExecutor::Init() {
  child_->Init();
  Tuple tuple;
  RID rid;
  while(child_->Next(&tuple, &rid)) {
    auto key = MakeAggregateKey(&tuple);
    auto value = MakeAggregateValue(&tuple);
    aht_.InsertCombine(key, value);
  }
  aht_iterator_ = aht_.Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // 获取一个key
  if(aht_iterator_ != aht_.End()) {
    std::vector<Value> value(aht_iterator_.Key().group_bys_);
    for(const auto &agg : aht_iterator_.Val().aggregates_) {
      value.push_back(agg);
    }
    *tuple = {value, &plan_->OutputSchema()};
    ++aht_iterator_;
    successful_ = true;
    return true;
  }

  // table is empty
  if(!successful_ && plan_->group_bys_.empty()) {
    // 空表只会返回一次
    successful_ = true;
    std::vector<Value> value;
    for(auto agg : plan_->agg_types_) {
      switch (agg) {
        case AggregationType::CountStarAggregate:
          value.emplace_back(INTEGER, 0);
          break;
        case AggregationType::CountAggregate:
        case AggregationType::SumAggregate:
        case AggregationType::MaxAggregate:
        case AggregationType::MinAggregate:
          value.emplace_back(ValueFactory::GetNullValueByType(TypeId::INTEGER));
          break ;
      }
    }
    *tuple = {value, &plan_->OutputSchema()};
    return true;
  }
  return false;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub

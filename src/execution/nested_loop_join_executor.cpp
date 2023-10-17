//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_schema_(plan->GetLeftPlan()->OutputSchema()),
      right_schema_(plan->GetRightPlan()->OutputSchema()),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor))
{
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
  is_inner_ = (plan->GetJoinType() == JoinType::INNER);
}

// 不论left or inner 都是通过left == right 来比对的
void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  Tuple tuple;
  RID rid;
  while(right_executor_->Next(&tuple, &rid)) {
    right_tuples_.push_back(tuple);
  }
  std::cout << "finish Join Init" << std::endl;
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  std::vector<Column> columns(left_schema_.GetColumns());
  columns.insert(columns.end(), right_schema_.GetColumns().begin(), right_schema_.GetColumns().end());
  Schema schema(columns);
  if (is_inner_) {
    return InnerJoin(schema, tuple);
  }
  return LeftJoin(schema, tuple);
}

auto NestedLoopJoinExecutor::InnerJoin(const bustub::Schema &schema, bustub::Tuple *tuple) -> bool {
  if (is_end_) {
    return false;
  }
  if (index_ != 0) {
    for (; index_ < right_tuples_.size(); index_++) {
      if (plan_->predicate_->EvaluateJoin(&left_tuple_, left_schema_, &right_tuples_[index_], right_schema_).GetAs<bool>()) {
        std::cout << "else if index_ now is: " << index_ << std::endl;
        std::vector<Value> value;
        for (uint32_t j = 0; j < left_schema_.GetColumnCount(); j++) {
          value.push_back(left_tuple_.GetValue(&left_schema_, j));
        }
        for (uint32_t j = 0; j < right_schema_.GetColumnCount(); j++) {
          value.push_back(right_tuples_[index_].GetValue(&right_schema_, j));
        }
        *tuple = {value, &schema};
        index_++;
        return true;
      }
    }
    // right table is over, reset
    index_ = 0;
  }
  if (index_ == 0) {
    // 当该循环结束，代表左表已经循环完成
    while (left_executor_->Next(&left_tuple_, &left_rid_)) {
      for (; index_ < right_tuples_.size(); index_++) {
        if (plan_->predicate_->EvaluateJoin(&left_tuple_, left_schema_, &right_tuples_[index_], right_schema_).GetAs<bool>()) {
          std::vector<Value> value;
          for (uint32_t j = 0; j < left_schema_.GetColumnCount(); j++) {
            value.push_back(left_tuple_.GetValue(&left_schema_, j));
          }
          for (uint32_t j = 0; j < right_schema_.GetColumnCount(); j++) {
            value.push_back(right_tuples_[index_].GetValue(&right_schema_, j));
          }
          *tuple = {value, &schema};
          index_++;
          return true;
        }
      }
      index_ = 0;
    }
    is_end_ = true;
    return false;
  }
  return true;
}

auto NestedLoopJoinExecutor::LeftJoin(const bustub::Schema &schema, bustub::Tuple *tuple) -> bool {
  if (is_end_) {
    return false;
  }
  if (index_ != 0) {
    for (; index_ < right_tuples_.size(); index_++) {
      if (plan_->predicate_->EvaluateJoin(&left_tuple_, left_schema_, &right_tuples_[index_], right_schema_).GetAs<bool>()) {
        std::cout << "else if index_ now is: " << index_ << std::endl;
        std::vector<Value> value;
        for (uint32_t j = 0; j < left_schema_.GetColumnCount(); j++) {
          value.push_back(left_tuple_.GetValue(&left_schema_, j));
        }
        for (uint32_t j = 0; j < right_schema_.GetColumnCount(); j++) {
          value.push_back(right_tuples_[index_].GetValue(&right_schema_, j));
        }
        *tuple = {value, &schema};
        index_++;
        return true;
      }
    }
    // right table is over, reset
    index_ = 0;
  }
  if (index_ == 0) {
    // 当该循环结束，代表左表已经循环完成
    while (left_executor_->Next(&left_tuple_, &left_rid_)) {
      is_match_ = false;
      for (; index_ < right_tuples_.size(); index_++) {
        if (plan_->predicate_->EvaluateJoin(&left_tuple_, left_schema_, &right_tuples_[index_], right_schema_).GetAs<bool>()) {
          is_match_ = true;
          std::vector<Value> value;
          for (uint32_t j = 0; j < left_schema_.GetColumnCount(); j++) {
            value.push_back(left_tuple_.GetValue(&left_schema_, j));
          }
          for (uint32_t j = 0; j < right_schema_.GetColumnCount(); j++) {
            value.push_back(right_tuples_[index_].GetValue(&right_schema_, j));
          }
          *tuple = {value, &schema};
          index_++;
          return true;
        }
      }
      index_ = 0;
      if (is_match_ == false) {
        std::vector<Value> value;
        for (uint32_t i = 0; i < left_schema_.GetColumnCount(); i++) {
          value.push_back(left_tuple_.GetValue(&left_schema_, i));
        }
        for (uint32_t i = 0; i < right_schema_.GetColumnCount(); i++) {
          value.push_back(ValueFactory::GetNullValueByType(right_schema_.GetColumn(i).GetType()));
        }
        *tuple = {value, &schema};
        return true;
      }
    }
    is_end_ = true;
    return false;
  }


//  if (index_ == 0) {
//    while (left_executor_->Next(&left_tuple_, &left_rid_)) {
//      for (; index_ < right_tuples_.size(); index_++) {
//        if (plan_->predicate_->EvaluateJoin(&left_tuple_, left_schema_, &right_tuples_[index_], right_schema_).GetAs<bool>()) {
//          is_match_ = true;
//          std::vector<Value> value;
//          for (uint32_t j = 0; j < left_schema_.GetColumnCount(); j++) {
//            value.push_back(left_tuple_.GetValue(&left_schema_, j));
//          }
//          for (uint32_t j = 0; j < right_schema_.GetColumnCount(); j++) {
//            value.push_back(right_tuples_[j].GetValue(&right_schema_, j));
//          }
//          *tuple = {value, &schema};
//          return true;
//        }
//      }
//      index_ = 0;
//      // if not match, push null
//      if (!is_match_) {
//        std::vector<Value> value;
//        for (uint32_t i = 0; i < left_schema_.GetColumnCount(); i++) {
//          value.push_back(left_tuple_.GetValue(&left_schema_, i));
//        }
//        for (uint32_t i = 0; i < right_schema_.GetColumnCount(); i++) {
//          value.push_back(ValueFactory::GetNullValueByType(right_schema_.GetColumn(i).GetType()));
//        }
//        *tuple = {value, &schema};
//        is_match_ = true;
//        return true;
//      }
//    }
//    is_end_ = true;
//    return false;
//  } else if (index_ != 0) {
//    for ( ; index_ < right_tuples_.size(); index_++) {
//      if (plan_->predicate_->EvaluateJoin(&left_tuple_, left_schema_, &right_tuples_[index_], right_schema_).GetAs<bool>()) {
//        std::vector<Value> value;
//        for (uint32_t j = 0; j < left_schema_.GetColumnCount(); j++) {
//          value.push_back(left_tuple_.GetValue(&left_schema_, j));
//        }
//        for (uint32_t j = 0; j < right_schema_.GetColumnCount(); j++) {
//          value.push_back(right_tuples_[j].GetValue(&right_schema_, j));
//        }
//        *tuple = {value, &schema};
//        return true;
//      }
//    }
//    // right table is over, reset
//    index_ = 0;
//  }
  return false;
}



}  // namespace bustub

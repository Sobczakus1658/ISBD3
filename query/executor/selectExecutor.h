
#pragma once

#include "../../types.h"

SELECT_TABLE_ERROR executeSelectBatch(const SelectQuery &query, const Batch &batch, std::vector<MixBatch> &outBatches);

SELECT_TABLE_ERROR transformBatch(const SelectQuery &query, const Batch &batch, MixBatch &outBatch);

SELECT_TABLE_ERROR orderAndLimitResult(std::vector<MixBatch> &batches, const std::vector<OrderByExpression> &orderBy, const std::optional<size_t> &limit);

SELECT_TABLE_ERROR validateOrderByAndLimit(const std::vector<MixBatch> &batches, const std::vector<OrderByExpression> &orderBy, const std::optional<size_t> &limit);

std::string spillBatchesToRun(const std::vector<MixBatch> &batches, const std::vector<OrderByExpression> &orderBy);

MixBatch mergeRunFiles(const std::vector<std::string> &runFiles, const std::vector<OrderByExpression> &orderBy, const std::optional<size_t> &limit);
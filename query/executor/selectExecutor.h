
#pragma once

#include "../../types.h"

SELECT_TABLE_ERROR executeSelectBatch(const SelectQuery &query, const Batch &batch, std::vector<MixBatch> &outBatches);

void orderAndLimitResult(std::vector<MixBatch> &batches, const std::vector<OrderByExpression> &orderBy, const std::optional<size_t> &limit);
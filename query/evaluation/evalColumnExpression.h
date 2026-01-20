
#pragma once

#include "../../types.h"
#include "expression_cache.h"

Value evalColumnExpression(const ColumnExpression &expr, const ResultRow &row, ExpressionCache *cache = nullptr);
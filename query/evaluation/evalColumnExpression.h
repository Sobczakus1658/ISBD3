
#pragma once

#include "../../types.h"
#include "expression_cache.h"

// Evaluate a ColumnExpression for a given row. If 'cache' is provided, the
// evaluator will consult and populate the per-row ExpressionCache so that
// identical subexpressions are computed only once.
Value evalColumnExpression(const ColumnExpression &expr, const ResultRow &row, ExpressionCache *cache = nullptr);
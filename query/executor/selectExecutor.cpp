
#pragma once

#include "selectExecutor.h"
#include <iostream>
#include <algorithm>
#include <string>

#include "../evaluation/evalColumnExpression.h"

SELECT_TABLE_ERROR executeSelectBatch(const SelectQuery &query, const Batch &batch, std::vector<MixBatch> &outBatches){
    MixBatch resultBatch;

    size_t projectedCols = query.columnClauses.size();
    resultBatch.columns.resize(projectedCols);
    size_t addedRows = 0;

    for (size_t rowIdx = 0; rowIdx < batch.num_rows; ++rowIdx) {
        ResultRow rawRow;
        for (const auto& intCol : batch.intColumns)
            rawRow.values.push_back(Value{ValueType::INT64, intCol.column[rowIdx], {}, false});
        for (const auto& strCol : batch.stringColumns)
            rawRow.values.push_back(Value{ValueType::VARCHAR, 0, strCol.column[rowIdx], false});

        if (query.whereClause) {
            Value whereVal = evalColumnExpression(*query.whereClause, rawRow);
            if (whereVal.type != ValueType::BOOL) {
                return SELECT_TABLE_ERROR::INVALID_WHERE;
            }
            if (!whereVal.boolValue) continue;
        }

        for (size_t c = 0; c < projectedCols; ++c) {
            Value v = evalColumnExpression(*query.columnClauses[c], rawRow);
            resultBatch.columns[c].type = v.type;
            resultBatch.columns[c].data.push_back(v);
        }
        ++addedRows;
    }

    resultBatch.num_rows = addedRows;
    outBatches.push_back(std::move(resultBatch));
    return SELECT_TABLE_ERROR::NONE;
}

void orderAndLimitResult(std::vector<MixBatch> &batches,
                         const std::vector<OrderByExpression> &orderBy,
                         const std::optional<size_t> &limit) {

    if (batches.empty()) return;

    size_t totalRows = 0;
    for (const auto &b : batches) totalRows += b.num_rows;
    if (totalRows == 0) return;

    size_t numCols = batches[0].columns.size();
    std::vector<ResultRow> allRows;
    allRows.reserve(totalRows);

    for (const auto &b : batches) {
        for (size_t r = 0; r < b.num_rows; ++r) {
            ResultRow row;
            row.values.reserve(numCols);
            for (size_t c = 0; c < numCols; ++c) {
                if (r < b.columns[c].data.size()) row.values.push_back(b.columns[c].data[r]);
                else row.values.push_back(Value{ValueType::INT64, 0, std::string(), false});
            }
            allRows.push_back(std::move(row));
        }
    }

    if (!orderBy.empty()) {
        std::sort(allRows.begin(), allRows.end(), [&](const ResultRow &a, const ResultRow &b) {
            for (const auto &o : orderBy) {
                const Value &va = a.values[o.columnIndex];
                const Value &vb = b.values[o.columnIndex];

                int cmp = 0;
                if (va.type == vb.type) {
                    switch (va.type) {
                        case ValueType::INT64:   cmp = (va.intValue < vb.intValue) ? -1 : (va.intValue > vb.intValue ? 1 : 0); break;
                        case ValueType::VARCHAR: cmp = va.stringValue.compare(vb.stringValue); break;
                        case ValueType::BOOL:    cmp = (va.boolValue == vb.boolValue) ? 0 : (va.boolValue ? 1 : -1); break;
                    }
                } else {
                    cmp = std::to_string(va.intValue).compare(std::to_string(vb.intValue));
                }

                if (cmp != 0) return o.ascending ? (cmp < 0) : (cmp > 0);
            }
            return false;
        });
    }

    size_t take = allRows.size();
    if (limit.has_value() && take > limit.value()) take = limit.value();

    MixBatch finalBatch;
    finalBatch.num_rows = take;
    finalBatch.columns.resize(numCols);

    for (size_t c = 0; c < numCols; ++c) {
        if (!allRows.empty()) finalBatch.columns[c].type = allRows[0].values[c].type;
        finalBatch.columns[c].data.reserve(take);
        for (size_t r = 0; r < take; ++r) {
            finalBatch.columns[c].data.push_back(allRows[r].values[c]);
        }
    }

    batches.clear();
    batches.push_back(std::move(finalBatch));
}

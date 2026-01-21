#include "selectExecutor.h"
#include <iostream>
#include <algorithm>
#include <string>
#include <unordered_map>

#include "../evaluation/evalColumnExpression.h"
#include "../evaluation/expression_cache.h"
#include "../evaluation/expression_hasher.h"
#include "../../metastore/metastore.h"
#include "../evaluation/evalColumnExpression.h"
#include <fstream>
#include <sstream>
#include <vector>
#include <queue>
#include <cstdio>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <nlohmann/json.hpp>
using json = nlohmann::ordered_json;
#include "../../utils/utils.h"

const size_t MEMORY_LIMIT = (size_t)4 * 1024 * 1024;

SELECT_TABLE_ERROR executeSelectBatch(const SelectQuery &query, const Batch &batch, std::vector<MixBatch> &outBatches){
    MixBatch mb;
    auto r = transformBatch(query, batch, mb);
    if (r != SELECT_TABLE_ERROR::NONE) return r;
    outBatches.push_back(std::move(mb));
    return SELECT_TABLE_ERROR::NONE;
}

SELECT_TABLE_ERROR transformBatch(const SelectQuery &query, const Batch &batch, MixBatch &outBatch) {
    TableInfo info;
    if (query.tableName.empty()) {
        info.name = std::string();
        info.id = 0;
        info.info.clear();
        info.location.clear();
        info.files.clear();
    } else {
        auto infoOpt = getTableInfoByName(query.tableName);
        if (!infoOpt) return SELECT_TABLE_ERROR::TABLE_NOT_EXISTS;
        info = *infoOpt;
    }

    std::unordered_map<std::string, size_t> intIndex;
    std::unordered_map<std::string, size_t> strIndex;
    for (size_t i = 0; i < batch.intColumns.size(); ++i) intIndex[batch.intColumns[i].name] = i;
    for (size_t i = 0; i < batch.stringColumns.size(); ++i) strIndex[batch.stringColumns[i].name] = i;


    bool hasNames = false;
    for (const auto &c : batch.intColumns) if (!c.name.empty()) { hasNames = true; break; }
    if (!hasNames) for (const auto &c : batch.stringColumns) if (!c.name.empty()) { hasNames = true; break; }
    if (!hasNames) {
        intIndex.clear();
        strIndex.clear();
        size_t nextInt = 0;
        size_t nextStr = 0;
        for (size_t c = 0; c < info.info.size(); ++c) {
            const auto &col = info.info[c];
            if (col.second == "INT64") {
                intIndex[col.first] = nextInt++;
            } else {
                strIndex[col.first] = nextStr++;
            }
        }
    }

    size_t baseCols = info.info.size();
    size_t projCols = query.columnClauses.size();
    size_t whereCol = query.whereClause ? 1 : 0;
    size_t totalCols = baseCols + projCols + whereCol;

    outBatch.columns.clear();
    outBatch.columns.resize(totalCols);
    outBatch.num_rows = 0;

    for (size_t c = 0; c < baseCols; ++c) {
        const auto &col = info.info[c];
        const std::string &typeStr = col.second;
        if (typeStr == "INT64") outBatch.columns[c].type = ValueType::INT64;
        else if (typeStr == "VARCHAR") outBatch.columns[c].type = ValueType::VARCHAR;
        else if (typeStr == "BOOL") outBatch.columns[c].type = ValueType::BOOL;
        else outBatch.columns[c].type = ValueType::VARCHAR;
    }
    for (size_t c = 0; c < projCols; ++c) outBatch.columns[baseCols + c].type = ValueType::INT64;
    if (whereCol) outBatch.columns[baseCols + projCols].type = ValueType::BOOL;

    std::unordered_map<size_t, int> exprCount;
    std::unordered_map<size_t, const ColumnExpression*> exprMap;
    if (query.whereClause) {
        size_t h = hashExpression(*query.whereClause);
        exprCount[h]++;
        exprMap.emplace(h, query.whereClause.get());
    }
    for (const auto &pc : query.columnClauses) {
        size_t h = hashExpression(*pc);
        exprCount[h]++;
        exprMap.emplace(h, pc.get());
    }
    std::vector<size_t> precomputeHashes;
    for (const auto &kv : exprCount) {
        if (kv.second > 1) {
            precomputeHashes.push_back(kv.first);
        }
    }

    for (size_t rowIdx = 0; rowIdx < batch.num_rows; ++rowIdx) {
        ResultRow rawRow;
        rawRow.values.reserve(baseCols);

        for (size_t c = 0; c < baseCols; ++c) {
            const auto &col = info.info[c];
            const std::string &name = col.first;
            const std::string &typeStr = col.second;
            if (typeStr == "INT64") {
                auto it = intIndex.find(name);
                if (it == intIndex.end()) {
                    std::string msg = std::string("transformBatch: missing int column '") + name + "' in input batch for table " + query.tableName;
                    log_error(msg);
                    std::string available = "available int columns: ";
                    for (const auto &p : intIndex) available += p.first + ",";
                    log_error(available);
                    std::string availableStr = "available str columns: ";
                    for (const auto &p : strIndex) availableStr += p.first + ",";
                    log_error(availableStr);
                    return SELECT_TABLE_ERROR::TABLE_NOT_EXISTS;
                }
                const auto &vec = batch.intColumns[it->second].column;
                rawRow.values.push_back(Value{ValueType::INT64, vec[rowIdx], std::string(), false});
            } else {
                auto it = strIndex.find(name);
                if (it == strIndex.end()) {
                    std::string msg = std::string("transformBatch: missing string column '") + name + "' in input batch for table " + query.tableName;
                    log_error(msg);
                    std::string available = "available int columns: ";
                    for (const auto &p : intIndex) available += p.first + ",";
                    log_error(available);
                    std::string availableStr = "available str columns: ";
                    for (const auto &p : strIndex) availableStr += p.first + ",";
                    log_error(availableStr);
                    return SELECT_TABLE_ERROR::TABLE_NOT_EXISTS;
                }
                const auto &vec = batch.stringColumns[it->second].column;
                rawRow.values.push_back(Value{ValueType::VARCHAR, 0, vec[rowIdx], false});
            }
        }

        ExpressionCache cache;

        for (size_t h : precomputeHashes) {
            auto it = exprMap.find(h);
            if (it == exprMap.end()) continue;
            const ColumnExpression *expr = it->second;
            (void)evalColumnExpression(*expr, rawRow, &cache);
        }

        bool wherePass = true;
        if (query.whereClause) {
            size_t wh = hashExpression(*query.whereClause);
            Value wv = evalColumnExpression(*query.whereClause, rawRow, &cache);
            if (wv.type != ValueType::BOOL) return SELECT_TABLE_ERROR::INVALID_WHERE;
            wherePass = wv.boolValue;
        }

        if (!wherePass) continue;

        for (size_t c = 0; c < baseCols; ++c) outBatch.columns[c].data.push_back(rawRow.values[c]);

        for (size_t p = 0; p < projCols; ++p) {
            size_t ph = hashExpression(*query.columnClauses[p]);
            Value v = evalColumnExpression(*query.columnClauses[p], rawRow, &cache);
            outBatch.columns[baseCols + p].type = v.type;
            outBatch.columns[baseCols + p].data.push_back(v);
        }

        if (whereCol) {
            size_t wh = hashExpression(*query.whereClause);
            Value wv = evalColumnExpression(*query.whereClause, rawRow, &cache);
            outBatch.columns[baseCols + projCols].data.push_back(wv);
        }

        outBatch.num_rows++;
    }

    return SELECT_TABLE_ERROR::NONE;
}

SELECT_TABLE_ERROR validateOrderByAndLimit(const std::vector<MixBatch> &batches,
                                          const std::vector<OrderByExpression> &orderBy,
                                          const std::optional<size_t> &limit) {

    if (batches.empty()) return SELECT_TABLE_ERROR::NONE;
    size_t numCols = batches[0].columns.size();
    for (const auto &o : orderBy) {
        if (o.columnIndex >= numCols) {
            return SELECT_TABLE_ERROR::INVALID_ORDER_BY;
        }
    }
    return SELECT_TABLE_ERROR::NONE;
}

SELECT_TABLE_ERROR orderAndLimitResult(std::vector<MixBatch> &batches,
                         const std::vector<OrderByExpression> &orderBy,
                         const std::optional<size_t> &limit) {

    auto v = validateOrderByAndLimit(batches, orderBy, limit);
    if (v != SELECT_TABLE_ERROR::NONE) return v;

    using json = nlohmann::ordered_json;

    if (batches.empty()) return SELECT_TABLE_ERROR::NONE;

    size_t totalRows = 0;
    for (const auto &b : batches) totalRows += b.num_rows;
    if (totalRows == 0) return SELECT_TABLE_ERROR::NONE;

    size_t numCols = batches[0].columns.size();

    size_t estPerInt = 8;
    size_t estPerBool = 1;
    size_t estPerVarchar = 24;

    size_t estRowSize = 0;
    for (size_t c = 0; c < numCols; ++c) {
        switch (batches[0].columns[c].type) {
            case ValueType::INT64: estRowSize += estPerInt; break;
            case ValueType::VARCHAR: estRowSize += estPerVarchar; break;
            case ValueType::BOOL: estRowSize += estPerBool; break;
        }
    }

    size_t estimatedBytes = estRowSize * totalRows;
    auto rowCompare = [&](const ResultRow &a, const ResultRow &b) {
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
    };

    if (estimatedBytes <= MEMORY_LIMIT) {
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

    if (!orderBy.empty()) std::sort(allRows.begin(), allRows.end(), rowCompare);
    
        size_t take = allRows.size();
        if (limit.has_value() && take > limit.value()) take = limit.value();

        MixBatch finalBatch;
        finalBatch.num_rows = take;
        finalBatch.columns.resize(numCols);
        for (size_t c = 0; c < numCols; ++c) {
            if (!allRows.empty()) finalBatch.columns[c].type = allRows[0].values[c].type;
            finalBatch.columns[c].data.reserve(take);
            for (size_t r = 0; r < take; ++r) finalBatch.columns[c].data.push_back(allRows[r].values[c]);
        }

        batches.clear();
        batches.push_back(std::move(finalBatch));
        return SELECT_TABLE_ERROR::NONE;
    }

    log_info(std::string("orderAndLimitResult: using external merge-sort"));
    auto serializeRow = [&](const ResultRow &row) {
        json j = json::array();
        for (const auto &v : row.values) {
            switch (v.type) {
                case ValueType::INT64: j.push_back(v.intValue); break;
                case ValueType::VARCHAR: j.push_back(v.stringValue); break;
                case ValueType::BOOL: j.push_back(v.boolValue); break;
            }
        }
        return j.dump();
    };

    auto deserializeRow = [&](const std::string &line) {
        json j = json::parse(line);
        ResultRow row;
        row.values.reserve(j.size());
        for (size_t i = 0; i < j.size(); ++i) {
            const auto &cell = j[i];
            if (cell.is_string()) row.values.push_back(Value{ValueType::VARCHAR, 0, cell.get<std::string>(), false});
            else if (cell.is_boolean()) row.values.push_back(Value{ValueType::BOOL, 0, std::string(), cell.get<bool>()});
            else row.values.push_back(Value{ValueType::INT64, cell.get<int64_t>(), std::string(), false});
        }
        return row;
    };

    std::vector<std::string> runFiles;
    std::vector<ResultRow> runRows;

    size_t runRowLimit = std::max<size_t>(1, MEMORY_LIMIT / 4 / std::max<size_t>(1, estRowSize));

    auto flushRun = [&](void) {
        if (runRows.empty()) return;
        if (!orderBy.empty()) std::sort(runRows.begin(), runRows.end(), rowCompare);
        char tmpl[] = "batches/runXXXXXX";
        int fd = mkstemp(tmpl);
        if (fd == -1) throw std::runtime_error("mkstemp failed");
        close(fd);
        std::string path = std::string(tmpl);
        std::ofstream ofs(path);
        for (const auto &r : runRows) ofs << serializeRow(r) << '\n';
        ofs.close();
        runFiles.push_back(path);
        runRows.clear();
    };

    for (const auto &b : batches) {
        for (size_t r = 0; r < b.num_rows; ++r) {
            ResultRow row;
            row.values.reserve(numCols);
            for (size_t c = 0; c < numCols; ++c) {
                if (r < b.columns[c].data.size()) row.values.push_back(b.columns[c].data[r]);
                else row.values.push_back(Value{ValueType::INT64, 0, std::string(), false});
            }
            runRows.push_back(std::move(row));
            if (runRows.size() >= runRowLimit) flushRun();
        }
    }
    flushRun();

    struct HeapItem { ResultRow row; size_t fileIdx; };
    auto cmpHeap = [&](const HeapItem &a, const HeapItem &b) { return rowCompare(b.row, a.row); };
    std::priority_queue<HeapItem, std::vector<HeapItem>, decltype(cmpHeap)> heap(cmpHeap);

    std::vector<std::ifstream> ifs;
    ifs.reserve(runFiles.size());
    for (const auto &p : runFiles) ifs.emplace_back(p);

    for (size_t i = 0; i < ifs.size(); ++i) {
        std::string line;
        if (std::getline(ifs[i], line)) {
            heap.push(HeapItem{deserializeRow(line), i});
        }
    }

    std::vector<ResultRow> merged;
    merged.reserve(limit.value_or(1024));
    while (!heap.empty() && (!limit.has_value() || merged.size() < limit.value())) {
        auto top = heap.top(); heap.pop();
        merged.push_back(std::move(top.row));
        size_t idx = top.fileIdx;
        std::string line;
        if (std::getline(ifs[idx], line)) heap.push(HeapItem{deserializeRow(line), idx});
    }

    for (size_t i = 0; i < ifs.size(); ++i) {
        ifs[i].close();
        std::remove(runFiles[i].c_str());
    }

    size_t take = merged.size();
    MixBatch finalBatch;
    finalBatch.num_rows = take;
    finalBatch.columns.resize(numCols);
    if (!merged.empty()) for (size_t c = 0; c < numCols; ++c) finalBatch.columns[c].type = merged[0].values[c].type;
    for (size_t c = 0; c < numCols; ++c) {
        finalBatch.columns[c].data.reserve(take);
        for (size_t r = 0; r < take; ++r) finalBatch.columns[c].data.push_back(merged[r].values[c]);
    }

    batches.clear();
    batches.push_back(std::move(finalBatch));
    return SELECT_TABLE_ERROR::NONE;
}

std::string spillBatchesToRun(const std::vector<MixBatch> &batches, const std::vector<OrderByExpression> &orderBy) {
    size_t numCols = 0;
    if (!batches.empty()) numCols = batches[0].columns.size();
    std::vector<ResultRow> rows;
    for (const auto &b : batches) {
        for (size_t r = 0; r < b.num_rows; ++r) {
            ResultRow row;
            row.values.reserve(numCols);
            for (size_t c = 0; c < numCols; ++c) {
                if (r < b.columns[c].data.size()) row.values.push_back(b.columns[c].data[r]);
                else row.values.push_back(Value{ValueType::INT64, 0, std::string(), false});
            }
            rows.push_back(std::move(row));
        }
    }

    auto rowCompareLocal = [&](const ResultRow &a, const ResultRow &b) {
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
            } else cmp = std::to_string(va.intValue).compare(std::to_string(vb.intValue));
            if (cmp != 0) return o.ascending ? (cmp < 0) : (cmp > 0);
        }
        return false;
    };

    if (!orderBy.empty()) std::sort(rows.begin(), rows.end(), rowCompareLocal);

    char tmpl[] = "batches/runXXXXXX";
    int fd = mkstemp(tmpl);
    if (fd == -1) throw std::runtime_error("mkstemp failed in spillBatchesToRun");
    close(fd);
    std::string path = std::string(tmpl);
    std::ofstream ofs(path);
    for (const auto &r : rows) {
        json j = json::array();
        for (const auto &v : r.values) {
            switch (v.type) {
                case ValueType::INT64: j.push_back(v.intValue); break;
                case ValueType::VARCHAR: j.push_back(v.stringValue); break;
                case ValueType::BOOL: j.push_back(v.boolValue); break;
            }
        }
        ofs << j.dump() << '\n';
    }
    ofs.close();
    return path;
}

MixBatch mergeRunFiles(const std::vector<std::string> &runFiles, const std::vector<OrderByExpression> &orderBy, const std::optional<size_t> &limit) {
    struct HeapItem { ResultRow row; size_t fileIdx; };
    auto rowCompareLocal = [&](const ResultRow &a, const ResultRow &b) {
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
            } else cmp = std::to_string(va.intValue).compare(std::to_string(vb.intValue));
            if (cmp != 0) return o.ascending ? (cmp < 0) : (cmp > 0);
        }
        return false;
    };

    auto cmpHeap = [&](const HeapItem &a, const HeapItem &b) { return rowCompareLocal(b.row, a.row); };
    std::priority_queue<HeapItem, std::vector<HeapItem>, decltype(cmpHeap)> heap(cmpHeap);

    std::vector<std::ifstream> ifs;
    ifs.reserve(runFiles.size());
    for (const auto &p : runFiles) ifs.emplace_back(p);

    auto deserializeRowLocal = [&](const std::string &line) {
        json j = json::parse(line);
        ResultRow row;
        row.values.reserve(j.size());
        for (size_t i = 0; i < j.size(); ++i) {
            const auto &cell = j[i];
            if (cell.is_string()) row.values.push_back(Value{ValueType::VARCHAR, 0, cell.get<std::string>(), false});
            else if (cell.is_boolean()) row.values.push_back(Value{ValueType::BOOL, 0, std::string(), cell.get<bool>()});
            else row.values.push_back(Value{ValueType::INT64, cell.get<int64_t>(), std::string(), false});
        }
        return row;
    };
    for (size_t i = 0; i < ifs.size(); ++i) {
        std::string line;
        if (std::getline(ifs[i], line)) {
            heap.push(HeapItem{deserializeRowLocal(line), i});
        }
    }

    std::vector<ResultRow> merged;
    merged.reserve(limit.value_or(1024));
    while (!heap.empty() && (!limit.has_value() || merged.size() < limit.value())) {
        auto top = heap.top(); heap.pop();
        merged.push_back(std::move(top.row));
        size_t idx = top.fileIdx;
        std::string line;
        if (std::getline(ifs[idx], line)) heap.push(HeapItem{deserializeRowLocal(line), idx});
    }

    for (size_t i = 0; i < ifs.size(); ++i) {
        ifs[i].close();
        std::remove(runFiles[i].c_str());
    }

    MixBatch finalBatch;
    if (merged.empty()) return finalBatch;
    size_t numCols = merged[0].values.size();
    finalBatch.num_rows = merged.size();
    finalBatch.columns.resize(numCols);
    for (size_t c = 0; c < numCols; ++c) finalBatch.columns[c].type = merged[0].values[c].type;
    for (size_t c = 0; c < numCols; ++c) {
        finalBatch.columns[c].data.reserve(merged.size());
        for (size_t r = 0; r < merged.size(); ++r) finalBatch.columns[c].data.push_back(merged[r].values[c]);
    }
    return finalBatch;
}

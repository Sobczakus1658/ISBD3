#include "executionService.h"
#include "../queries/queries.h"
#include "../results/results.h"
#include "../serialization/deserializator.h"
#include "../query/executor/selectExecutor.h"
#include "../query/planer/selectPlaner.h"
#include "../query/selectQuery.h"
#include "../query/evaluation/expression_cache.h"
#include "../query/evaluation/evalColumnExpression.h"
#include <csv.hpp>
#include <random>
#include <iostream>
#include "../utils/utils.h"


namespace fs = std::filesystem;
size_t MEMORY_LIMIT = 4ULL * 1024ULL * 1024ULL;

Batch make_empty_batch(size_t numIntCols, size_t numStrCols) {
    Batch b;
    b.intColumns.resize(numIntCols);
    b.stringColumns.resize(numStrCols);
    b.num_rows = 0;
    return b;
}

std::string get_path(const TableInfo &info){
    if (!info.location.empty()) {
        fs::path p(info.location);
        try {
            if (!fs::exists(p)) fs::create_directories(p);
        } catch (const std::exception &e) {
            log_error(std::string("get_path: cannot create directory for location: ") + e.what());
        }
        return info.location;
    }

    fs::path tableDir = fs::path(base) / info.name;
    try {
        if (!fs::exists(tableDir)) {
            fs::create_directories(tableDir);
        }
    } catch (const std::exception &e) {
        log_error(std::string("get_path: cannot create table dir: ") + e.what());
    }
    return tableDir;
}

void revert_path(fs::path tableDir){
    try {
        if (fs::exists(tableDir) && fs::is_directory(tableDir)) {
            if (fs::is_empty(tableDir)) {
                fs::remove(tableDir);
            }
        }
    } catch (const std::exception &e) {
        std::cerr << "revert_path: filesystem error: " << e.what() << std::endl;
    }
}
QueryCreatedResponse copyCSV(CopyQuery q, string query_id) {
    QueryCreatedResponse response;
    if (q.destinationTableName.empty()) {
        response.status = CSV_TABLE_ERROR::TABLE_NOT_FOUND;
        return response;
    }

    std::optional<TableInfo> infoOpt = getTableInfoByName(q.destinationTableName);
    if (!infoOpt) {
        response.status = CSV_TABLE_ERROR::TABLE_NOT_FOUND;
        return response;
    }
    TableInfo info = *infoOpt;
    std::vector<std::string> colTypes;
    std::string path = get_path(info);
    std::vector<std::string> fileNames;
    size_t files_reported = 0;

    colTypes.reserve(info.info.size());

    for (const auto& col : info.info) {
        colTypes.push_back(col.second);
    }

    size_t intCount = 0;
    size_t strCount = 0;

    std::vector<std::string> intNames;
    std::vector<std::string> strNames;
    for (const auto &col : info.info) {
        if (col.second == "INT64") {
            intNames.push_back(col.first);
        } else {
            strNames.push_back(col.first);
        }
    }

    for (const auto& t : colTypes) {
        if (t == "INT64")
            intCount++;
        else 
            strCount++;
        
    }

    csv::CSVFormat format;
    if (q.doesCsvContainHeader)
        format.header_row(0);
    else
        format.no_header();

    try {
        std::ifstream fin(q.path);
        if (fin) {
            std::string firstLine;
            while (std::getline(fin, firstLine)) {
                if (!firstLine.empty()) break;
            }
            if (!firstLine.empty()) {
                size_t commaCount = std::count(firstLine.begin(), firstLine.end(), ',');
                size_t semiCount = std::count(firstLine.begin(), firstLine.end(), ';');
                if (semiCount > commaCount) {
                    format.delimiter(';');
                } else {
                    format.delimiter(',');
                }
            }
        }
    } catch (const std::exception &e) {
    }

    std::vector<Batch> batches;
    Batch batch = make_empty_batch(intCount, strCount);
    for (size_t i = 0; i < batch.intColumns.size() && i < intNames.size(); ++i) batch.intColumns[i].name = intNames[i];
    for (size_t i = 0; i < batch.stringColumns.size() && i < strNames.size(); ++i) batch.stringColumns[i].name = strNames[i];

    if (!fs::exists(q.path)) {
        response.status = CSV_TABLE_ERROR::FILE_NOT_FOUND;
        revert_path(path);
        return response;
    }
    csv::CSVReader reader(q.path, format);
    
    bool send = false;

    std::vector<std::pair<int,int>> csvToTable;

    if (!q.destinationColumns.empty()) {

        std::unordered_map<std::string, int> tableNameToIndex;
        for (size_t i = 0; i < info.info.size(); i++) {
            tableNameToIndex[info.info[i].first] = i;
        }

        std::unordered_map<std::string, int> headerNameToIndex;
        if (q.doesCsvContainHeader) {
            auto col_names = reader.get_col_names();
            for (size_t i = 0; i < col_names.size(); ++i) {
                headerNameToIndex[col_names[i]] = (int)i;
            }
        }

        int implicitCsvIdx = 0;
        for (const auto &colName : q.destinationColumns) {
            if (tableNameToIndex.find(colName) == tableNameToIndex.end()) {
                response.status = CSV_TABLE_ERROR::INVALID_DESTINATION_COLUMN;
                revert_path(path);
                return response;
            }
            int tableIdx = tableNameToIndex[colName];

            int csvIdx = -1;
            if (q.doesCsvContainHeader) {
                if (headerNameToIndex.find(colName) == headerNameToIndex.end()) {
                    response.status = CSV_TABLE_ERROR::INVALID_DESTINATION_COLUMN;
                    revert_path(path);
                    return response;
                }
                csvIdx = headerNameToIndex[colName];
            } else {
                csvIdx = implicitCsvIdx++;
            }

            csvToTable.push_back({csvIdx, tableIdx});
        }

    } else {
        for (size_t i = 0; i < colTypes.size(); i++) {
            csvToTable.push_back({(int)i, (int)i});
        }
    }

    for (csv::CSVRow& row : reader) {

        size_t columnsToProcess = csvToTable.size();

        size_t intIdx = 0, strIdx = 0;
        for (size_t i = 0; i < columnsToProcess; i++) {
            int csv_i = csvToTable[i].first;
            int tbl_i = csvToTable[i].second;
            const auto &colType = colTypes[tbl_i];
            try {
                if (csv_i < 0 || (size_t)csv_i >= row.size()) {
                    response.status = CSV_TABLE_ERROR::INVALID_TYPE;
                    revert_path(path);
                    return response;
                }
                if (colType == "INT64") {
                    auto val = row[csv_i].get<std::int64_t>();
                    batch.intColumns[intIdx].column.push_back(val);
                    intIdx++;
                } else {
                    auto sval = row[csv_i].get<std::string>();
                    batch.stringColumns[strIdx].column.push_back(sval);
                    strIdx++;
                }
            } catch (const std::exception &e) {
                response.status = CSV_TABLE_ERROR::INVALID_TYPE;
                revert_path(path);
                return response;
            }
        }

        batch.num_rows++;

        if (batch.num_rows == BATCH_SIZE) {
            batches.push_back(std::move(batch));
            batch = make_empty_batch(intCount, strCount);
            for (size_t i = 0; i < batch.intColumns.size() && i < intNames.size(); ++i) batch.intColumns[i].name = intNames[i];
            for (size_t i = 0; i < batch.stringColumns.size() && i < strNames.size(); ++i) batch.stringColumns[i].name = strNames[i];
            if (batches.size() > BATCH_NUMBER){
                if (!send) {
                    changeStatus(query_id, QueryStatus::RUNNING);
                    send = !send;
                }
                for (auto &bb : batches) {
                    for (size_t i = 0; i < bb.intColumns.size() && i < intNames.size(); ++i) bb.intColumns[i].name = intNames[i];
                    for (size_t i = 0; i < bb.stringColumns.size() && i < strNames.size(); ++i) bb.stringColumns[i].name = strNames[i];
                }
                std::vector<std::string> tmp = std::move(serializator(batches, path, PART_LIMIT));
                fileNames.insert(fileNames.end(), tmp.begin(), tmp.end());
                batches.clear();
            }
        }
    }

    if (!batches.empty() || batch.num_rows > 0) {
        if (batch.num_rows > 0) {
            batches.push_back(std::move(batch));
        }

        if (!send) {
            changeStatus(query_id, QueryStatus::RUNNING);
            send = !send;
        }

        for (auto &bb : batches) {
            for (size_t i = 0; i < bb.intColumns.size() && i < intNames.size(); ++i) bb.intColumns[i].name = intNames[i];
            for (size_t i = 0; i < bb.stringColumns.size() && i < strNames.size(); ++i) bb.stringColumns[i].name = strNames[i];
        }

        std::vector<std::string> tmp = std::move(serializator(batches, path, PART_LIMIT));
        fileNames.insert(fileNames.end(), tmp.begin(), tmp.end());
        batches.clear();
    }

    addLocationAndFiles(info.id, path, fileNames);
    response.status = CSV_TABLE_ERROR::NONE;
    return response;
}

SELECT_TABLE_ERROR selectTable(const SelectQuery &select_query, string queryId){
    SelectQuery &sq = const_cast<SelectQuery&>(select_query);
    auto exprUsesColumnRef = [&](const ColumnExpression &expr) {
        std::function<bool(const ColumnExpression&)> visit;
        visit = [&](const ColumnExpression &e) -> bool {
            switch (e.type) {
                case ExprType::COLUMN_REF: return true;
                case ExprType::LITERAL: return false;
                case ExprType::UNARY_OP:
                    if (e.unary.operand) return visit(*e.unary.operand);
                    return false;
                case ExprType::BINARY_OP:
                    if (e.binary.left && visit(*e.binary.left)) return true;
                    if (e.binary.right && visit(*e.binary.right)) return true;
                    return false;
                case ExprType::FUNCTION:
                    for (const auto &arg : e.function.args) if (arg && visit(*arg)) return true;
                    return false;
            }
            return false;
        };
        return visit(expr);
    };

    TableInfo info;
    bool haveTableInfo = false;
    if (sq.tableName.empty()) {
        bool usesCols = false;
        if (sq.whereClause) usesCols = usesCols || exprUsesColumnRef(*sq.whereClause);
        for (const auto &pc : sq.columnClauses) if (pc) usesCols = usesCols || exprUsesColumnRef(*pc);

        if (usesCols) {
            auto tables = getTables();
            if (tables.size() == 1) {
                sq.tableName = tables.begin()->second;
            } else {
                return SELECT_TABLE_ERROR::TABLE_NOT_EXISTS;
            }
        } else {
            info.name = std::string();
            info.id = 0;
            info.info.clear();
            info.location.clear();
            info.files.clear();
            haveTableInfo = true;
        }
    }

    if (!haveTableInfo) {
        std::optional<TableInfo> infoOpt = getTableInfoByName(sq.tableName);
        if (!infoOpt) {
            return SELECT_TABLE_ERROR::TABLE_NOT_EXISTS;
        }
        info = *infoOpt;
    }

    auto planResult = planSelectQuery(sq, info);

    if (planResult != SELECT_TABLE_ERROR::NONE) {
        return planResult;
    }


    changeStatus(queryId, QueryStatus::RUNNING);
    initResult(queryId);

    std::vector<MixBatch> accumulatedBatches;
    std::vector<std::string> runFiles;

    if (info.name.empty() && info.files.empty()) {
        size_t projCols = select_query.columnClauses.size();
        MixBatch mb;
        mb.num_rows = 1;
        mb.columns.resize(projCols);

        ExpressionCache cache;
        ResultRow row;
        row.values.clear();

        for (size_t p = 0; p < projCols; ++p) {
            const auto &exprPtr = select_query.columnClauses[p];
            if (!exprPtr) continue;
            Value v = evalColumnExpression(*exprPtr, row, &cache);
            ColumnData cd;
            cd.type = exprPtr->resultType;
            cd.data.push_back(v);
            mb.columns[p] = std::move(cd);
        }

        std::vector<MixBatch> outVec;
        outVec.push_back(std::move(mb));
        modifyResult(queryId, outVec);
        return SELECT_TABLE_ERROR::NONE;
    }


    auto estimateBatchesBytes = [](const std::vector<MixBatch> &batches) {
        size_t bytes = 0;
        for (const auto &b : batches) {
            for (const auto &col : b.columns) {
                bytes += sizeof(col);
                bytes += col.data.size() * sizeof(decltype(col.data)::value_type);
                if (col.type == ValueType::VARCHAR) {
                    for (const auto &v : col.data) bytes += v.stringValue.size();
                }
            }
            bytes += sizeof(b.num_rows);
        }
        return bytes;
    };

    for (const auto &f : info.files) {
        std::string path = info.location;
        if (!path.empty() && path.back() != '/' && path.back() != '\\') path.push_back('/');
        path += f;

        std::vector<Batch> batches = move(deserializator(path));

        for (auto &batch : batches) {
            SELECT_TABLE_ERROR r = executeSelectBatch(select_query, batch, accumulatedBatches);
            if (r != SELECT_TABLE_ERROR::NONE) {
                log_error(std::string("selectTable: executeSelectBatch returned error code ") + std::to_string((int)r));
            }
            size_t est = estimateBatchesBytes(accumulatedBatches);
            size_t accRows = 0;
            for (const auto &b : accumulatedBatches) accRows += b.num_rows;
            if (est > MEMORY_LIMIT) {
                try {
                    std::string runPath = spillBatchesToRun(accumulatedBatches, select_query.orderByClauses);
                    runFiles.push_back(runPath);
                    accumulatedBatches.clear();
                } catch (const std::exception &e) {
                    log_error(std::string("spillBatchesToRun failed: ") + e.what());
                }
            }
        }
    }

    if (!runFiles.empty()) {
        if (!accumulatedBatches.empty()) {
            try {
                std::string runPath = spillBatchesToRun(accumulatedBatches, select_query.orderByClauses);
                runFiles.push_back(runPath);
                accumulatedBatches.clear();
            } catch (const std::exception &e) {
                log_error(std::string("spillBatchesToRun failed for trailing batches: ") + e.what());
            }
        }
        MixBatch finalBatch = mergeRunFiles(runFiles, select_query.orderByClauses, select_query.limit);
        std::vector<MixBatch> outVec;
        outVec.push_back(std::move(finalBatch));
        size_t baseCols = info.info.size();
        size_t projCols = select_query.columnClauses.size();
        std::vector<MixBatch> projectedOut;
        projectedOut.reserve(outVec.size());
        for (const auto &mb : outVec) {
            MixBatch pm;
            pm.num_rows = mb.num_rows;
            pm.columns.resize(projCols);
            for (size_t p = 0; p < projCols; ++p) {
                if (baseCols + p < mb.columns.size()) pm.columns[p] = mb.columns[baseCols + p];
                else pm.columns[p] = ColumnData();
            }
            projectedOut.push_back(std::move(pm));
        }
        modifyResult(queryId, projectedOut);
    } else {
        SELECT_TABLE_ERROR ord = orderAndLimitResult(accumulatedBatches, select_query.orderByClauses, select_query.limit);
        if (ord != SELECT_TABLE_ERROR::NONE) return ord;
        size_t tot = 0; for (const auto &b : accumulatedBatches) tot += b.num_rows;
        size_t baseCols2 = info.info.size();
        size_t projCols2 = select_query.columnClauses.size();
        std::vector<MixBatch> projectedAcc;
        projectedAcc.reserve(accumulatedBatches.size());
        for (const auto &mb : accumulatedBatches) {
            MixBatch pm;
            pm.num_rows = mb.num_rows;
            pm.columns.resize(projCols2);
            for (size_t p = 0; p < projCols2; ++p) {
                if (baseCols2 + p < mb.columns.size()) pm.columns[p] = mb.columns[baseCols2 + p];
                else pm.columns[p] = ColumnData();
            }
            projectedAcc.push_back(std::move(pm));
        }
        modifyResult(queryId, projectedAcc);
    }

    return SELECT_TABLE_ERROR::NONE;
}



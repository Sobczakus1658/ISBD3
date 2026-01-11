#include "executionService.h"
#include "../queries/queries.h"
#include "../results/results.h"
#include "../serialization/deserializator.h"
#include "../query/executor/selectExecutor.h"
#include "../query/planer/selectPlaner.h"
#include <csv.hpp>
#include <random>
#include <iostream>
#include "../utils/utils.h"


namespace fs = std::filesystem;

Batch make_empty_batch(size_t numIntCols, size_t numStrCols) {
    Batch b;
    b.intColumns.resize(numIntCols);
    b.stringColumns.resize(numStrCols);
    b.num_rows = 0;
    return b;
}

std::string get_path(const TableInfo &info){
    if (!info.location.empty()) {
        return info.location;
    }

    fs::path tableDir = fs::path(base) / info.name;
    if (!fs::exists(tableDir)) {
        fs::create_directories(tableDir);
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

    for (const auto& t : colTypes) {
        if (t == "INT64")
            intCount++;
        else 
            strCount++;
        
    }

    csv::CSVFormat format;
    format.delimiter(',').quote('"');
    if (q.doesCsvContainHeader)
        format.header_row(0);
    else
        format.no_header();


    std::vector<Batch> batches;
    Batch batch = make_empty_batch(intCount, strCount);

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
            for (size_t i = 0; i < col_names.size(); ++i) headerNameToIndex[col_names[i]] = (int)i;
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
            if (batches.size() > BATCH_NUMBER){
                if (!send) {
                    changeStatus(query_id, QueryStatus::RUNNING);
                    send = !send;
                }
                std::vector<std::string> tmp = std::move(serializator(batches, path, PART_LIMIT));
                fileNames.insert(fileNames.end(), tmp.begin(), tmp.end());
                batches.clear();
            }
        }
    }
    if (batch.num_rows > 0) {
        batches.push_back(std::move(batch));
        if (!send) {
            changeStatus(query_id, QueryStatus::RUNNING);
            send = !send;
        }
        std::vector<std::string> tmp = std::move(serializator(batches, path, PART_LIMIT));
        fileNames.insert(fileNames.end(), tmp.begin(), tmp.end());
    }

    addLocationAndFiles(info.id, path, fileNames);
    response.status = CSV_TABLE_ERROR::NONE;
    return response;
}

SELECT_TABLE_ERROR selectTable(const SelectQuery &select_query, string queryId){
    std::optional<TableInfo> infoOpt = getTableInfoByName(select_query.tableName);
    if (!infoOpt) {
        return SELECT_TABLE_ERROR::TABLE_NOT_EXISTS;
    }

    TableInfo info = *infoOpt;

    auto planResult = planSelectQuery(const_cast<SelectQuery&>(select_query), info);

    if (planResult != SELECT_TABLE_ERROR::NONE) {
        return planResult;
    }

    changeStatus(queryId, QueryStatus::RUNNING);
    initResult(queryId);

    std::vector<MixBatch> accumulatedBatches;
    for (const auto &f : info.files) {
        std::string path = info.location;
        if (!path.empty() && path.back() != '/' && path.back() != '\\') path.push_back('/');
        path += f;

        std::vector<Batch> batches = move(deserializator(path));

        for (auto &batch : batches) {
            executeSelectBatch(select_query, batch, accumulatedBatches);
        }
    }

    orderAndLimitResult(accumulatedBatches, select_query.orderByClauses, select_query.limit);
    modifyResult(queryId, accumulatedBatches);

    return SELECT_TABLE_ERROR::NONE;
}



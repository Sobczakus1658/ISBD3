#include "executionService.h"
#include "../queries/queries.h"
#include "../serialization/deserializator.h"
#include <csv.hpp>
#include <random>


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

QueryCreatedResponse copyCSV(CopyQuery q, string query_id) {
    std::optional<TableInfo> infoOpt = getTableInfoByName(q.destinationTableName);
    if (!infoOpt) {
        throw std::runtime_error("Table not found: " + q.destinationTableName);
    }
    TableInfo info = *infoOpt;
    std::vector<std::string> colTypes;
    std::string path = get_path(info);
    std::vector<std::string> fileNames;
    QueryCreatedResponse response;

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
        return response;
    }
    csv::CSVReader reader(q.path, format);
    
    uint64_t intIdx = 0;
    uint64_t strIdx = 0;
    bool send = false;

    for (csv::CSVRow& row : reader) {

        if (row.size() != colTypes.size()) {
            response.status = CSV_TABLE_ERROR::INVALID_COLUMN_NUMBER;
            return response;
        }

        size_t intIdx = 0, strIdx = 0;

        for (size_t i = 0; i < row.size(); i++) {
            const auto& colType = colTypes[i];
            try {
                if (colType == "INT64") {
                    auto val = row[i].get<std::int64_t>();
                    batch.intColumns[intIdx].column.push_back(val);
                    intIdx++;
                } else {
                    auto sval = row[i].get<std::string>();
                    batch.stringColumns[strIdx].column.push_back(sval);
                    strIdx++;
                } 
            } catch (const std::exception &e) {
                response.status = CSV_TABLE_ERROR::INVALID_TYPE;
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

SELECT_TABLE_ERROR selectTable(SelectQuery select_query, string queryId){
    std::optional<TableInfo> infoOpt = getTableInfoByName(select_query.tableName);
    if (!infoOpt) {
        return SELECT_TABLE_ERROR::TABLE_NOT_EXISTS;
    }
    TableInfo info = *infoOpt;
    changeStatus(queryId, QueryStatus::RUNNING);
    for (const auto &f : info.files) {
        std::string path = info.location;
        if (!path.empty() && path.back() != '/' && path.back() != '\\') path.push_back('/');
        path += f;

        std::vector<Batch> batches = move(deserializator(path));
        modifyQuery(queryId, batches);
    }
    return SELECT_TABLE_ERROR::NONE;
}


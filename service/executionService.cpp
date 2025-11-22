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

QueryCreatedResponse copyCSV(CopyQuery q){
    std::optional<TableInfo> infoOpt = getTableInfo(q.destinationTableName);
    if (!infoOpt) {
        throw std::runtime_error("Table not found: " + q.destinationTableName);
    }
    TableInfo info = *infoOpt;
    std::vector<std::string> colTypes;
    std::string path = get_path(info);
    std::vector<std::string> fileNames;

    colTypes.reserve(info.info.size());

    for (const auto& col : info.info) {
        colTypes.push_back(col.second);
    }

    size_t intCount = 0;
    size_t strCount = 0;

    for (const auto& t : colTypes) {
        if (t == "INT64")
            intCount++;
        else if (t == "VARCHAR")
            strCount++;
        else
            throw std::runtime_error("Unsupported column type: " + t);
    }

    csv::CSVFormat format;
    format.delimiter(',').quote('"');
    if (q.doesCsvContainHeader)
        format.header_row(0);
    else
        format.no_header();


    std::vector<Batch> batches;
    Batch batch = make_empty_batch(intCount, strCount);

    csv::CSVReader reader(q.path, format);
    
    uint64_t intIdx = 0;
    uint64_t strIdx = 0;

    for (csv::CSVRow& row : reader) {

        if (row.size() != colTypes.size()) {
            throw std::runtime_error("Expected number of columns are different that expected");
        }

        size_t intIdx = 0, strIdx = 0;

        for (size_t i = 0; i < row.size(); i++) {
            const auto& colType = colTypes[i];
            if (colType == "INT64") {
                batch.intColumns[intIdx].column.push_back(row[i].get<std::int64_t>());
                intIdx++;
            } 
            else if (colType == "VARCHAR") {
                batch.stringColumns[strIdx].column.push_back(row[i].get<std::string>());
                strIdx++;
            } 
            else {
                throw std::runtime_error("Unsupported type: " + colType);
            }
        }

        batch.num_rows++;

        if (batch.num_rows == BATCH_SIZE) {
            batches.push_back(std::move(batch));
            batch = make_empty_batch(intCount, strCount);
            if (batches.size() > BATCH_NUMBER){
                            std::vector<std::string> tmp = std::move(serializator(batches, path, PART_LIMIT));
                fileNames.insert(fileNames.end(), tmp.begin(), tmp.end());
            }
            batches.clear();
        }
    }
    if (batch.num_rows > 0) {
        batches.push_back(std::move(batch));
        std::vector<std::string> tmp = std::move(serializator(batches, path, PART_LIMIT));
        fileNames.insert(fileNames.end(), tmp.begin(), tmp.end());
    }

    addLocationAndFiles(info.id, path, fileNames);

    QueryCreatedResponse empty;
    empty.success = true;
    return empty;
}

std::string selectTable(std::string name){
    std::optional<TableInfo> infoOpt = getTableInfo(name);
    if (!infoOpt) {
        throw std::runtime_error("Table not found: " + name);
    }
    TableInfo info = *infoOpt;

    std::random_device rd;
    std::mt19937_64 gen(rd());
    uint64_t id = gen();
    string queryId = std::to_string(id);
    initQuery(queryId);
    for (const auto &f : info.files) {
        std::string path = info.location;
        if (!path.empty() && path.back() != '/' && path.back() != '\\') path.push_back('/');
        path += f;

        std::vector<Batch> batches = move(deserializator(path));
        modifyQuery(queryId, batches);
    }
    return queryId;
}


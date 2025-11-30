#pragma once

#include <vector>
#include <string>
#include <cstdint>
#include <cstddef>
#include <filesystem>
#include <variant>
#include <optional>

using namespace std;
inline constexpr size_t BATCH_SIZE = 8192;
inline constexpr size_t BATCH_NUMBER = 10;
inline constexpr int compresion_level = 3;
inline constexpr uint32_t file_magic = 0x21374201;
inline constexpr uint32_t batch_magic = 0x69696969;
static constexpr uint8_t INTEGER = 0;
static constexpr uint8_t STRING  = 1;
static constexpr uint64_t PART_LIMIT = 3500ULL * 1024ULL * 1024ULL;
static constexpr uint64_t SHORTER_LIMIT = 3500ULL * 1024ULL;
static const std::string base = std::filesystem::current_path() / "batches/";

enum class CREATE_TABLE_ERROR {
    NONE,
    TABLE_EXISTS,
    INVALID_COLUMN_TYPE
};

enum class SELECT_TABLE_ERROR {
    NONE,
    TABLE_NOT_EXISTS
};

struct Problem {
    std::string error;
    std::optional<std::string> context;
};

struct CreateTableResult {
    std::string tableId;
    std::vector<Problem> problem;
};

using Column = std::variant<std::vector<int64_t>, std::vector<std::string>>;

struct QueryResult {
    int rowCount;
    std::vector<Column> columns;
};

struct QueryError {
    std::vector<Problem> problems;
};


using ColumnInfo = pair<uint64_t, uint8_t>;
using ColumnInfoShow = pair<string, string>;

struct IntColumn {
    string name;
    vector<int64_t> column;
};

struct StringColumn {
    string name;
    vector<string> column;
};

struct Batch {
    vector<IntColumn> intColumns;
    vector<StringColumn> stringColumns;
    size_t num_rows;
};

struct CopyQuery {
    string path;
    string destinationTableName;
    vector<string> destinationColumns;
    bool doesCsvContainHeader;
};

struct SelectQuery {
    string tableName;
};

using QueryToJson = std::variant<SelectQuery, CopyQuery>;

enum class QueryType {COPY, SELECT, ERROR};

enum class CSV_TABLE_ERROR{NONE, INVALID_TYPE, FILE_NOT_FOUND, INVALID_COLUMN_NUMBER, TABLE_NOT_FOUND, INVALID_DESTINATION_COLUMN};

enum class QueryStatus{CREATED, PLANNING, RUNNING, COMPLETED, FAILED};


struct QueryResponse {
    string queryId;
    QueryStatus status;
    bool isResultAvailable;
    QueryToJson query;
};

struct QueryCreatedResponse {
    string queryId;
    CSV_TABLE_ERROR status; 
};
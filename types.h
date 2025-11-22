#include <vector>
#pragma once

#include <vector>
#include <string>
#include <cstdint>
#include <cstddef>
#include <filesystem>

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
    TABLE_EXISTS
};

struct CreateTableResult {
    std::string tableId;
    CREATE_TABLE_ERROR error;
};
// avoid pulling entire std namespace into global scope in headers

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

enum class QueryType {COPY, SELECT, ERROR};

enum class QueryStatus{CREATED, PLANNING, RUNNING, COMPLETED, FAILED};

struct QueryCreatedResponse {
    bool success;
};

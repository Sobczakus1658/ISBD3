
#include <string>
#include <vector>
#include <optional>
#include <cstdint>

#include "../types.h"

struct TableInfo {
    uint64_t id;
    string name;
    vector<ColumnInfoShow> info;
    string location;
    vector<string> files;
};

map<uint64_t, string> tablesFromMetastore(); 

std::optional<TableInfo> getTableInfoMetastore(uint64_t id);

// bool deleteTables(uint64_t id); 

// CreateTableResult createTables(const std::string& json_info);
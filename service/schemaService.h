#include <string>
#include <vector>
#include <optional>
#include <cstdint>
#include <map>

#include "../metastore/metastore.h"
#include "../types.h"

map<uint64_t, string> getTables(); 

std::optional<TableInfo> getTableInfo(uint64_t id);

bool deleteTable(uint64_t id); 

CreateTableResult createTable(const std::string& json_info);

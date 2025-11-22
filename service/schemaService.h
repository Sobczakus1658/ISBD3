#include <string>
#include <vector>
#include <optional>
#include <cstdint>
#include <map>

#include "../metastore/metastore.h"
#include "../types.h"
#include <nlohmann/json.hpp>

using json = nlohmann::json;

std::map<uint64_t, std::string> getTables(); 

std::optional<TableInfo> getTableInfoByID(uint64_t id);

bool deleteTable(uint64_t id); 

CreateTableResult createTable(const json& json_info);

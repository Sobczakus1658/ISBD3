#include "schemaService.h"

#include <fstream>
#include <iostream>
#include <nlohmann/json.hpp>

using json = nlohmann::json;

std::map<uint64_t, std::string> getTables() {
    return tablesFromMetastore();
}

std::optional<TableInfo> getTableInfoByID(uint64_t id) {
    return getTableInfoMetastore(id);
}

bool deleteTable(uint64_t id) {
    return deleteTableMetastore(id);
}

CreateTableResult createTable(const json& json_info) {
    return createTableMetastore(json_info);
}


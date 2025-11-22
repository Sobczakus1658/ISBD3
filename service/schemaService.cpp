#include "schemaService.h"

#include <fstream>
#include <iostream>
#include <nlohmann/json.hpp>

using json = nlohmann::json;

json prepareTablesInfo(const map<uint64_t, string>& tables) {
    json response_json = json::array();

    for (const auto& [id, name] : tables) {
        response_json.push_back({
            {"id", id},
            {"name", name}
        });
    }
}


json getTables() {
    return prepareTablesInfo(tablesFromMetastore());
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


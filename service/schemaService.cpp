#include "schemaService.h"

map<uint64_t, std::string> getTables() {
    return tablesFromMetastore();
}

std::optional<TableInfo> getTableInfo(uint64_t id) {
    return getTableInfoMetastore(id);
}

bool deleteTable(uint64_t id) {
    return false;
}

CreateTableResult createTable(const std::string& json_info) {
    return CreateTableResult{ "", CREATE_TABLE_ERROR::NONE};
}
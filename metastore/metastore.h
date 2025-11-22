
#pragma once

#include <string>
#include <vector>
#include <optional>
#include <cstdint>
#include <map>

#include "../types.h"
#include <nlohmann/json.hpp>

using json = nlohmann::json;

struct TableInfo {
    uint64_t id;
    std::string name;
    std::vector<ColumnInfoShow> info;
    std::string location;
    std::vector<std::string> files;
};

std::map<uint64_t, std::string> tablesFromMetastore(); 

std::optional<TableInfo> getTableInfoMetastore(uint64_t id);

std::optional<TableInfo> getTableInfo(const std::string& name); 

bool deleteTableMetastore(uint64_t id); 

CreateTableResult createTableMetastore(const json& json_info);

void addLocationAndFiles(uint64_t id, const std::string& location, const std::vector<std::string>& files); 
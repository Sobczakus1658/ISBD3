
#pragma once

#include <string>
#include <vector>
#include <optional>
#include <cstdint>
#include <map>

#include <nlohmann/json.hpp>
#include "../types.h"

using json = nlohmann::ordered_json;;

struct TableInfo {
    uint64_t id;
    std::string name;
    std::vector<ColumnInfoShow> info;
    std::string location;
    std::vector<std::string> files;
};

std::map<uint64_t, std::string> getTables(); 

std::optional<TableInfo> getTableInfo(uint64_t id);

std::optional<TableInfo> getTableInfoByName(const std::string& name); 

bool deleteTable(uint64_t id); 

CreateTableResult createTable(const json& json_info);

void addLocationAndFiles(uint64_t id, const std::string& location, const std::vector<std::string>& files); 
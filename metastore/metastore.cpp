#include <string>
#include <vector>
#include <optional>
#include <cstdint>
#include <nlohmann/json.hpp>
#include <fstream>
#include <iostream>
#include <filesystem>

#include "metastore.h"
#include "../utils/utils.h"

using namespace std;

static const filesystem::path basePath =  filesystem::current_path() / "metastore/metastore.json";

std::optional<TableInfo> getTableInfoByName(const std::string& name) {
    json meta = readLocalFile(basePath);
    if (!meta.is_object()) {
        meta = json::object();
        meta["tables"] = json::object();
    } else if (!meta.contains("tables") || !meta["tables"].is_object()) {
        meta["tables"] = json::object();
    }
    for (auto &kv : meta["tables"].items()) {
        if (kv.key() == name){
            const json &obj = kv.value();
            TableInfo tableinfo;
            tableinfo.name = name;
            tableinfo.id = stoull(obj["id"].get<string>());
            if (obj.contains("column_order") && obj["column_order"].is_array()) {
                for (const auto &col_name_json : obj["column_order"]) {
                    std::string col_name = col_name_json.get<std::string>();
                    std::string col_type = obj["columns"].at(col_name).get<std::string>();
                    tableinfo.info.emplace_back(col_name, col_type);
                }
            } else {
                for (auto &col : obj["columns"].items()) {
                    std::string col_name = col.key();
                    std::string col_type = col.value().get<string>();
                    tableinfo.info.emplace_back(col_name, col_type);
                }
            }
            if (obj.contains("location") && obj["location"].is_string()) {
                tableinfo.location = obj["location"].get<std::string>();
            } else {
                tableinfo.location = "";
            }
            tableinfo.files.clear();
            if (obj.contains("files") && obj["files"].is_array()) {
                for (const auto &f : obj["files"]) {
                    if (f.is_string()) tableinfo.files.push_back(f.get<std::string>());
                }
            }
            return tableinfo;
        }
    }
    return nullopt;
}

std::optional<TableInfo> getTableInfo(uint64_t id) {
    std::map<uint64_t, std::string> tables = getTables();
    auto it = tables.find(id);
    if (it == tables.end()) return std::nullopt;
    return getTableInfoByName(it->second);
}

map<uint64_t, string> getTables(){
    map<uint64_t, string> names;
    json meta = readLocalFile(basePath);
    if (!meta.is_object()) {
        meta = json::object();
        meta["tables"] = json::object();
    } else if (!meta.contains("tables") || !meta["tables"].is_object()) {
        meta["tables"] = json::object();
    }
    for (auto &kv : meta["tables"].items()) {
        const std::string table_name = kv.key();
        const json &obj = kv.value();
        uint64_t id = stoull(obj["id"].get<string>());
        names.insert({id, table_name});
    }
    return names;
}

bool deleteTable(uint64_t id) {
    map<uint64_t, string> tables = getTables();
    auto it = tables.find(id);
    if (it == tables.end()) return false;
    const std::string name = it->second;

    json meta = readLocalFile(basePath);
    if (!meta.is_object()) {
        meta = json::object();
        meta["tables"] = json::object();
    } else if (!meta.contains("tables") || !meta["tables"].is_object()) {
        meta["tables"] = json::object();
    }
    if (!meta["tables"].contains(name)) return false;

    const json &obj = meta["tables"][name];

    std::string location = obj["location"].get<std::string>();

    std::vector<std::string> files;

    for (const auto &f : obj["files"]) {
        if (f.is_string()) files.push_back(f.get<std::string>());
    }
    removeFiles(location, files);
    meta["tables"].erase(name);
    saveFile(basePath, meta);
    return true;
}

CreateTableResult createTable(const json& json_info) {

    CreateTableResult result;
    std::vector<Problem> problems;
    result.problem = problems;
    auto it = json_info.begin();
    std::string table_name = it.key();
    const json &obj = it.value();

    json meta = readLocalFile(basePath);
    if (!meta.is_object()) {
        meta = json::object();
        meta["tables"] = json::object();
    } else if (!meta.contains("tables") || !meta["tables"].is_object()) {
        meta["tables"] = json::object();
    }

    if (meta["tables"].contains(table_name)) {
        Problem p; p.error = "Table with name " + table_name + " already exists !";
        problems.push_back(p);
        result.problem = problems;
        return result;
    }

    vector<string> duplicates = findDuplicateColumns(obj["columns"]);
    if (!duplicates.empty()) {
        for (const auto &d : duplicates) {
            Problem p; p.error = std::string("duplicate column name: ") + d;
            problems.push_back(p);
        }
        result.problem = problems;
        return result;
    }

    json new_entry = json::object();
    new_entry["id"] = generateID();

    new_entry["columns"] = json::object();
    for (auto &c : obj["columns"].items()) {
        const std::string col_name = c.key();
        const json &col_type_json = c.value();
        if (!col_type_json.is_string()) {
            Problem p; p.error = "column " + col_name + " has invalid type, it should be VARCHAR or INT64";
            problems.push_back(p);
            continue;
        }
        std::string col_type = col_type_json.get<std::string>();
        if (col_type != "INT64" && col_type != "VARCHAR") {
            Problem p; p.error = "column " + col_name + " has invalid type, it should be VARCHAR or INT64";
            problems.push_back(p);
            continue;
        }
        new_entry["columns"][col_name] = col_type;
    }
    if (!problems.empty()) {
        result.problem = problems;
        return result;
    }
    new_entry["location"] = "";
    new_entry["files"] = json::array();

    meta["tables"][table_name] = new_entry;
    saveFile(basePath, meta);

    result.tableId = new_entry["id"];
    return result;
}

void addLocationAndFiles(uint64_t id, const std::string &location, const std::vector<std::string> &files) {

    json data = readLocalFile(basePath);
    if (!data.is_object()) {
        data = json::object();
        data["tables"] = json::object();
    } else if (!data.contains("tables") || !data["tables"].is_object()) {
        data["tables"] = json::object();
    }
    std::map<uint64_t, std::string> tables = getTables();
    auto it = tables.find(id);
    if (it == tables.end()) return;
    const std::string name = it->second;

    data["tables"][name]["location"] = location;
    if (!data["tables"][name].contains("files") || !data["tables"][name]["files"].is_array()) {
        data["tables"][name]["files"] = json::array();
    }
    for (const auto &f : files) data["tables"][name]["files"].push_back(f);
    saveFile(basePath, data);
}




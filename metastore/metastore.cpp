
#include <string>
#include <vector>
#include <optional>
#include <cstdint>
#include <nlohmann/json.hpp>
#include <fstream>
#include <iostream>
#include <filesystem>
#include <random>

#include "../types.h"
#include "metastore.h"

namespace fs = std::filesystem;
using json = nlohmann::json;
using namespace std;

static const fs::path basePath =  fs::current_path() / "metastore/metastore.json";

json readFile(){
    json result;
    ifstream metastore(basePath);
    if (!metastore.is_open()) {
        return json::array();
    }
    metastore >> result;
    return result;
}

std::optional<TableInfo> getTableInfoByName(const std::string& name) {
    json meta = readFile();
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
    map<uint64_t, string>  names;
    json meta = readFile();
    for (auto &kv : meta["tables"].items()) {
        const std::string table_name = kv.key();
        const json &obj = kv.value();
        uint64_t id = stoull(obj["id"].get<string>());
        names.insert({id, table_name});
    }
    return names;
}

void removeFiles(const std::string &Path, const std::vector<std::string> &file_names){ 
    fs::path dir(Path);
    for (const auto &fname : file_names) {
        fs::path p = dir / fname;
        if (fs::exists(p)) {
            fs::remove(p);
        }
    }
}

bool deleteTable(uint64_t id) {
    map<uint64_t, string> tables = getTables();
    auto it = tables.find(id);
    if (it == tables.end()) return false;
    const std::string name = it->second;

    json meta = readFile();
    if (!meta["tables"].contains(name)) return false;

    const json &obj = meta["tables"][name];

    std::string location = obj["location"].get<std::string>();

    std::vector<std::string> files;

    for (const auto &f : obj["files"]) {
        if (f.is_string()) files.push_back(f.get<std::string>());
    }
    removeFiles(location, files);
    meta["tables"].erase(name);
    std::ofstream out(basePath);
    if (!out.is_open()) return false;
    out << meta.dump(2);
    out.close();
    return true;
}

CreateTableResult createTable(const json& json_info) {

    auto it = json_info.begin();
    std::string table_name = it.key();
    const json &obj = it.value();

    json meta = readFile();

    if (meta["tables"].contains(table_name)) {
        return CreateTableResult{"", CREATE_TABLE_ERROR::TABLE_EXISTS};
    }

    std::random_device rd;
    std::mt19937_64 gen(rd());
    uint64_t id = gen();

    json new_entry = json::object();
    new_entry["id"] = std::to_string(id);

    new_entry["columns"] = json::object();
    for (auto &c : obj["columns"].items()) {
        const std::string col_name = c.key();
        const json &col_type_json = c.value();
        if (!col_type_json.is_string()) {
            return CreateTableResult{"", CREATE_TABLE_ERROR::INVALID_COLUMN_TYPE};
        }
        std::string col_type = col_type_json.get<std::string>();
        if (col_type != "INT64" && col_type != "VARCHAR") {
            return CreateTableResult{"", CREATE_TABLE_ERROR::INVALID_COLUMN_TYPE};
        }
        new_entry["columns"][col_name] = col_type;
    }
    new_entry["location"] = "";
    new_entry["files"] = json::array();

    meta["tables"][table_name] = new_entry;
    std::ofstream out(basePath);
 
    out << meta.dump(2);
    out.close();

    return CreateTableResult{std::to_string(id), CREATE_TABLE_ERROR::NONE};
}

void addLocationAndFiles(uint64_t id, const std::string &location, const std::vector<std::string> &files) {
    json data = readFile();
    std::map<uint64_t, std::string> tables = getTables();
    auto it = tables.find(id);
    if (it == tables.end()) return;
    const std::string name = it->second;

    data["tables"][name]["location"] = location;
    data["tables"][name]["files"] = json::array();
    for (const auto &f : files) data["tables"][name]["files"].push_back(f);

    std::ofstream out(basePath);
    if (!out.is_open()) return;
    out << data.dump(2);
    out.close();
}





#include <string>
#include <vector>
#include <optional>
#include <cstdint>
#include <nlohmann/json.hpp>
#include <fstream>
#include <iostream>
#include <filesystem>

#include "../types.h"
#include "metastore.h"

namespace fs = filesystem;
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

optional<TableInfo> getTableInfo(string name) {
    json meta = readFile();
    for (auto &kv : meta["tables"].items()) {
        if (kv.key() == name){
            const json &obj = kv.value();
            TableInfo tableinfo;
            tableinfo.name = name;
            tableinfo.id = stoull(obj["id"].get<string>());
            for (auto &col : obj["columns"].items()) {
                std::string col_name = col.key();
                std::string col_type = col.value().get<string>();
                tableinfo.info.emplace_back(col_name, col_type);
            }
            return tableinfo;
        }
    }
    return nullopt;
}

map<uint64_t, string> tablesFromMetastore(){
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

optional<TableInfo> getTableInfoMetastore(uint64_t id){
    map<uint64_t, string> map = tablesFromMetastore();
    return getTableInfo(map[id]);
}
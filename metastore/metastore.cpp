
#include <string>
#include <vector>
#include <optional>
#include <cstdint>
#include <unordered_set>
#include <nlohmann/json.hpp>
#include <fstream>
#include <iostream>
#include <filesystem>
#include <random>

#include "../types.h"
#include "metastore.h"

namespace fs = std::filesystem;
using json = nlohmann::ordered_json;
using namespace std;

static const fs::path basePath =  fs::current_path() / "metastore/metastore.json";

json readFile(){
    json result;
    ifstream metastore(basePath);
    if (!metastore.is_open()) {
        // if metastore doesn't exist yet, return an object with empty "tables" map
        json empty = json::object();
        empty["tables"] = json::object();
        return empty;
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
    std::cout << "[metastore] deleteTable called for id=" << id << std::endl;
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
    std::cout << "[metastore] files to remove: ";
    for (const auto &fn : files) std::cout << fn << ", ";
    std::cout << std::endl;
    removeFiles(location, files);
    meta["tables"].erase(name);
    std::ofstream out(basePath);
    if (!out.is_open()) return false;
    out << meta.dump(2);
    out.close();
    return true;
}

CreateTableResult createTable(const json& json_info) {

    CreateTableResult result;
    vector<string> errors;
    result.error = errors;
    std::cout << "[metastore] createTable called with: " << json_info.dump() << std::endl;
    auto it = json_info.begin();
    std::string table_name = it.key();
    const json &obj = it.value();

    // helper to print result
    auto dumpResult = [](const CreateTableResult &r) {
        std::cout << "[metastore] createTable result: tableId='" << r.tableId << "' errors=[";
        for (size_t i = 0; i < r.error.size(); ++i) {
            if (i) std::cout << ", ";
            std::cout << r.error[i];
        }
        std::cout << "]" << std::endl;
    };

    // quick helper: check duplicate column names
        auto findDuplicateColumns = [](const json &cols) {
            std::vector<std::string> dupes;
            if (!cols.is_object()) return dupes;
            std::unordered_set<std::string> seen;
            size_t iter = 0;
            for (const auto &c : cols.items()) {
                const std::string name = c.key();
                bool inserted = seen.insert(name).second;
                if (!inserted) dupes.push_back(name);

                // print status after this iteration
                std::cout << "[findDuplicateColumns] iter=" << iter << " processing='" << name << "'\n";

                std::cout << "  dupes (size=" << dupes.size() << "): [";
                for (size_t i = 0; i < dupes.size(); ++i) {
                    if (i) std::cout << ", ";
                    std::cout << dupes[i];
                }
                std::cout << "]\n";

                std::cout << "  seen (size=" << seen.size() << "): {";
                bool first = true;
                for (const auto &s : seen) {
                    if (!first) std::cout << ", ";
                    std::cout << s;
                    first = false;
                }
                std::cout << "}\n";

                ++iter;
            }
            return dupes;
        };

    json meta = readFile();

    if (meta["tables"].contains(table_name)) {
        string error = "Table with name " + table_name + " already exists !";
        errors.push_back(error);
        result.error = errors;
        dumpResult(result);
        return result;
        // return CreateTableResult{"", CREATE_TABLE_ERROR::TABLE_EXISTS};
    }

    // validate columns object exists
    if (!obj.contains("columns") || !obj["columns"].is_object()) {
        errors.push_back("Invalid or missing 'columns' definition for table");
        result.error = errors;
        dumpResult(result);
        return result;
    }

    // check duplicate column names
    auto dupes = findDuplicateColumns(obj["columns"]);
    if (!dupes.empty()) {
        for (const auto &d : dupes) errors.push_back(std::string("duplicate column name: ") + d);
        result.error = errors;
        dumpResult(result);
        return result;
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
            std::cerr << "[metastore] column '" << col_name << "' has non-string type: " << col_type_json.dump() << std::endl;
            errors.push_back("column " + col_name + " has invalid type, it should be VARCHAR or INT64");
            // return CreateTableResult{"", CREATE_TABLE_ERROR::INVALID_COLUMN_TYPE};
        }
        std::string col_type = col_type_json.get<std::string>();
        if (col_type != "INT64" && col_type != "VARCHAR") {
            std::cerr << "[metastore] column '" << col_name << "' has unsupported type: " << col_type << std::endl;
            errors.push_back("column " + col_name + " has invalid type, it should be VARCHAR or INT64");
            // return CreateTableResult{"", CREATE_TABLE_ERROR::INVALID_COLUMN_TYPE};
        }
        new_entry["columns"][col_name] = col_type;
    }
    if (!errors.empty()) {
        result.error = errors;
        dumpResult(result);
        return result;
    }
    new_entry["location"] = "";
    new_entry["files"] = json::array();

    meta["tables"][table_name] = new_entry;
    std::ofstream out(basePath);
 
    out << meta.dump(2);
    out.close();
    result.tableId = std:: to_string(id);
    std::cout << "[metastore] created table '" << table_name << "' id=" << result.tableId << std::endl;
    return result;

    // return CreateTableResult{std::to_string(id), CREATE_TABLE_ERROR::NONE};
}

void addLocationAndFiles(uint64_t id, const std::string &location, const std::vector<std::string> &files) {
    std::cout << "[metastore] addLocationAndFiles id=" << id << " location='" << location << "' files_count=" << files.size() << std::endl;
    json data = readFile();
    std::map<uint64_t, std::string> tables = getTables();
    auto it = tables.find(id);
    if (it == tables.end()) return;
    const std::string name = it->second;

    data["tables"][name]["location"] = location;
    if (!data["tables"][name].contains("files") || !data["tables"][name]["files"].is_array()) {
        data["tables"][name]["files"] = json::array();
    }
    for (const auto &f : files) data["tables"][name]["files"].push_back(f);

    std::ofstream out(basePath);
    if (!out.is_open()) return;
    out << data.dump(2);
    out.close();
    std::cout << "[metastore] addLocationAndFiles: updated metastore for table '" << name << "'" << std::endl;
}




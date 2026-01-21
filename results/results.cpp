#include "results.h"
#include "../utils/utils.h"
#include <nlohmann/json.hpp>
#include <fstream>
#include <iostream>
#include <algorithm>

using json = nlohmann::ordered_json;
static const filesystem::path basePath =  filesystem::current_path() / "results/results.json";

std::optional<QueryResult> getQueryResult(const std::string &id, int rowLimit){
    json data = readLocalFile(basePath);

    for (const auto &entry : data) {
        if (!entry.is_object()) continue;
        std::string entryId = entry.value("id", std::string());
        if (entryId != id) continue;

        QueryResult result;
        int totalRows = entry.value("rowCount", 0);
        int to_take = totalRows;
        if (rowLimit > 0 && rowLimit < totalRows) to_take = rowLimit;
        result.rowCount = to_take;
        result.columns.clear();

        if (entry.contains("columns") && entry["columns"].is_array()) {
            for (const auto &col : entry["columns"]) {
                if (!col.is_array()) continue;

                        bool hasString = false;
                        bool hasBool = false;
                        for (size_t i = 0; i < col.size() && (int)i < to_take; ++i) {
                            const auto &v = col[i];
                            if (v.is_string()) { hasString = true; break; }
                            if (v.is_boolean()) { hasBool = true; break; }
                        }

                        if (hasString) {
                            std::vector<std::string> vec;
                            for (int i = 0; i < to_take && (size_t)i < col.size(); ++i) {
                                const auto &v = col[i];
                                if (v.is_string()) vec.push_back(v.get<std::string>());
                                else vec.push_back(v.dump());
                            }
                            result.columns.push_back(std::move(vec));
                        } else if (hasBool) {
                            std::vector<bool> vec;
                            for (int i = 0; i < to_take && (size_t)i < col.size(); ++i) {
                                const auto &v = col[i];
                                vec.push_back(v.get<bool>());
                            }
                            result.columns.push_back(std::move(vec));
                        } else {
                            std::vector<int64_t> vec;
                            int take = std::min((int)col.size(), to_take);
                            for (int i = 0; i < take; ++i) {
                                const auto &v = col[i];
                                vec.push_back(v.get<int64_t>());
                            }
                            result.columns.push_back(std::move(vec));
                        }
            }
        }

        return result;
    }

    return std::nullopt;
}

void initResult(std::string id){
    json results = readLocalFile(basePath);;

    json new_entry = json::object();
    new_entry["id"] = id;
    new_entry["rowCount"] = 0;
    new_entry["columns"] = json::array();

    results.push_back(new_entry);
    saveFile(basePath, results);
}

void modifyResult(std::string id, std::vector<MixBatch>& batches){
    json results = readLocalFile(basePath);

    auto it = std::find_if(results.begin(), results.end(),
        [&id](const json& entry) -> bool {
            return entry.value("id", "") == id;
        });
    if (it == results.end()) {
        std::cerr << "modifyQuery: id not found: " << id << "\n";
        return;
    }
    json& entry = *it;

    size_t totalColumns = 0;
    if (!batches.empty()) {
        totalColumns = batches[0].columns.size();
    }

    if (!entry.contains("columns") || entry["columns"].is_null()) {
        entry["columns"] = json::array();
    } else if (entry["columns"].is_object()) {
        json old = entry["columns"];
        entry["columns"] = json::array();
        for (auto &it : old.items()) {
            entry["columns"].push_back(it.value());
        }
    } else if (!entry["columns"].is_array()) {
        entry["columns"] = json::array();
    }

    if (entry["columns"].size() < totalColumns) {
        for (size_t i = entry["columns"].size(); i < totalColumns; ++i) {
            entry["columns"].push_back(json::array());
        }
    }

    for (const auto& batch : batches) {
       size_t colIdx = 0;
       for (const auto& col : batch.columns) {
           for (const auto& val : col.data) {
               if (val.type == ValueType::INT64) entry["columns"][colIdx].push_back(val.intValue);
               else if (val.type == ValueType::VARCHAR) entry["columns"][colIdx].push_back(val.stringValue);
               else if (val.type == ValueType::BOOL) entry["columns"][colIdx].push_back(val.boolValue);
               else entry["columns"][colIdx].push_back(nullptr);
           }
           colIdx++;
       }

       if (!entry.contains("rowCount") || !entry["rowCount"].is_number_integer()) {
           entry["rowCount"] = 0;
       }
             entry["rowCount"] = entry["rowCount"].get<int>() + batch.num_rows;
    }

    log_info(std::string("modifyResult: wrote result id=") + id + std::string(" rows=") + std::to_string(entry["rowCount"].get<int>()));

    saveFile(basePath, results);
}

void removeResult(const std::string &id) {
    json results = readLocalFile(basePath);
    for (auto it = results.begin(); it != results.end(); ++it) {
        if (!it->is_object()) continue;
        if ((*it).value("id", std::string()) == id) {
            results.erase(it);
            saveFile(basePath, results);
            return;
        }
    }
}
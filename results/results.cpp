#include "results.h"
#include <nlohmann/json.hpp>
#include <fstream>
#include <iostream>

using json = nlohmann::ordered_json;;
namespace fs = std::filesystem;
static const fs::path basePath =  fs::current_path() / "results/results.json";

json readFileResult(){
    json result;
    ifstream err(basePath);
    if (!err.is_open()) {
        return json::array();
    }
    err >> result;
    return result;
}

std::optional<QueryResult> getQueryResult(std::string id){
    json data = readFileResult();

    for (const auto &entry : data) {
        if (!entry.is_object()) continue;
        std::string entryId = entry.value("id", std::string());
        if (entryId != id) continue;

        QueryResult result;
        result.rowCount = entry.value("rowCount", 0);
        result.columns.clear();

        if (entry.contains("columns") && entry["columns"].is_array()) {
            for (const auto &col : entry["columns"]) {
                if (!col.is_array()) continue;

                bool hasString = false;
                for (const auto &v : col) {
                    if (v.is_string()) { hasString = true; break; }
                }

                if (hasString) {
                    std::vector<std::string> vec;
                    for (const auto &v : col) {
                        if (v.is_string()) vec.push_back(v.get<std::string>());
                        else vec.push_back(v.dump());
                    }
                    result.columns.push_back(std::move(vec));
                } else {
                    std::vector<int64_t> vec;
                    for (const auto &v : col) {
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
    json results = readFileResult();

    json new_entry = json::object();
    new_entry["id"] = id;
    new_entry["rowCount"] = 0;
    new_entry["columns"] = json::array();

    results.push_back(new_entry);
    std::ofstream out(basePath);
    out << std::setw(2) << results << std::endl;
    out.close();
}

void modifyResult(std::string id, vector<Batch>& batches){
    json results = readFileResult();

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
        totalColumns = batches[0].intColumns.size() + batches[0].stringColumns.size();
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
       for (const auto& col : batch.intColumns) {
           for (auto val : col.column) {
               entry["columns"][colIdx].push_back(val);
           }
           colIdx++;
       }
       for (const auto& col : batch.stringColumns) {
           for (const auto& val : col.column) {
               entry["columns"][colIdx].push_back(val);
           }
           colIdx++;
       }

       if (!entry.contains("rowCount") || !entry["rowCount"].is_number_integer()) {
           entry["rowCount"] = 0;
       }
       entry["rowCount"] = entry["rowCount"].get<int>() + batch.num_rows;
    }


    std::ofstream outFile(basePath);
    outFile << std::setw(2) << results << std::endl;
    outFile.close();
}
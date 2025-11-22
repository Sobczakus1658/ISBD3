#include "queries.h"
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iomanip>


using json = nlohmann::json;
using namespace std;

namespace fs = std::filesystem;
static const fs::path basePath =  fs::current_path() / "queries/results.json";

json readFileResult(){
    json result;
    ifstream metastore(basePath);
    if (!metastore.is_open()) {
        return json::array();
    }
    metastore >> result;
    return result;
}

void initQuery(std::string id){
    json results = readFileResult();

    json new_entry = json::object();
    new_entry["id"] = id;
    new_entry["rowCount"] = 0;

    new_entry["columns"] = json::array();

    if (!results.is_array()) {
        if (results.is_object()) {
            json old = results;
            results = json::array();
            for (auto &it : old.items()) {
                json v = it.value();
                if (v.is_object() && !v.contains("id")) {
                    v["id"] = it.key();
                }
                results.push_back(std::move(v));
            }
        } else {
            results = json::array();
        }
    }
    results.push_back(new_entry);
    std::ofstream out(basePath);
    out << std::setw(2) << results << std::endl;
    out.close();
}

void modifyQuery(std::string id, vector<Batch>& batches){
    cout<<batches.size();
    cout.flush();
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

nlohmann::json getQueryResult(const std::string &id) {
    json results = readFileResult();
    for (const auto &entry : results) {
        if (entry.contains("id") && entry["id"].get<std::string>() == id) {
            json out = json::object();
            out["rowCount"] = entry.value("rowCount", 0);
            if (entry.contains("columns") && entry["columns"].is_array()) {
                out["columns"] = entry["columns"];
            } else {
                out["columns"] = json::array();
            }
            return out;
        }
    }
    return json::object();
}
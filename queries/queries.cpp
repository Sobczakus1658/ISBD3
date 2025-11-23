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
    // trzeba dać tutaj status created
    json results = readFileResult();

    json new_entry = json::object();
    new_entry["id"] = id;
    new_entry["rowCount"] = 0;
    new_entry["status"] = QueryStatus::CREATED;
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
void modifyStatus(std::string id, QueryStatus status) {
    json results = readFileResult();

    auto it = std::find_if(results.begin(), results.end(),
        [&id](const json& entry) -> bool {
            return entry.value("id", "") == id;
        });
    if (it == results.end()) {
        std::cerr << "modifyStatus: id not found: " << id << "\n";
        return;
    }
    (*it)["status"] = static_cast<int>(status);

    std::ofstream outFile(basePath);
    outFile << std::setw(2) << results << std::endl;
    outFile.close();
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

std::optional<QueryResponse> getQueryResponse(const std::string &id) {
    json results = readFileResult();

    for (const auto &entry : results) {
        std::string entryId = entry.value("id", std::string());
        if (entryId != id) continue;

        QueryResponse resp;
        resp.queryId = id;

        if (entry.contains("status")) {
            const json &s = entry["status"];
            resp.status = static_cast<QueryStatus>(s.get<int>());
        } else {
            resp.status = QueryStatus::FAILED;
        }
        resp.isResultAvailable = (resp.status == QueryStatus::FAILED || resp.status == QueryStatus::COMPLETED);

        if (entry.contains("queryDefinition") && entry["queryDefinition"].is_object()) {
            const json &def = entry["queryDefinition"];
            if (def.contains("CopyQuery") && def["CopyQuery"].is_object()) {
                const json &copy_query = def["CopyQuery"];
                resp.copyQuery.path = copy_query.value("sourceFilepath", std::string());
                resp.copyQuery.destinationTableName = copy_query.value("destinationTableName", std::string());
                resp.copyQuery.doesCsvContainHeader = copy_query.value("doesCsvContainHeader", false);
                if (copy_query.contains("destinationColumns") && copy_query["destinationColumns"].is_array()) {
                    for (const auto &c : copy_query["destinationColumns"]) {
                        if (c.is_string()) resp.copyQuery.destinationColumns.push_back(c.get<std::string>());
                    }
                }
            } else if (def.contains("SelectQuery") && def["SelectQuery"].is_object()) {
                const json &select_query = def["SelectQuery"];
                resp.selectQuery.tableName = select_query.value("tableName", std::string());
            }
        }

        return resp;
    }

    return std::nullopt;
}

nlohmann::json getQueries(){
    json results = readFileResult();
    json out = json::array();
    //TODO
    return out;
}
void changeStatus(std::string id, QueryStatus status) {

}
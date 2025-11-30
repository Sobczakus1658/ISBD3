#include "queries.h"
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iomanip>
#include "../utils/utils.h"

using namespace std;

static const filesystem::path basePath =  filesystem::current_path() / "queries/queries.json";

void initQuery(std::string id){
    json results = readLocalFile(basePath);

    json new_entry = json::object();
    new_entry["queryId"] = id;
    new_entry["queryDefinition"] = json::object();
    new_entry["status"] = QueryStatus::CREATED;
    new_entry["isResultAvailable"] = false;
    results.push_back(new_entry);
    saveFile(basePath, results);
}

void modifyStatus(std::string id, QueryStatus status) {
    json results = readLocalFile(basePath);

    auto it = std::find_if(results.begin(), results.end(),
        [&id](const json& entry) -> bool {
            return entry.value("id", "") == id;
        });
    if (it == results.end()) {
        std::cerr << "modifyStatus: id not found: " << id << "\n";
        return;
    }
    (*it)["status"] = static_cast<int>(status);

    saveFile(basePath, results);
}

std::optional<QueryResponse> getQueryResponse(const std::string &id) {
    json results = readLocalFile(basePath);

    for (const auto &entry : results) {
        std::string entryId = entry.value("queryId", std::string());
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
                resp.query = jsonToCopyQuery(def["CopyQuery"]);
            } else if (def.contains("SelectQuery") && def["SelectQuery"].is_object()) {
                resp.query = jsonToSelectQuery(def["SelectQuery"]);
            }
        }

        return resp;
    }

    return std::nullopt;
}

json getQueries(){
    json results = readLocalFile(basePath);
    json out = json::array();

    for (const auto &entry : results) {
        if (!entry.is_object()) continue;

        std::string id = "";
        if (entry.contains("queryId") && entry["queryId"].is_string()) id = entry["queryId"].get<std::string>();

        if (id.empty()) continue;

        int status_num = -1;
        if (entry.contains("status") && entry["status"].is_number_integer()) {
            status_num = entry["status"].get<int>();
        }

        json obj = json::object();
        obj["queryId"] = id;
        obj["status"] = statusToStringFromInt(status_num) ;
        out.push_back(std::move(obj));
    }

    return out;
}

void changeStatus(std::string id, QueryStatus status) {
    json results = readLocalFile(basePath);

    for (auto &entry : results) {
        if (!entry.is_object()) continue;
        std::string entryQid = entry.value("queryId", std::string());
        if (entryQid == id) {
            entry["status"] = static_cast<int>(status);
            bool isResult = (status == QueryStatus::FAILED || status == QueryStatus::COMPLETED);
            entry["isResultAvailable"] = isResult;
            saveFile(basePath, results);
        }
    }
}

void addQueryDefinition(std::string id, QueryToJson query) {
    json results = readLocalFile(basePath);
    for (auto &entry : results) {
        if (!entry.is_object()) continue;

        std::string entryQid = entry.value("queryId", std::string());
        if (entryQid == id) {
            entry["queryDefinition"] = buildQueryDefinition(query);
            saveFile(basePath, results);
        }
    }
}
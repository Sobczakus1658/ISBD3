#include "queries.h"
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iomanip>


using json = nlohmann::json;
using namespace std;

namespace fs = std::filesystem;
static const fs::path basePath =  fs::current_path() / "queries/results.json";

json readFileQueries(){
    json result;
    ifstream queries(basePath);
    if (!queries.is_open()) {
        return json::array();
    }
    queries >> result;
    return result;
}

static json copyQueryToJson(const CopyQuery &q) {
    json j = json::object();
    j["sourceFilepath"] = q.path;
    j["destinationTableName"] = q.destinationTableName;
    j["doesCsvContainHeader"] = q.doesCsvContainHeader;
    j["destinationColumns"] = json::array();
    for (const auto &c : q.destinationColumns) j["destinationColumns"].push_back(c);
    return j;
}

static json selectQueryToJson(const SelectQuery &q) {
    json j = json::object();
    j["tableName"] = q.tableName;
    return j;
}

static json buildQueryDefinition(const QueryToJson &q) {
    if (auto pc = std::get_if<CopyQuery>(&q)) {
        json def = json::object();
        def["CopyQuery"] = copyQueryToJson(*pc);
        return def;
    }
    if (auto ps = std::get_if<SelectQuery>(&q)) {
        json def = json::object();
        def["SelectQuery"] = selectQueryToJson(*ps);
        return def;
    }
    return json::object();
}

static CopyQuery jsonToCopyQuery(const json &copy_query) {
    CopyQuery cq;
    cq.path = copy_query.value("sourceFilepath", std::string());
    cq.destinationTableName = copy_query.value("destinationTableName", std::string());
    cq.doesCsvContainHeader = copy_query.value("doesCsvContainHeader", false);
    if (copy_query.contains("destinationColumns") && copy_query["destinationColumns"].is_array()) {
        for (const auto &c : copy_query["destinationColumns"]) {
            if (c.is_string()) cq.destinationColumns.push_back(c.get<std::string>());
        }
    }
    return cq;
}

static SelectQuery jsonToSelectQuery(const json &select_query) {
    SelectQuery sq;
    sq.tableName = select_query.value("tableName", std::string());
    return sq;
}

void initQuery(std::string id){
    json results = readFileQueries();

    json new_entry = json::object();
    new_entry["queryId"] = id;
    new_entry["queryDefinition"] = json::object();
    new_entry["status"] = QueryStatus::CREATED;
    new_entry["isResultAvailable"] = false;
    results.push_back(new_entry);
    std::ofstream out(basePath);
    out << std::setw(2) << results << std::endl;
    out.close();
}

void modifyStatus(std::string id, QueryStatus status) {
    json results = readFileQueries();

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

std::optional<QueryResponse> getQueryResponse(const std::string &id) {
    json results = readFileQueries();

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
                resp.query = jsonToCopyQuery(def["CopyQuery"]);
            } else if (def.contains("SelectQuery") && def["SelectQuery"].is_object()) {
                resp.query = jsonToSelectQuery(def["SelectQuery"]);
            }
        }

        return resp;
    }

    return std::nullopt;
}

nlohmann::json getQueries(){
    json results = readFileQueries();
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
        obj["status"] = status_num;
        out.push_back(std::move(obj));
    }

    return out;
}

void changeStatus(std::string id, QueryStatus status) {
    json results = readFileQueries();

    for (auto &entry : results) {
        if (!entry.is_object()) continue;
        std::string entryQid = entry.value("queryId", std::string());
        if (entryQid == id) {
            entry["status"] = static_cast<int>(status);
            bool isResult = (status == QueryStatus::FAILED || status == QueryStatus::COMPLETED);
            entry["isResultAvailable"] = isResult;
            std::ofstream outFile(basePath);
            outFile << std::setw(2) << results << std::endl;
            outFile.close();
        }
    }
}

void addQueryDefinition(std::string id, QueryToJson query) {
    json results = readFileQueries();
    for (auto &entry : results) {
        if (!entry.is_object()) continue;

        std::string entryQid = entry.value("queryId", std::string());
        if (entryQid == id) {
            entry["queryDefinition"] = buildQueryDefinition(query);
            std::ofstream outFile(basePath);
            outFile << std::setw(2) << results << std::endl;
            outFile.close();
        }
    }
}
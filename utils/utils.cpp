#include "../queries/queries.h"
#include "../results/results.h"
#include "../errors/errors.h"
#include <fstream>
#include <random>
#include <unordered_set>

#include "corvusoft/restbed/settings.hpp"
#include "corvusoft/restbed/resource.hpp"
#include "corvusoft/restbed/service.hpp"
#include "corvusoft/restbed/request.hpp"
#include "corvusoft/restbed/response.hpp"
#include "corvusoft/restbed/session.hpp"
#include "corvusoft/restbed/logger.hpp"

#include "../service/executionService.h"
#include "utils.h"

using json = nlohmann::ordered_json;

using namespace std;
using namespace restbed;
namespace fs = std::filesystem;

void log_info(const std::string &msg) {
    std::cerr << "[info] " << msg << std::endl;
}

void log_error(const std::string &msg) {
    std::cerr << "[error] " << msg << std::endl;
}

json getSystemInfo() {
    json response_json;
    response_json["interfaceVersion"] = 1.0;
    response_json["version"] = 1.0;
    response_json["author"] = "Michał Sobczak";
    return response_json;
}

string prepareTablesInfo(const map<uint64_t, string>& tables) {
    json response_json = json::object();
    for (const auto& [id, name] : tables) {
        response_json[name] = std::to_string(id);
    }
    return response_json.dump();
}

string prepareTableInfo(TableInfo& tableInfo){
    json response_json;

    response_json["id"] = tableInfo.id;
    response_json["name"] = tableInfo.name;
    response_json["info"] = json::array();
    for (const auto &col : tableInfo.info) {
        json colobj;
        colobj["name"] = col.first;
        colobj["type"] = col.second;
        response_json["info"].push_back(colobj);
    }
    return response_json.dump();
}

string statusToStringFromInt(int s) {
    switch (s) {
        case 0: return "CREATED";
        case 1: return "PLANNING";
        case 2: return "RUNNING";
        case 3: return "COMPLETED";
        case 4: return "FAILED";
        default: return "UNKNOWN";
    }
}

string statusToString(QueryStatus s) {
    return statusToStringFromInt(static_cast<int>(s));
}

json prepareQueryResponse(const QueryResponse &response) {
    json json_info = json::object();

    json_info["queryId"] = response.queryId;
    json_info["status"] = statusToString(response.status);
    json_info["isResultAvailable"] = response.isResultAvailable;

    if (auto pc = std::get_if<CopyQuery>(&response.query)) {
        json copy_query = json::object();
        copy_query["sourceFilepath"] = pc->path;
        copy_query["destinationTableName"] = pc->destinationTableName;
        copy_query["doesCsvContainHeader"] = pc->doesCsvContainHeader;
        copy_query["destinationColumns"] = json::array();
        for (const auto &c : pc->destinationColumns) copy_query["destinationColumns"].push_back(c);
        json_info["CopyQuery"] = std::move(copy_query);
    } else if (auto ps = std::get_if<SelectQuery>(&response.query)) {
        json select_query = json::object();
        select_query["tableName"] = ps->tableName;
        json_info["SelectQuery"] = std::move(select_query);
    }

    return json_info;
}

json prepareQueryResultResponse(const QueryResult& response) {
    json j;

    j["rowCount"] = response.rowCount;
    j["columns"] = json::array();

    for (const auto& col : response.columns) {
        json colJson = std::visit([](const auto& vec) {
            json columnJson = json::array();
            for (const auto& v : vec) {
                columnJson.push_back(v);
            }
            return columnJson;
        }, col);

        j["columns"].push_back(colJson);
    }

    return j;
}

json prepareQueryErrorResponse( const QueryError& error) {
    json j = json::object();
    j["problems"] = json::array();
    for (const auto &p : error.problems) {
        json prob = json::object();
        prob["error"] = p.error;
        if (p.context.has_value()) prob["context"] = p.context.value();
        j["problems"].push_back(std::move(prob));
    }
    return j;
}

json createErrorResponse(const string message) {
    json j = json::object();
    j["problems"] = json::array();
    j["problems"].push_back({ {"error", message} });
    return j;
}

json errorResponse(const vector<Problem>& messages) {
    json j = json::object();
    j["problems"] = json::array();
    for (const auto& message : messages) {
        json prob = json::object();
        prob["error"] = message.error;
        if (message.context.has_value()) prob["context"] = message.context.value();
        j["problems"].push_back(std::move(prob));
    }
    return j;
}

string handleCsvError(const std::string &query_id, CSV_TABLE_ERROR code) {
    changeStatus(query_id, QueryStatus::FAILED);
    std::string msg;
    switch (code) {
        case CSV_TABLE_ERROR::FILE_NOT_FOUND:
            msg = "The file at this path does not exist";
            break;
        case CSV_TABLE_ERROR::INVALID_COLUMN_NUMBER:
            msg = "Actual and expected number of columns are different";
            break;
        case CSV_TABLE_ERROR::INVALID_TYPE:
            msg = "Invalid Column Type";
            break;
        case CSV_TABLE_ERROR::TABLE_NOT_FOUND:
            msg = "Table not found";
            break;
        case CSV_TABLE_ERROR::INVALID_DESTINATION_COLUMN:
            msg = "Invalid destination column";
            break;
        default:
            msg = "Unexpected error";
            break;
    }
    json error = createErrorResponse(msg);
    addError(query_id, error);
    return error.dump();
}

QueryType recogniseQuery(const json &query) {
    const json *def = &query;
    if (query.is_object() && query.contains("queryDefinition"))
        def = &query["queryDefinition"];

    if (!def->is_object())
        return QueryType::ERROR;

    if (def->contains("CopyQuery") && (*def)["CopyQuery"].is_object()) {
        const auto &cq = (*def)["CopyQuery"];
        if (cq.contains("sourceFilepath") && cq.contains("destinationTableName") &&
            cq["sourceFilepath"].is_string() && cq["destinationTableName"].is_string()) {
            return QueryType::COPY;
        }
        return QueryType::ERROR;
    }
    
    if (def->contains("SelectQuery") && (*def)["SelectQuery"].is_object()) {
        const auto &cq = (*def)["SelectQuery"];
        if (cq.contains("tableName") && cq["tableName"].is_string()) {
            return QueryType::SELECT;
        }
        return QueryType::ERROR;
    }

    return QueryType::ERROR;
}

string generateID(){
    std::random_device rd;
    std::mt19937_64 gen(rd());
    uint64_t id = gen();
    string query_id = std::to_string(id);
    return query_id;
}

CopyQuery createCopyQuery(const json& cq) {
    CopyQuery copyQuery;

    copyQuery.destinationTableName = cq["destinationTableName"].get<std::string>();
    copyQuery.path = cq["sourceFilepath"].get<std::string>();
    copyQuery.doesCsvContainHeader = cq["doesCsvContainHeader"].get<bool>();

    if (cq.contains("destinationColumns") && cq["destinationColumns"].is_array()) {
        std::vector<std::string> columns;
        for (const auto &c : cq["destinationColumns"]) {
            if (c.is_string()) columns.push_back(c.get<std::string>());
        }
        copyQuery.destinationColumns = columns;
    }
    return copyQuery;
}

json copyQueryToJson(const CopyQuery &q) {
    json j = json::object();
    j["sourceFilepath"] = q.path;
    j["destinationTableName"] = q.destinationTableName;
    j["doesCsvContainHeader"] = q.doesCsvContainHeader;
    j["destinationColumns"] = json::array();
    for (const auto &c : q.destinationColumns) j["destinationColumns"].push_back(c);
    return j;
}

json selectQueryToJson(const SelectQuery &q) {
    json j = json::object();
    j["tableName"] = q.tableName;
    return j;
}

json buildQueryDefinition(const QueryToJson &q) {
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

CopyQuery jsonToCopyQuery(const json &copy_query) {
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

SelectQuery jsonToSelectQuery(const json &select_query) {
    SelectQuery sq;
    sq.tableName = select_query.value("tableName", std::string());
    return sq;
}

json readLocalFile(const std::filesystem::path &basePath){
    json result;
    std::ifstream content(basePath);
    if (!content.is_open()) {
        return json::array();
    }
    content >> result;
    return result;
}

void saveFile(const std::filesystem::path &basePath, json results) {
    std::ofstream outFile(basePath);
    outFile << std::setw(2) << results << std::endl;
    outFile.close();
}

void removeFiles(const std::string &Path, const std::vector<std::string> &file_names){ 
    fs::path dir(Path);
    for (const auto &fname : file_names) {
        fs::path p = dir / fname;
        if (fs::exists(p)) {
            fs::remove(p);
        }
    }
    if (fs::exists(dir) && fs::is_directory(dir)) {
        try {
            if (fs::is_empty(dir)) {
                fs::remove(dir);
                log_info(std::string("removeFiles: removed empty directory: ") + dir.string());
            }
        } catch (const std::exception &e) {
            log_error(std::string("removeFiles: filesystem error: ") + e.what());
        }
    }
}

std::vector<std::string> findDuplicateColumns(const json &cols) {
    std::vector<std::string> dupes;
    if (!cols.is_object()) return dupes;
    std::unordered_set<std::string> seen;
    size_t iter = 0;
    for (const auto &c : cols.items()) {
        const std::string name = c.key();
        bool inserted = seen.insert(name).second;
        if (!inserted) dupes.push_back(name);
        ++iter;
    }
    return dupes;
}

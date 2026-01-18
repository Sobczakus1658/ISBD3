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
#include "../query/selectQuery.h"

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
    // API expects interfaceVersion and version as strings per OpenAPI client model
    response_json["interfaceVersion"] = std::string("1.0");
    response_json["version"] = std::string("1.0");
    response_json["author"] = "Michał Sobczak";
    return response_json;
}

string prepareTablesInfo(const map<uint64_t, string>& tables) {
    json response_json = json::array();
    for (const auto& [id, name] : tables) {
        json e = json::object();
        e["tableId"] = std::to_string(id);
        e["name"] = name;
        response_json.push_back(std::move(e));
    }
    return response_json.dump();
}

string prepareTableInfo(TableInfo& tableInfo){
    json response_json;
    response_json = json::object();
    response_json["name"] = tableInfo.name;
    response_json["columns"] = json::array();
    for (const auto &col : tableInfo.info) {
        json colobj;
        colobj["name"] = col.first;
        colobj["type"] = col.second;
        response_json["columns"].push_back(colobj);
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
    json def = buildQueryDefinition(response.query);
    json_info["isResultAvailable"] = response.isResultAvailable;

    // Try to return the original stored JSON queryDefinition (if present in queries/queries.json)
    // so that clients receive exactly the shape they submitted (matches OpenAPI schema).
    try {
        std::filesystem::path qpath = std::filesystem::current_path() / "queries" / "queries.json";
        std::ifstream in(qpath);
        if (in.is_open()) {
            json all; in >> all;
            for (const auto &entry : all) {
                if (!entry.is_object()) continue;
                std::string qid = entry.value("queryId", std::string());
                if (qid == response.queryId && entry.contains("queryDefinition")) {
                    json_info["queryDefinition"] = entry["queryDefinition"];
                    return json_info;
                }
            }
        }
    } catch (const std::exception &e) {
        // fall back to constructed definition
    }

    // fallback: construct a minimal definition from internal Query object
    json_info["queryDefinition"] = def;

    return json_info;
}

json prepareQueryResultResponse(const QueryResult& response) {
    json j;

    j["rowCount"] = response.rowCount;
    j["columns"] = json::array();

    struct ColumnToJson {
        json operator()(const std::vector<int64_t>& vec) const {
            json a = json::array();
            for (auto v : vec) a.push_back(v);
            return a;
        }
        json operator()(const std::vector<std::string>& vec) const {
            json a = json::array();
            for (const auto& v : vec) a.push_back(v);
            return a;
        }
        json operator()(const std::vector<bool>& vec) const {
            json a = json::array();
            for (bool v : vec) a.push_back(v);
            return a;
        }
    };

    for (const auto& col : response.columns) {
        json colJson = std::visit(ColumnToJson{}, col);
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
    if (!query.is_object() || !query.contains("queryDefinition")) return QueryType::ERROR;
    const json &def = query["queryDefinition"];
    if (!def.is_object()) return QueryType::ERROR;

    if (def.contains("sourceFilepath") && def.contains("destinationTableName")
        && def["sourceFilepath"].is_string() && def["destinationTableName"].is_string()) {
        return QueryType::COPY;
    }

    if (def.contains("columnClauses") &&
        def["columnClauses"].is_array() &&
        !def["columnClauses"].empty()) {
        return QueryType::SELECT;
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
        return copyQueryToJson(*pc);
    }
    if (auto ps = std::get_if<SelectQuery>(&q)) {
        return selectQueryToJson(*ps);
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
    try {
        auto parent = basePath.parent_path();
        if (!parent.empty() && !std::filesystem::exists(parent)) {
            std::filesystem::create_directories(parent);
        }
    } catch (const std::exception &e) {
        log_error(std::string("readLocalFile: could not create parent directory: ") + e.what());
    }

    std::ifstream content(basePath);
    if (!content.is_open()) {
        log_info(std::string("readLocalFile: file not found, returning empty array: ") + basePath.string());
        return json::array();
    }
    try {
        content >> result;
    } catch (const std::exception &e) {
        log_error(std::string("readLocalFile: failed to parse JSON from ") + basePath.string() + std::string(": ") + e.what());
        return json::array();
    }
    return result;
}

void saveFile(const std::filesystem::path &basePath, json results) {
    try {
        auto parent = basePath.parent_path();
        if (!parent.empty() && !std::filesystem::exists(parent)) {
            std::filesystem::create_directories(parent);
        }
    } catch (const std::exception &e) {
        log_error(std::string("saveFile: could not create parent directory: ") + e.what());
        return;
    }

    std::ofstream outFile(basePath);
    if (!outFile.is_open()) {
        log_error(std::string("saveFile: failed to open file for writing: ") + basePath.string());
        return;
    }
    outFile << std::setw(2) << results << std::endl;
    outFile.close();
}

bool validateCreateTableRequest(const json &parsed, json &out_create, std::vector<Problem> &problems) {
    out_create = json::object();

    if (parsed.is_object() && parsed.contains("name") && parsed.contains("columns") && parsed["name"].is_string() && parsed["columns"].is_array()) {
        std::string tname = parsed["name"].get<std::string>();
        json columns_obj = json::object();
        for (const auto &col : parsed["columns"]) {
            if (!col.is_object() || !col.contains("name") || !col.contains("type") || !col["name"].is_string() || !col["type"].is_string()) {
                Problem p; p.error = "Invalid column definition";
                problems.push_back(p);
                continue;
            }
            std::string cname = col["name"].get<std::string>();
            std::string ctype = col["type"].get<std::string>();
            columns_obj[cname] = ctype;
        }
        if (!problems.empty()) return false;
        json inner = json::object();
        inner["columns"] = columns_obj;
        out_create[tname] = inner;
        return true;
    }

    if (parsed.is_object()) {
        auto it = parsed.begin();
        if (it != parsed.end() && it.value().is_object() && it.value().contains("columns") && it.value()["columns"].is_object()) {
            out_create = parsed;
            return true;
        }
        Problem p; p.error = "Missing required fields: name and columns";
        problems.push_back(p);
        return false;
    }

    Problem p; p.error = "Invalid create table request";
    problems.push_back(p);
    return false;
}

bool parseResultRequestBody(const std::string &json_body, int &rowLimit, bool &flushResult, json &errorOrParsed) {
    if (json_body.empty()) return true;

    try {
        json parsed = json::parse(json_body);
        if (parsed.contains("rowLimit") && parsed["rowLimit"].is_number_integer()) {
            rowLimit = parsed["rowLimit"].get<int>();
        }
        if (parsed.contains("flushResult") && parsed["flushResult"].is_boolean()) {
            flushResult = parsed["flushResult"].get<bool>();
        }
        return true;
    } catch (const std::exception &e) {
        std::string err = std::string("JSON parse error: ") + e.what();
        json error = json::object();
        error["problems"] = json::array();
        error["problems"].push_back({ {"error", err}, {"context", json_body} });
        errorOrParsed = error;
        return false;
    }
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

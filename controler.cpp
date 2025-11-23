
#include <iostream>
#include "queries/queries.h"
#include "results/results.h"
#include "errors/errors.h"
#include "metastore/metastore.h"
#include <iostream>
#include <fstream>
#include <unordered_map>
#include <memory>
#include <string>
#include <vector>
#include <optional>
#include <nlohmann/json.hpp>

#include "corvusoft/restbed/settings.hpp"
#include "corvusoft/restbed/resource.hpp"
#include "corvusoft/restbed/service.hpp"
#include "corvusoft/restbed/request.hpp"
#include "corvusoft/restbed/response.hpp"
#include "corvusoft/restbed/session.hpp"
#include "corvusoft/restbed/logger.hpp"

#include "service/executionService.h"

#include <random> 
#include "controler.h"

using json = nlohmann::json;

using namespace std;
using namespace restbed;

json prepareTablesInfo(const map<uint64_t, string>& tables) {
    json response_json = json::array();

    for (const auto& [id, name] : tables) {
        response_json.push_back({
            {"id", id},
            {"name", name}
        });
    }
    return response_json;
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

string statusToString(QueryStatus s) {
    switch (s) {
            case QueryStatus::CREATED: return "CREATED";
            case QueryStatus::PLANNING: return "PLANNING";
            case QueryStatus::RUNNING: return "RUNNING";
            case QueryStatus::COMPLETED: return "COMPLETED";
            case QueryStatus::FAILED: return "FAILED";
            default: return "UNKNOWN";
        }
}

json prepareQueryResponse(const QueryResponse &response) {
    json json_info = json::object();

    json_info["queryId"] = response.queryId;
    json_info["status"] = statusToString(response.status);
    json_info["isResultAvailable"] = response.isResultAvailable;

    bool hasCopy = !response.copyQuery.destinationTableName.empty() || !response.copyQuery.path.empty();
    if (hasCopy) {
        json copy_query = json::object();
        copy_query["sourceFilepath"] = response.copyQuery.path;
        copy_query["destinationTableName"] = response.copyQuery.destinationTableName;
        copy_query["doesCsvContainHeader"] = response.copyQuery.doesCsvContainHeader;
        copy_query["destinationColumns"] = json::array();
        for (const auto &c : response.copyQuery.destinationColumns) copy_query["destinationColumns"].push_back(c);
        json_info["CopyQuery"] = std::move(copy_query);
    } else {
        json select_query = json::object();
        select_query["tableName"] = response.selectQuery.tableName;
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

void getTablesHandler(const shared_ptr<Session> session) {
    session->close(200,  prepareTablesInfo(getTables()).dump(), { {"Content-Type", "application/json"} });
}

void getTableByIdHandler(const shared_ptr<Session> session) {
    const auto request = session->get_request();
    std::string tableIdStr = request->get_path_parameter("tableId");
    uint64_t tableId = std::stoull(tableIdStr);
    optional<TableInfo> tableInfo = getTableInfo(tableId);

    if (!tableInfo.has_value()) {
        session->close(404, "{\"error\":\"Table not found\"}\n", {{"Content-Type", "application/json"}});
        return;
    }
    session->close(200, prepareTableInfo(tableInfo.value()),{{"Content-Type", "application/json"}});
}

void createTableHandler(const shared_ptr<Session> session) {
    int content_length = session->get_request()->get_header("Content-Length", 0);

    session->fetch(content_length,
        [](const std::shared_ptr<restbed::Session> session, const restbed::Bytes &body) {

            std::string json_body(body.begin(), body.end());
            json parsed = json::parse(json_body);

            CreateTableResult result = createTable(parsed);
            json response_json;

            switch (result.error){
                case CREATE_TABLE_ERROR::NONE:
                    response_json["tableId"] = result.tableId;
                    session->close(200, response_json.dump(), { {"Content-Type", "application/json"} });
                    break;

                case CREATE_TABLE_ERROR::INVALID_COLUMN_TYPE:
                    response_json["problems"] = {
                        { {"error", "Invalid column type. Allowed: INT64, VARCHAR"} }
                    };
                    session->close(400, response_json.dump(), { {"Content-Type", "application/json"} });
                    break;

                case CREATE_TABLE_ERROR::TABLE_EXISTS:
                    response_json["problems"] = {
                        { {"error", "A table with the specified name already exists."} }
                    };
                    session->close(400, response_json.dump(), { {"Content-Type", "application/json"} });
            }
        }
    );
}

void deleteTableHandler(const shared_ptr<Session> session) {
    const auto request = session->get_request();
    std::string tableIdStr = request->get_path_parameter("tableId");
    uint64_t tableId = std::stoull(tableIdStr);
    bool result = deleteTable(tableId);
    
    if (!result) {
        session->close(404, "{\"error during deleting Table\"}\n", {{"Content-Type", "application/json"}});
        return;
    }
    session->close(200, "",{{"Content-Type", "application/json"}});

}

void getQueriesHandler(const shared_ptr<Session> session) {
    session->close(200, getQueries().dump(),{{"Content-Type", "application/json"}});
}

void submitQueryHandler(const shared_ptr<Session> session)
{
    int content_length = session->get_request()->get_header("Content-Length", 0);

    session->fetch(content_length,
        [](const std::shared_ptr<restbed::Session> session, const restbed::Bytes &body) {
            std::string json_body(body.begin(), body.end());
            json json_message = json::parse(json_body);
            QueryType type = recogniseQuery(json_message);
            const auto &def = json_message["queryDefinition"];
            std::random_device rd;
            std::mt19937_64 gen(rd());
            uint64_t id = gen();
            string query_id = std::to_string(id);
            initQuery(query_id);
            json jsonResponse;
            jsonResponse["queryId"] = query_id;
            changeStatus(query_id, QueryStatus::PLANNING);
            switch(type) {
                case QueryType::COPY: {
                    const auto &cq = def["CopyQuery"];
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
                    
                    addCopyBody(query_id, copyQuery);
                    QueryCreatedResponse response = copyCSV(copyQuery, query_id);

                    switch (response.status){
                    case CSV_TABLE_ERROR::NONE :
                        changeStatus(query_id, QueryStatus::COMPLETED);
                        session->close(200, jsonResponse.dump(), { {"Content-Type", "application/json"} });
                        break;

                    case CSV_TABLE_ERROR::FILE_NOT_FOUND :
                        changeStatus(query_id, QueryStatus::FAILED);
                        session->close(400, createErrorResponse("The file at this path does not exist").dump(), { {"Content-Type", "application/json"} });
                        break;

                    case CSV_TABLE_ERROR::INVALID_COLUMN_NUMBER :
                        changeStatus(query_id, QueryStatus::FAILED);
                        session->close(400, createErrorResponse("Actual and expected number of columns are different").dump(), { {"Content-Type", "application/json"} });
                        break;

                    case CSV_TABLE_ERROR::INVALID_TYPE :
                        changeStatus(query_id, QueryStatus::FAILED);
                        session->close(400, createErrorResponse("Invalid Coumn Type").dump(), { {"Content-Type", "application/json"} });
                        break;

                    default:
                        changeStatus(query_id, QueryStatus::FAILED);
                        session->close(400, createErrorResponse("Unexpected error").dump(), { {"Content-Type", "application/json"} });
                        break;
                    }
                } 
                case QueryType:: SELECT: {
                    const auto &select_query = def["SelectQuery"];
                    SelectQuery sq;
                    sq.tableName = select_query["tableName"];
                    addSelectBody(query_id, sq);
                    SELECT_TABLE_ERROR response = selectTable(sq, query_id);
                    if (response == SELECT_TABLE_ERROR::NONE){
                        changeStatus(query_id, QueryStatus::COMPLETED);
                        session->close(200, jsonResponse.dump(), { {"Content-Type", "application/json"} });
                        break;
                    }
                    changeStatus(query_id, QueryStatus::FAILED);
                    string error = "Table " + select_query["tableName"].dump() + " does not exist" ;
                    session->close(400, createErrorResponse(error).dump(), { {"Content-Type", "application/json"} });
                }
                default: {
                    session->close(400, "Invalid QueryType", { {"Content-Type", "application/json"} });
                }

            }
        }
    );
}

void getQueryHandler(const shared_ptr<Session> session) {
    const auto request = session->get_request();
    std::string queryIdStr = request->get_path_parameter("queryId");
    optional<QueryResponse> result = getQueryResponse(queryIdStr);

    if (!result.has_value()) {
        session->close(404, "{\"error\":\"Query with this id not found\"}\n", {{"Content-Type", "application/json"}});
        return;
    }
    json response = json::array();
    response.push_back(prepareQueryResponse(result.value()));
    
    session->close(200, response.dump(), { {"Content-Type", "application/json"} });
}

void getQueryResultHandler(const shared_ptr<Session> session) {
    const auto request = session->get_request();
    std::string queryIdStr = request->get_path_parameter("queryId");
    optional<QueryResult> result = getQueryResult(queryIdStr);
    if (!result.has_value()) {
        session->close(404, "{\"error\":\"Query with this id not found\"}\n", {{"Content-Type", "application/json"}});
        return;
    }
    json response = json::array();
    response.push_back(prepareQueryResultResponse(result.value()));
    
    session->close(200, response.dump(), { {"Content-Type", "application/json"} });
}

void getQueryErrorHandler(const shared_ptr<Session> session) {
    const auto request = session->get_request();
    std::string queryIdStr = request->get_path_parameter("queryId");
    optional<QueryError> result = getQueryError(queryIdStr);
    if (!result.has_value()) {
        session->close(404, "{\"error\":\"Query with this id not found\"}\n", {{"Content-Type", "application/json"}});
        return;
    }
    json response = json::array();
    response.push_back(prepareQueryErrorResponse(result.value()));
    
    session->close(200, response.dump(), { {"Content-Type", "application/json"} });
}

void setUpApi(){

    auto tablesResource = make_shared<Resource>();
    tablesResource->set_path("/tables");
    tablesResource->set_method_handler("GET", getTablesHandler);

    auto tableResource = make_shared<Resource>();
    tableResource->set_path("/table/{tableId: .*}");
    tableResource->set_method_handler("GET", getTableByIdHandler);
    tableResource->set_method_handler("DELETE", deleteTableHandler);

    auto createTableResource = make_shared<Resource>();
    createTableResource->set_path("/table");
    createTableResource->set_method_handler("PUT", createTableHandler);

    auto queryResource = make_shared<Resource>();
    queryResource->set_path("/query");
    queryResource->set_method_handler("POST", submitQueryHandler);

    auto getQueriesResource = std::make_shared<Resource>();
    getQueriesResource->set_path("/queries");
    getQueriesResource->set_method_handler("GET", getQueriesHandler);

    auto getQueryResource = make_shared<Resource>();
    getQueryResource->set_path("/query/{queryId: .*}");
    getQueryResource->set_method_handler("GET", getQueryHandler);

    auto queryResultResource = make_shared<Resource>();
    queryResultResource->set_path("/result/{queryId: .*}");
    queryResultResource->set_method_handler("GET", getQueryResultHandler);

    auto queryErrorResource = make_shared<Resource>();
    queryErrorResource->set_path("/error/{queryId: .*}");
    queryErrorResource->set_method_handler("GET", getQueryErrorHandler);

    auto settings = make_shared<Settings>();
    settings->set_port(8080);
    settings->set_default_header("Connection", "close");

    Service service;
    service.publish(tablesResource);
    service.publish(tableResource);
    service.publish(createTableResource);
    service.publish(queryResource);
    service.publish(queryResultResource);
    service.publish(getQueriesResource);
    service.publish(getQueryResource);
    service.publish(queryErrorResource);

    service.start(settings);
}

// int main(){
//     setUpApi();
// }
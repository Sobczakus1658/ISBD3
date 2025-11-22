#include "utils.h"
#include <iostream>
#include "../queries/queries.h"

using json = nlohmann::json;

using namespace std;
using namespace restbed;

json queryResultToJson(const QueryResult& result) {
    json j;

    j["rowCount"] = result.rowCount;
    j["columns"] = json::array();

    for (const auto& col : result.columns) {
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

json queryResponseToJson(const QueryCreatedResponse &response) {
    json j;
    j["success"] = response.success;
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
    session->close(200, getTables().dump(), { {"Content-Type", "application/json"} });
}

void getTableByIdHandler(const shared_ptr<Session> session) {
    const auto request = session->get_request();
    std::string tableIdStr = request->get_path_parameter("tableId");
    uint64_t tableId = std::stoull(tableIdStr);
    optional<TableInfo> tableInfo = getTableInfoByID(tableId);

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

                    QueryCreatedResponse response = copyCSV(copyQuery);
                    json jsonResponse =  queryResponseToJson(response);
                    int status = response.success ? 200 : 400;
                    session->close(status, jsonResponse.dump(), { {"Content-Type", "application/json"} });
                    return;
                } 
                case QueryType:: SELECT: {
                    const auto &cq = def["SelectQuery"];
                    string queryId = selectTable(cq["tableName"]);

                    nlohmann::json jsonResponse;
                    jsonResponse["queryId"] = queryId;

                    session->close(200, jsonResponse.dump(), { {"Content-Type", "application/json"} });
                }
                default: {
                    session->close(400, "Invalid QueryType", { {"Content-Type", "application/json"} });
                }

            }
        }
    );
}

void getQueryResultHandler(const shared_ptr<Session> session)
{
    const auto request = session->get_request();
    std::string queryIdStr = request->get_path_parameter("queryId");
    json result = getQueryResult(queryIdStr);
    json response = json::array();
    response.push_back(result);
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

    auto queryResultResource = make_shared<Resource>();
    queryResultResource->set_path("/result/{queryId: .*}");
    queryResultResource->set_method_handler("GET", getQueryResultHandler);

    auto settings = make_shared<Settings>();
    settings->set_port(8080);
    settings->set_default_header("Connection", "close");

    Service service;
    service.publish(tablesResource);
    service.publish(tableResource);
    service.publish(createTableResource);
    service.publish(queryResource);
    service.publish(queryResultResource);

    service.start(settings);
}

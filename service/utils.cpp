#include "utils.h"

using json = nlohmann::json;

#include <iostream>
using namespace std;
using namespace restbed;

void f6() { }

string createJson(const map<uint64_t, string>& tables) {
    std::string json = "[";
    bool first = true;
    for (const auto &p : tables) {
        if (!first) json += ",";
        first = false;
        json += "{\"id\":" + std::to_string(p.first) + ",\"name\":\"" + p.second + "\"}";
    }
    json += "]";
    return json;
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

    if (def->contains("tableName") && (*def)["tableName"].is_string())
        return QueryType::SELECT;

    return QueryType::ERROR;
}

void getTablesHandler(const shared_ptr<Session> session) {
    map<uint64_t, string> tables = getTables();
    session->close(200, createJson(tables), { {"Content-Type", "application/json"} });
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
    json response_json;
    response_json["id"] = tableInfo->id;
    response_json["name"] = tableInfo->name;
    response_json["info"] = json::array();
    for (const auto &col : tableInfo->info) {
        json colobj;
        colobj["name"] = col.first;
        colobj["type"] = col.second;
        response_json["info"].push_back(colobj);
    }

    session->close(200, response_json.dump(),{{"Content-Type", "application/json"}});
}

void createTableHandler(const shared_ptr<Session> session) {
    int content_length = session->get_request()->get_header("Content-Length", 0);

    session->fetch(content_length,
        [](const std::shared_ptr<restbed::Session> session, const restbed::Bytes &body) {

            std::string json_body(body.begin(), body.end());
            json parsed;
            try {
                parsed = json::parse(json_body);
            } catch (const std::exception &e) {
                json err;
                err["error"] = std::string("Invalid JSON: ") + e.what();
                err["raw"] = json_body;
                session->close(400, err.dump(), {{"Content-Type","application/json"}});
                return;
            }


            CreateTableResult result = createTable(parsed);

            json response_json;
            if (result.error == CREATE_TABLE_ERROR::NONE) {
                response_json["tableId"] = result.tableId;
                session->close(200, response_json.dump(), { {"Content-Type", "application/json"} });
            } else {
                response_json["message"] = "Cannot create table";
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
    session->close(200, "true",{{"Content-Type", "application/json"}});

}

void submitQueryHandler(const shared_ptr<Session> session)
{
    int content_length = session->get_request()->get_header("Content-Length", 0);

    session->fetch(content_length,
        [](const std::shared_ptr<restbed::Session> session, const restbed::Bytes &body) {
            std::string json_body(body.begin(), body.end());
            json json_message = json::parse(json_body);
            QueryType type = recogniseQuery(json_message);
            switch(type) {
                case QueryType::COPY: {
                    if (!json_message.contains("queryDefinition") || !json_message["queryDefinition"].is_object()) {
                        throw std::runtime_error("Missing queryDefinition");
                    }

                    const auto &def = json_message["queryDefinition"];

                    if (!def.contains("CopyQuery") || !def["CopyQuery"].is_object()) {
                        throw std::runtime_error("Missing CopyQuery definition");
                    }

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
    f6();
    session->close(200, "[]", { {"Content-Type", "application/json"} });
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

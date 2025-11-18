// #include <iostream>
// #include <fstream>
// #include <unordered_map>
// #include <memory>
// #include <string>
// #include <vector>
// #include <optional>
// #include <nlohmann/json.hpp>

// #include "corvusoft/restbed/settings.hpp"
// #include "corvusoft/restbed/resource.hpp"
// #include "corvusoft/restbed/service.hpp"
// #include "corvusoft/restbed/request.hpp"
// #include "corvusoft/restbed/response.hpp"
// #include "corvusoft/restbed/session.hpp"
// #include "corvusoft/restbed/logger.hpp"

// #include "schemaService.h"
#include "utils.h"
// #include "exectutionService.h"

using json = nlohmann::json;

using namespace std;
using namespace restbed;

void f1() { }
void f2() { }
void f3() { }
void f4() { }
void f5() { }
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

void getTablesHandler(const shared_ptr<Session> session) {
    map<uint64_t, string> tables = getTables();
    session->close(200, createJson(tables), { {"Content-Type", "application/json"} });
}

void getTableByIdHandler(const shared_ptr<Session> session) {
    const auto request = session->get_request();
    std::string tableIdStr = request->get_path_parameter("tableId");
    uint64_t tableId = std::stoull(tableIdStr);
    std::optional<TableInfo> tableInfo = getTableInfo(tableId);

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
            json parsed = json::parse(json_body);

            CreateTableResult result = createTable(json_body);

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
    f4();
    session->close(200, "", {});
}



void submitQueryHandler(const shared_ptr<Session> session)
{
    f5();
    session->close(200, "\"new_query_id\"", { {"Content-Type", "application/json"} });
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

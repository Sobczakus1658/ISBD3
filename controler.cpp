
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
#include "utils/utils.h"

#include "controler.h"

using json = nlohmann::ordered_json;

using namespace std;
using namespace restbed;

bool parseJson(string json_body, json& destination){
    json parsed;
    try {
        parsed = json::parse(json_body);
        destination = parsed;
        return true;
    } catch (const std::exception &e) {
        std::string err = std::string("JSON parse error: ") + e.what();
        json error = json::object();
        error["problems"] = json::array();
        error["problems"].push_back({ {"error", err}, {"context", json_body} });
        destination = error;
        return false;
    }
}

bool parseId(std::string idStr, uint64_t& id ) {
    try {
        id = std::stoull(idStr);
    } catch (const std::exception &e) {
        return false;
    } 
    return true;
}

void closeConnection(const shared_ptr<Session> session, int status, string body){
    session->close(status,  body , { {"Content-Type", "application/json"} });
}

void getTablesHandler(const shared_ptr<Session> session) {
    log_info("handler getTablesHandler entered");

    closeConnection(session, 200, prepareTablesInfo(getTables()));
}

void getTableByIdHandler(const shared_ptr<Session> session) {
    log_info("handler getTableByIdHandler entered");

    const auto request = session->get_request();
    std::string tableIdStr = request->get_path_parameter("tableId");

    uint64_t tableId;
    if (!parseId(tableIdStr, tableId)) {
        log_info("handler getTableByIdHandler finished with status 404");
        string error = "{\"error\":\" tableId " + tableIdStr +  " is incorrect\"}\n";
        closeConnection(session, 404, error);
        return;
    }

    optional<TableInfo> tableInfo = getTableInfo(tableId);

    if (!tableInfo.has_value()) {
        log_info("handler getTableByIdHandler finished with status 404");
        string error = "{\"error\":\"Table with id " + tableIdStr +  "not found\"}\n";
        closeConnection(session, 404, error);
        return;
    }
    log_info("handler getTableByIdHandler finished with status 200");
    closeConnection(session, 200, prepareTableInfo(tableInfo.value()));

}

void createTableHandler(const shared_ptr<Session> session) {
    log_info("handler createTableHandler entered");
    int content_length = session->get_request()->get_header("Content-Length", 0);

    session->fetch(content_length,
        [](const std::shared_ptr<restbed::Session> session, const restbed::Bytes &body) {

            std::string json_body(body.begin(), body.end());

            json parsed;
            if (!parseJson(json_body, parsed)) {
                log_info("handler createTableHandler: invalid json");
                closeConnection(session, 400, parsed.dump());
                return;
            }

            CreateTableResult result = createTable(parsed);

            json response_json;
            int status = 200;

            if (result.problem.empty()) {
                response_json["tableId"] = result.tableId;
            } else {
                response_json = errorResponse(result.problem);
                status = 400;
            }
            log_info("handler createTableHandler finished with status " + std::to_string(status));
            closeConnection(session, status, response_json.dump());
        }
    );
}

void deleteTableHandler(const shared_ptr<Session> session) {
    log_info("handler deleteTableHandler entered");
    const auto request = session->get_request();
    std::string tableIdStr = request->get_path_parameter("tableId");

    uint64_t tableId;
    if (!parseId(tableIdStr, tableId)) {
        log_info("handler deleteTableHandler finished with status 404");
        string error = "{\"error\":\" tableId " + tableIdStr +  " is incorrect\"}\n";
        closeConnection(session, 404, error);
        return;
    }

    bool result = deleteTable(tableId);
    
    if (!result) {
        string error = "{\"error\":\"Table with id " + tableIdStr +  "not found\"}\n";
        closeConnection(session, 404, error);
        log_info("handler deleteTableHandler finished with status 404");
        return;
    }
    closeConnection(session, 200, "");
    log_info("handler deleteTableHandler finished with status 200");
}

void getQueriesHandler(const shared_ptr<Session> session) {
    log_info("handler getQueriesHandler entered");
    closeConnection(session, 200,getQueries().dump() );
}

void submitQueryHandler(const shared_ptr<Session> session) {
    log_info("handler submitQueryHandler entered");
    int content_length = session->get_request()->get_header("Content-Length", 0);

    session->fetch(content_length,
        [](const std::shared_ptr<restbed::Session> session, const restbed::Bytes &body) {
            std::string json_body(body.begin(), body.end());
            
            json json_message;
            if (!parseJson(json_body, json_message)) {
                log_info("handler submitQueryHandler: invalid json");
                closeConnection(session, 400, json_message.dump());
                return;
            }

            QueryType type = recogniseQuery(json_message);

            if (type == QueryType::ERROR) {
                log_info("handler submitQueryHandler: Invalid QueryType");
                closeConnection(session, 400, "Invalid QueryType");
                return;
            }

            const auto &def = json_message["queryDefinition"];
            string query_id = generateID();
            initQuery(query_id);
            json jsonResponse;
            jsonResponse["queryId"] = query_id;
            changeStatus(query_id, QueryStatus::PLANNING);
            log_info("submitQuery changed status to PLANNING");

            switch(type) {
                case QueryType::COPY: {
                    log_info("submitQuery handling COPY query");
                    CopyQuery copyQuery = createCopyQuery(def["CopyQuery"]); 
                    
                    addQueryDefinition(query_id, copyQuery);
                    QueryCreatedResponse response = copyCSV(copyQuery, query_id);

                    if (response.status == CSV_TABLE_ERROR::NONE) {
                        changeStatus(query_id, QueryStatus::COMPLETED);
                        log_info("submitQuery - copy finished with status 200");
                        closeConnection(session, 200, jsonResponse.dump());
                        return;
                    }
                    log_info("submitQuery - copy finished with status 400");
                    string error = handleCsvError(query_id, response.status);

                    closeConnection(session, 400, error);
                    break;
                } 
                case QueryType::SELECT: {
                    const auto &select_query = def["SelectQuery"];
                    SelectQuery sq;
                    sq.tableName = select_query["tableName"];
                    addQueryDefinition(query_id, sq);
                    SELECT_TABLE_ERROR response = selectTable(sq, query_id);

                    if (response == SELECT_TABLE_ERROR::NONE){
                        changeStatus(query_id, QueryStatus::COMPLETED);
                        log_info("submitQuery - select finished with status 200");
                        closeConnection(session, 200, jsonResponse.dump());
                        return;
                    }
                    
                    changeStatus(query_id, QueryStatus::FAILED);
                    string error = "Table " + select_query["tableName"].dump() + " does not exist" ;
                    log_info("submitQuery - select finished with status 400");
                    closeConnection(session, 400, createErrorResponse(error).dump());
                }
            }
        }
    );
}

void getQueryHandler(const shared_ptr<Session> session) {
    log_info("handler getQueryHandler entered");
    const auto request = session->get_request();
    std::string queryIdStr = request->get_path_parameter("queryId");

    uint64_t queryId;
    if (!parseId(queryIdStr, queryId)) {
        log_info("handler getQueryHandler finished with status 404");
        string error = "{\"error\":\" queryId " + queryIdStr + " is incorrect\"}\n";
        closeConnection(session, 404, error);
        return;
    }

    optional<QueryResponse> result = getQueryResponse(queryIdStr);

    if (!result.has_value()) {
        log_info("handler getQueryHandler finished with status 404");
        string error = "{\"error\":\"Query with this id not found\"}\n";
        closeConnection(session, 404, error);
        return;
    }
    json response = prepareQueryResponse(result.value());
    log_info("handler getQueryHandler finished with status 200");
    closeConnection(session, 200, response.dump());
}

void getQueryResultHandler(const shared_ptr<Session> session) {
    log_info("handler getQueryResultHandler entered");
    const auto request = session->get_request();
    std::string queryIdStr = request->get_path_parameter("queryId");
    uint64_t queryId;
    if (!parseId(queryIdStr, queryId)) {
        log_info("handler getQueryResultHandler finished with status 404");
        string error = "{\"error\":\" queryId " + queryIdStr + " is incorrect\"}\n";
        closeConnection(session, 404, error);
        return;
    }

    int content_length = session->get_request()->get_header("Content-Length", 0);
    session->fetch(content_length, [queryIdStr](const std::shared_ptr<restbed::Session> sess, const restbed::Bytes &body) {
        std::string json_body(body.begin(), body.end());
        int rowLimit = 0;
        bool flushResult = false;
        json parsed;

        if (!json_body.empty()) {
            if (parseJson(json_body, parsed)) {
                if (parsed.contains("rowLimit") && parsed["rowLimit"].is_number_integer()) {
                    rowLimit = parsed["rowLimit"].get<int>();
                }
                if (parsed.contains("flushResult") && parsed["flushResult"].is_boolean()) {
                    flushResult = parsed["flushResult"].get<bool>();
                }
            } else {
                closeConnection(sess, 400, parsed.dump());
                return;
            }
        }

        optional<QueryResult> result = getQueryResult(queryIdStr, rowLimit);
        if (!result.has_value()) {
            log_info("handler getQueryResultHandler finished with status 404");
            string error = "{\"error\":\"Query with this id not found\"}\n";
            closeConnection(sess, 404, error);
            return;
        }
        QueryResult qr = result.value();
        json response = json::array();
        response.push_back(prepareQueryResultResponse(qr));
        log_info("handler getQueryResultHandler finished with status 200");
        closeConnection(sess, 200, response.dump());

        if (flushResult) {
            removeResult(queryIdStr);
            log_info(std::string("handler getQueryResultHandler: flushed result for query ") + queryIdStr);
        }
    });
}

void getQueryErrorHandler(const shared_ptr<Session> session) {
    log_info("handler getQueryErrorHandler entered");
    const auto request = session->get_request();
    std::string queryIdStr = request->get_path_parameter("queryId");

    uint64_t queryId;
    if (!parseId(queryIdStr, queryId)) {
        log_info("handler getQueryErrorHandler finished with status 404");
        string error = "{\"error\":\" queryId " + queryIdStr + " is incorrect\"}\n";
        closeConnection(session, 404, error);
        return;
    }

    optional<QueryError> result = getQueryError(queryIdStr);

    if (!result.has_value()) {
        log_info("handler getQueryErrorHandler finished with status 404");
        string error = "{\"error\":\"Query with this id not found\"}\n";
        closeConnection(session, 404, error);
        return;
    }
    json response = prepareQueryErrorResponse(result.value());
    log_info("handler getQueryErrorHandler finished with status 200");
    closeConnection(session, 200, response.dump());
}

void getSystemHandler(const shared_ptr<Session> session) {
    log_info("handler getSystemHandler entered");
    closeConnection(session, 200, getSystemInfo().dump());
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

    auto systemResource = make_shared<Resource>();
    systemResource->set_path("/system/info");
    systemResource->set_method_handler("GET", getSystemHandler);

    auto settings = make_shared<Settings>();
    settings->set_port(8085);
    settings->set_bind_address("0.0.0.0");
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
    service.publish(systemResource);

    service.start(settings);
}

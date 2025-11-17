#include <iostream>
#include <fstream>
#include <unordered_map>
#include <memory>
#include <string>

#include "corvusoft/restbed/settings.hpp"
#include "corvusoft/restbed/resource.hpp"
#include "corvusoft/restbed/service.hpp"
#include "corvusoft/restbed/request.hpp"
#include "corvusoft/restbed/response.hpp"
#include "corvusoft/restbed/session.hpp"
#include "corvusoft/restbed/logger.hpp"



using namespace std;
using namespace restbed;

extern void simpleTest();
extern void simpleBatchTest();
extern void columnTest();
extern void someFilesTest();
extern void bigTest();
extern void clearAfterTests();

void f1() { }
void f2() { }
void f3() { }
void f4() { }
void f5() { }
void f6() { }


void getTablesHandler(const shared_ptr<Session> session)
{
    f1();
    session->close(200, "[]", { {"Content-Type", "application/json"} });
}

void getTableByIdHandler(const shared_ptr<Session> session)
{
    f2();
    session->close(200, "{}", { {"Content-Type", "application/json"} });
}

void createTableHandler(const shared_ptr<Session> session)
{
    f3();
    session->close(200, "\"new_table_id\"", { {"Content-Type", "application/json"} });
}

void deleteTableHandler(const shared_ptr<Session> session)
{
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
int main(){
    columnTest();
    // simpleTest();
    // simpleBatchTest();
    // someFilesTest();
    // bigTest();
    // clearAfterTests();

    auto tablesResource = make_shared<Resource>();
    tablesResource->set_path("/tables");
    tablesResource->set_method_handler("GET", getTablesHandler);

    auto tableResource = make_shared<Resource>();
    // Restbed requires path parameters to include a pattern, e.g. {name: .*}
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
    return 0;
}

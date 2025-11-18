#include <iostream>
#include <fstream>
#include <unordered_map>
#include <memory>
#include <string>

#include "service/utils.h"

#include "corvusoft/restbed/settings.hpp"
#include "corvusoft/restbed/resource.hpp"
#include "corvusoft/restbed/service.hpp"
#include "corvusoft/restbed/request.hpp"
#include "corvusoft/restbed/response.hpp"
#include "corvusoft/restbed/session.hpp"
#include "corvusoft/restbed/logger.hpp"



using namespace std;
// using namespace restbed;

extern void simpleTest();
extern void simpleBatchTest();
extern void columnTest();
extern void someFilesTest();
extern void bigTest();
extern void clearAfterTests();

// void f1() { }
// void f2() { }
// void f3() { }
// void f4() { }
// void f5() { }
// void f6() { }


// void getTablesHandler(const shared_ptr<Session> session)
// {
//     f1();
//     session->close(200, "[]", { {"Content-Type", "application/json"} });
// }

// void getTableByIdHandler(const shared_ptr<Session> session)
// {
//     f2();
//     session->close(200, "{}", { {"Content-Type", "application/json"} });
// }

// void createTableHandler(const shared_ptr<Session> session)
// {
//     f3();
//     session->close(200, "\"new_table_id\"", { {"Content-Type", "application/json"} });
// }

// void deleteTableHandler(const shared_ptr<Session> session)
// {
//     f4();
//     session->close(200, "", {});
// }

// void submitQueryHandler(const shared_ptr<Session> session)
// {
//     f5();
//     session->close(200, "\"new_query_id\"", { {"Content-Type", "application/json"} });
// }

// void getQueryResultHandler(const shared_ptr<Session> session)
// {
//     f6();
//     session->close(200, "[]", { {"Content-Type", "application/json"} });
// }
int main(){
    // columnTest();
    // simpleTest();
    // simpleBatchTest();
    // someFilesTest();
    // bigTest();
    // clearAfterTests();
    setUpApi();
}

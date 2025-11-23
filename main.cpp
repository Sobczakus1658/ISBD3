#include <iostream>
#include <fstream>
#include <unordered_map>
#include <memory>
#include <string>

#include "controler.h"
#include "corvusoft/restbed/settings.hpp"
#include "corvusoft/restbed/resource.hpp"
#include "corvusoft/restbed/service.hpp"
#include "corvusoft/restbed/request.hpp"
#include "corvusoft/restbed/response.hpp"
#include "corvusoft/restbed/session.hpp"
#include "corvusoft/restbed/logger.hpp"

using namespace std;

extern void simpleTest();
extern void simpleBatchTest();
extern void columnTest();
extern void someFilesTest();
extern void bigTest();
extern void clearAfterTests();

int main(){
    // columnTest();
    // simpleTest();
    // simpleBatchTest();
    // someFilesTest();
    // bigTest();
    // clearAfterTests();
    setUpApi();
}

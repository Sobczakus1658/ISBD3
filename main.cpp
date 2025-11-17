#include <iostream>
#include <fstream>
#include <unordered_map>

extern void simpleTest();
extern void simpleBatchTest();
extern void columnTest();
extern void someFilesTest();
extern void bigTest();
extern void clearAfterTests();

int main(){
    columnTest();
    simpleTest();
    simpleBatchTest();
    someFilesTest();
    bigTest();
    clearAfterTests();
    return 0;
}

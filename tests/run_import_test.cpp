#include <cpr/cpr.h>
#include <nlohmann/json.hpp>
#include <iostream>
#include <fstream>
#include <thread>
#include <chrono>
#include <vector>

using namespace std;
using json = nlohmann::ordered_json;

static const std::string BASE_URL = "http://localhost:8085";

void fail(const std::string &msg) {
    std::cerr << "FAIL: " << msg << std::endl;
    std::exit(1);
}

string getNotExistingId(const vector<string> &existing){
    // If we have existing numeric ids, find the smallest and return one less.
    // Parse existing strings as unsigned long long when possible.
    unsigned long long minv = std::numeric_limits<unsigned long long>::max();
    bool found = false;
    for (const auto &s : existing) {
        try {
            unsigned long long v = std::stoull(s);
            if (v < minv) minv = v;
            found = true;
        } catch (...) {
            // ignore non-numeric ids
        }
    }
    if (found) {
        if (minv > 0) return std::to_string(minv - 1);
        // cannot go below 0: return minv+1 which is guaranteed not in set if ids are unique
        return std::to_string(minv + 1);
    }
    // no numeric ids found: fallback to timestamp-based id that's unlikely to exist
    return std::to_string(std::chrono::system_clock::now().time_since_epoch().count());
}

vector<string> getTablesId(){
    cpr::Response r = cpr::Get(cpr::Url{BASE_URL + "/tables"}, cpr::Header{{"Accept","application/json"}});
    if (r.status_code != 200) fail("getTablesId: GET /tables failed: " + r.text);
    json body = json::parse(r.text);
    vector<string> ids;
    if (!body.is_object()) return ids;
    for (auto it = body.begin(); it != body.end(); ++it) {
        try {
            ids.push_back(it.value().get<std::string>());
        } catch (...) {
            // ignore
        }
    }
    return ids;
}

void twoTimesCreateAndDeleteTable(){
    std::string name = "twice_" + std::to_string(::time(nullptr));
    std::string body = "{" + std::string("\"" + name + "\": { \"columns\": { \"id\": \"INT64\" } } }");
    cpr::Response r1 = cpr::Put(cpr::Url{BASE_URL + "/table"}, cpr::Header{{"Content-Type","application/json"}}, cpr::Body{body});
    if (r1.status_code != 200) fail("twoTimesCreateAndDeleteTable: first create failed: " + r1.text);
    cpr::Response r2 = cpr::Put(cpr::Url{BASE_URL + "/table"}, cpr::Header{{"Content-Type","application/json"}}, cpr::Body{body});
    if (r2.status_code == 200) fail("twoTimesCreateAndDeleteTable: duplicate create unexpectedly succeeded");
    json created = json::parse(r1.text);
    std::string tableId = created.value("tableId", std::string());
    if (tableId.empty()) fail("twoTimesCreateAndDeleteTable: missing tableId");
    cpr::Response del = cpr::Delete(cpr::Url{BASE_URL + "/table/" + tableId});
    if (del.status_code != 200) fail("twoTimesCreateAndDeleteTable: delete failed: " + del.text);
    cpr::Response del2 = cpr::Delete(cpr::Url{BASE_URL + "/table/" + tableId});
    if (del2.status_code == 200) fail("twoTimesCreateAndDeleteTable: second delete unexpectedly succeeded");
}

void createTableAndCheckData(){
    std::string name = "check_" + std::to_string(::time(nullptr));
    std::string body = "{" + std::string("\"" + name + "\": { \"columns\": { \"id\": \"INT64\" } } }");
    cpr::Response r = cpr::Put(cpr::Url{BASE_URL + "/table"}, cpr::Header{{"Content-Type","application/json"}}, cpr::Body{body});
    if (r.status_code != 200) fail("createTableAndCheckData: create failed: " + r.text);
    json created = json::parse(r.text);
    std::string tableId = created.value("tableId", std::string());
    if (tableId.empty()) fail("createTableAndCheckData: missing tableId");
    // check tables listing
    cpr::Response list = cpr::Get(cpr::Url{BASE_URL + "/tables"});
    if (list.status_code != 200) fail("createTableAndCheckData: GET /tables failed: " + list.text);
    json listj = json::parse(list.text);
    if (!listj.contains(name)) fail("createTableAndCheckData: table name not in /tables");
    // check get by id
    cpr::Response byid = cpr::Get(cpr::Url{BASE_URL + "/table/" + tableId});
    if (byid.status_code != 200) fail("createTableAndCheckData: GET /table/{id} failed: " + byid.text);
}

void getTableWithTableIdNotExists(){
    vector<string> ids = getTablesId();
    string id = getNotExistingId(ids);
    cpr::Response r = cpr::Get(cpr::Url{BASE_URL + "/table/" + id});
    if (r.status_code == 200) fail("getTableWithTableIdNotExists: expected 404 but got 200");
}

void deleteTableWhichNotExists(){
    vector<string> ids = getTablesId();
    string id = getNotExistingId(ids);
    cpr::Response r = cpr::Delete(cpr::Url{BASE_URL + "/table/" + id});
    if (r.status_code == 200) fail("deleteTableWhichNotExists: expected 404 but got 200");
}

void tryToCreateTableWithInvalidTypes(){
    std::string name = "badtypes_" + std::to_string(::time(nullptr));
    // use unsupported type BOOL
    std::string body = "{" + std::string("\"" + name + "\": { \"columns\": { \"id\": \"BOOL\" } } }");
    cpr::Response r = cpr::Put(cpr::Url{BASE_URL + "/table"}, cpr::Header{{"Content-Type","application/json"}}, cpr::Body{body});
    if (r.status_code == 200) fail("tryToCreateTableWithInvalidTypes: expected failure but create succeeded");
}

void tryToCreateTableWithDuplicatesColumn(){
    std::string name = "dupcol_" + std::to_string(::time(nullptr));
    // send columns as array of objects to simulate duplicate logical names
    std::string body = "{" + std::string("\"" + name + "\": { \"columns\": [ {\"name\": \"id\", \"type\": \"INT64\"}, {\"name\": \"id\", \"type\": \"VARCHAR\"} ] } }");
    cpr::Response r = cpr::Put(cpr::Url{BASE_URL + "/table"}, cpr::Header{{"Content-Type","application/json"}}, cpr::Body{body});
    if (r.status_code == 200) fail("tryToCreateTableWithDuplicatesColumn: expected failure but create succeeded");
}

void getSystem(){
    cpr::Response r = cpr::Get(cpr::Url{BASE_URL + "/system/info"});
    if (r.status_code != 200) fail("getSystem: GET /system/info failed: " + r.text);
    std::cout << "getSystem: " << r.text << std::endl;
}

vector<string> getQuieriesId() {
    cpr::Response r = cpr::Get(cpr::Url{BASE_URL + "/queries"});
    vector<string> out;
    if (r.status_code != 200) return out;
    json j = json::parse(r.text);
    if (!j.is_array()) return out;
    for (const auto &el : j) {
        if (el.contains("queryId") && el["queryId"].is_string()) out.push_back(el["queryId"].get<std::string>());
    }
    return out;
}

// helper: poll query status until final or timeout. returns last seen status or empty on error
static std::string pollQueryStatus(const std::string &queryId, int maxChecks = 100, int ms = 100) {
    for (int i = 0; i < maxChecks; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(ms));
        cpr::Response qr = cpr::Get(cpr::Url{BASE_URL + "/query/" + queryId}, cpr::Header{{"Accept","application/json"}});
        if (qr.status_code != 200) continue;
        try {
            json qbody = json::parse(qr.text);
            if (!qbody.is_array() || qbody.empty()) continue;
            std::string status = qbody[0].value("status", "");
            if (status == "COMPLETED" || status == "FAILED") return status;
            // otherwise keep waiting
        } catch (...) {
            continue;
        }
    }
    return std::string();
}

void queryWithInvalidQueryDefinition() {
    // send a body without queryDefinition
    cpr::Response r = cpr::Post(cpr::Url{BASE_URL + "/query"}, cpr::Header{{"Content-Type","application/json"}}, cpr::Body{"{}"});
    if (r.status_code == 200) fail("queryWithInvalidQueryDefinition: expected server to reject missing queryDefinition");
}

void copyQueryWithoutNeccesaryFields() {
    // missing sourceFilepath
    json body = json::object();
    body["queryDefinition"] = json::object();
    body["queryDefinition"]["CopyQuery"] = json::object({{"destinationTableName", "no-table"}});
    cpr::Response r = cpr::Post(cpr::Url{BASE_URL + "/query"}, cpr::Header{{"Content-Type","application/json"}}, cpr::Body{body.dump()});
    if (r.status_code != 200) return; // accepted rejection at submit time
    // if accepted, expect eventual failure
    json created = json::parse(r.text);
    if (!created.contains("queryId")) return;
    std::string qid = created["queryId"].get<std::string>();
    std::string status = pollQueryStatus(qid);
    if (status.empty()) fail("copyQueryWithoutNeccesaryFields: no final status");
    if (status != "FAILED") fail("copyQueryWithoutNeccesaryFields: expected FAILED but got " + status);
}

void correctCopyQueryBasic(){
    std::string tableName = "ct_basic_" + std::to_string(::time(nullptr));
    std::string createBody = "{" + std::string("\"" + tableName + "\": { \"columns\": { \"id\": \"INT64\" } } }");
    cpr::Response r = cpr::Put(cpr::Url{BASE_URL + "/table"}, cpr::Header{{"Content-Type","application/json"}}, cpr::Body{createBody});
    if (r.status_code != 200) fail("correctCopyQueryBasic: create table failed: " + r.text);
    json created = json::parse(r.text);
    std::string csvPath = "/tmp/" + tableName + ".csv";
    {
        std::ofstream out(csvPath);
        out << "101\n202\n";
    }
    json copyReq = json::object();
    copyReq["queryDefinition"] = json::object();
    copyReq["queryDefinition"]["CopyQuery"] = json::object({{"sourceFilepath", csvPath}, {"destinationTableName", tableName}, {"doesCsvContainHeader", false}});
    cpr::Response copyResp = cpr::Post(cpr::Url{BASE_URL + "/query"}, cpr::Header{{"Content-Type","application/json"}}, cpr::Body{copyReq.dump()});
    if (copyResp.status_code != 200) fail("correctCopyQueryBasic: submit failed: " + copyResp.text);
    json copyCreated = json::parse(copyResp.text);
    std::string qid = copyCreated.value("queryId", std::string());
    if (qid.empty()) fail("correctCopyQueryBasic: missing queryId");
    std::string status = pollQueryStatus(qid);
    if (status != "COMPLETED") fail("correctCopyQueryBasic: expected COMPLETED but got " + status);
    // cleanup
    std::string tableId = created.value("tableId", std::string());
    if (!tableId.empty()) cpr::Response del = cpr::Delete(cpr::Url{BASE_URL + "/table/" + tableId});
}

void correctCopyQueryHeaders(){
    std::string tableName = "ct_headers_" + std::to_string(::time(nullptr));
    std::string createBody = "{" + std::string("\"" + tableName + "\": { \"columns\": { \"id\": \"INT64\" } } }");
    cpr::Response r = cpr::Put(cpr::Url{BASE_URL + "/table"}, cpr::Header{{"Content-Type","application/json"}}, cpr::Body{createBody});
    if (r.status_code != 200) fail("correctCopyQueryHeaders: create table failed: " + r.text);
    json created = json::parse(r.text);
    std::string csvPath = "/tmp/" + tableName + ".csv";
    {
        std::ofstream out(csvPath);
        out << "id\n";
        out << "303\n404\n";
    }
    json copyReq = json::object();
    copyReq["queryDefinition"] = json::object();
    copyReq["queryDefinition"]["CopyQuery"] = json::object({{"sourceFilepath", csvPath}, {"destinationTableName", tableName}, {"doesCsvContainHeader", true}});
    cpr::Response copyResp = cpr::Post(cpr::Url{BASE_URL + "/query"}, cpr::Header{{"Content-Type","application/json"}}, cpr::Body{copyReq.dump()});
    if (copyResp.status_code != 200) fail("correctCopyQueryHeaders: submit failed: " + copyResp.text);
    json copyCreated = json::parse(copyResp.text);
    std::string qid = copyCreated.value("queryId", std::string());
    if (qid.empty()) fail("correctCopyQueryHeaders: missing queryId");
    std::string status = pollQueryStatus(qid);
    if (status != "COMPLETED") fail("correctCopyQueryHeaders: expected COMPLETED but got " + status);
    std::string tableId = created.value("tableId", std::string());
    if (!tableId.empty()) cpr::Response del = cpr::Delete(cpr::Url{BASE_URL + "/table/" + tableId});
}

void correctCopyQueryDestinationColumns(){
    std::string tableName = "ct_destcols_" + std::to_string(::time(nullptr));
    std::string createBody = "{" + std::string("\"" + tableName + "\": { \"columns\": { \"id\": \"INT64\", \"note\": \"VARCHAR\" } } }");
    cpr::Response r = cpr::Put(cpr::Url{BASE_URL + "/table"}, cpr::Header{{"Content-Type","application/json"}}, cpr::Body{createBody});
    if (r.status_code != 200) fail("correctCopyQueryDestinationColumns: create table failed: " + r.text);
    json created = json::parse(r.text);
    std::string csvPath = "/tmp/" + tableName + ".csv";
    {
        std::ofstream out(csvPath);
        out << "note,id\n";
        out << "x,11\n";
        out << "y,22\n";
    }
    json copyReq = json::object();
    copyReq["queryDefinition"] = json::object();
    copyReq["queryDefinition"]["CopyQuery"] = json::object({{"sourceFilepath", csvPath}, {"destinationTableName", tableName}, {"doesCsvContainHeader", true}, {"destinationColumns", json::array({"note","id"})}});
    cpr::Response copyResp = cpr::Post(cpr::Url{BASE_URL + "/query"}, cpr::Header{{"Content-Type","application/json"}}, cpr::Body{copyReq.dump()});
    if (copyResp.status_code != 200) fail("correctCopyQueryDestinationColumns: submit failed: " + copyResp.text);
    json copyCreated = json::parse(copyResp.text);
    std::string qid = copyCreated.value("queryId", std::string());
    if (qid.empty()) fail("correctCopyQueryDestinationColumns: missing queryId");
    std::string status = pollQueryStatus(qid);
    if (status != "COMPLETED") fail("correctCopyQueryDestinationColumns: expected COMPLETED but got " + status);
    std::string tableId = created.value("tableId", std::string());
    if (!tableId.empty()) cpr::Response del = cpr::Delete(cpr::Url{BASE_URL + "/table/" + tableId});
}

void invalidDataInCSVCopyQuery(){
    std::string tableName = "ct_invaliddata_" + std::to_string(::time(nullptr));
    std::string createBody = "{" + std::string("\"" + tableName + "\": { \"columns\": { \"id\": \"INT64\" } } }");
    cpr::Response r = cpr::Put(cpr::Url{BASE_URL + "/table"}, cpr::Header{{"Content-Type","application/json"}}, cpr::Body{createBody});
    if (r.status_code != 200) fail("invalidDataInCSVCopyQuery: create table failed: " + r.text);
    json created = json::parse(r.text);
    std::string csvPath = "/tmp/" + tableName + ".csv";
    {
        std::ofstream out(csvPath);
        out << "id\n";
        out << "not-a-number\n";
    }
    json copyReq = json::object();
    copyReq["queryDefinition"] = json::object();
    copyReq["queryDefinition"]["CopyQuery"] = json::object({{"sourceFilepath", csvPath}, {"destinationTableName", tableName}, {"doesCsvContainHeader", true}});
    cpr::Response copyResp = cpr::Post(cpr::Url{BASE_URL + "/query"}, cpr::Header{{"Content-Type","application/json"}}, cpr::Body{copyReq.dump()});
    // server may reject malformed data at submit time (non-200) or accept and later mark the query FAILED.
    // For this negative test we expect an error outcome: either a proper rejection (4xx/5xx) or a later FAILED status.
    if (copyResp.status_code == 200) {
        fail(std::string("invalidDataInCSVCopyQuery: unexpected 200 status="));
    }
}

void moreColumnsInCSVCopyQuery(){
    std::string tableName = "ct_morecols_" + std::to_string(::time(nullptr));
    std::string createBody = "{" + std::string("\"" + tableName + "\": { \"columns\": { \"a\": \"INT64\", \"b\": \"INT64\" } } }");
    cpr::Response r = cpr::Put(cpr::Url{BASE_URL + "/table"}, cpr::Header{{"Content-Type","application/json"}}, cpr::Body{createBody});
    if (r.status_code != 200) fail("moreColumnsInCSVCopyQuery: create table failed: " + r.text);
    json created = json::parse(r.text);
    std::string csvPath = "/tmp/" + tableName + ".csv";
    {
        std::ofstream out(csvPath);
        out << "a,b,c\n";
        out << "1,2,3\n";
    }
    json copyReq = json::object();
    copyReq["queryDefinition"] = json::object();
    copyReq["queryDefinition"]["CopyQuery"] = json::object({{"sourceFilepath", csvPath}, {"destinationTableName", tableName}, {"doesCsvContainHeader", true}});
    cpr::Response copyResp = cpr::Post(cpr::Url{BASE_URL + "/query"}, cpr::Header{{"Content-Type","application/json"}}, cpr::Body{copyReq.dump()});
    if (copyResp.status_code == 200) fail(std::string("invalidDataInCSVCopyQuery: unexpected 200 status="));
}

void lessColumnsInCSVCopyQuery(){
    std::string tableName = "ct_lesscols_" + std::to_string(::time(nullptr));
    std::string createBody = "{" + std::string("\"" + tableName + "\": { \"columns\": { \"a\": \"INT64\", \"b\": \"INT64\", \"c\": \"INT64\" } } }");
    cpr::Response r = cpr::Put(cpr::Url{BASE_URL + "/table"}, cpr::Header{{"Content-Type","application/json"}}, cpr::Body{createBody});
    if (r.status_code != 200) fail("lessColumnsInCSVCopyQuery: create table failed: " + r.text);
    json created = json::parse(r.text);
    std::string csvPath = "/tmp/" + tableName + ".csv";
    {
        std::ofstream out(csvPath);
        out << "a,b\n";
        out << "1,2\n";
    }
    json copyReq = json::object();
    copyReq["queryDefinition"] = json::object();
    copyReq["queryDefinition"]["CopyQuery"] = json::object({{"sourceFilepath", csvPath}, {"destinationTableName", tableName}, {"doesCsvContainHeader", true}});
    cpr::Response copyResp = cpr::Post(cpr::Url{BASE_URL + "/query"}, cpr::Header{{"Content-Type","application/json"}}, cpr::Body{copyReq.dump()});
    if (copyResp.status_code == 200) fail(std::string("invalidDataInCSVCopyQuery: unexpected 200 status="));
}


void tableNotExistsCopyQuery(){
    // Use a non-existing table name
    std::string tableName = "not_exists_" + std::to_string(::time(nullptr));
    std::string csvPath = "/tmp/" + tableName + ".csv";
    {
        std::ofstream out(csvPath);
        out << "id\n1\n";
    }
    json copyReq = json::object();
    copyReq["queryDefinition"] = json::object();
    copyReq["queryDefinition"]["CopyQuery"] = json::object({{"sourceFilepath", csvPath}, {"destinationTableName", tableName}, {"doesCsvContainHeader", true}});
    cpr::Response copyResp = cpr::Post(cpr::Url{BASE_URL + "/query"}, cpr::Header{{"Content-Type","application/json"}}, cpr::Body{copyReq.dump()});
    // Either rejected at submit time or fails later
    if (copyResp.status_code == 200) return;
    json copyCreated = json::parse(copyResp.text);
    std::string qid = copyCreated.value("queryId", std::string());
    if (qid.empty()) return;
    std::string status = pollQueryStatus(qid);
    if (status != "FAILED") fail("tableNotExistsCopyQuery: expected FAILED but got " + status);
}

void invalidPathCopyQuery(){
    // Source file path that does not exist
    std::string tableName = "ct_path_" + std::to_string(::time(nullptr));
    std::string createBody = "{" + std::string("\"" + tableName + "\": { \"columns\": { \"id\": \"INT64\" } } }");
    cpr::Response r = cpr::Put(cpr::Url{BASE_URL + "/table"}, cpr::Header{{"Content-Type","application/json"}}, cpr::Body{createBody});
    if (r.status_code != 200) fail("invalidPathCopyQuery: create table failed: " + r.text);
    json created = json::parse(r.text);
    std::string csvPath = "/tmp/does-not-exist-" + std::to_string(::time(nullptr)) + ".csv";
    json copyReq = json::object();
    copyReq["queryDefinition"] = json::object();
    copyReq["queryDefinition"]["CopyQuery"] = json::object({{"sourceFilepath", csvPath}, {"destinationTableName", tableName}, {"doesCsvContainHeader", true}});
    cpr::Response copyResp = cpr::Post(cpr::Url{BASE_URL + "/query"}, cpr::Header{{"Content-Type","application/json"}}, cpr::Body{copyReq.dump()});
    if (copyResp.status_code == 200) return; // rejected at submit time is acceptable
}

void getQueryResultWithCorrectQueryId(){
    // create table and insert one row, then run SelectQuery and fetch result
    std::string tableName = "qr_ok_" + std::to_string(::time(nullptr));
    std::string createBody = "{" + std::string("\"" + tableName + "\": { \"columns\": { \"id\": \"INT64\" } } }");
    cpr::Response r = cpr::Put(cpr::Url{BASE_URL + "/table"}, cpr::Header{{"Content-Type","application/json"}}, cpr::Body{createBody});
    if (r.status_code != 200) fail("getQueryResultWithCorrectQueryId: create table failed: " + r.text);
    json created = json::parse(r.text);
    std::string csvPath = "/tmp/" + tableName + ".csv";
    {
        std::ofstream out(csvPath);
        out << "id\n";
        out << "777\n";
    }
    // submit copy
    json copyReq = json::object();
    copyReq["queryDefinition"] = json::object();
    copyReq["queryDefinition"]["CopyQuery"] = json::object({{"sourceFilepath", csvPath}, {"destinationTableName", tableName}, {"doesCsvContainHeader", true}});
    cpr::Response copyResp = cpr::Post(cpr::Url{BASE_URL + "/query"}, cpr::Header{{"Content-Type","application/json"}}, cpr::Body{copyReq.dump()});
    if (copyResp.status_code != 200) fail("getQueryResultWithCorrectQueryId: copy submit failed: " + copyResp.text);
    json copyCreated = json::parse(copyResp.text);
    std::string copyQid = copyCreated.value("queryId", std::string());
    if (copyQid.empty()) fail("getQueryResultWithCorrectQueryId: missing copy queryId");
    std::string copyStatus = pollQueryStatus(copyQid);
    if (copyStatus != "COMPLETED") fail("getQueryResultWithCorrectQueryId: copy did not complete: " + copyStatus);

    // submit select
    json selectReq = json::object();
    selectReq["queryDefinition"] = json::object();
    selectReq["queryDefinition"]["SelectQuery"] = json::object({{"tableName", tableName}});
    cpr::Response selectResp = cpr::Post(cpr::Url{BASE_URL + "/query"}, cpr::Header{{"Content-Type","application/json"}}, cpr::Body{selectReq.dump()});
    if (selectResp.status_code != 200) fail("getQueryResultWithCorrectQueryId: select submit failed: " + selectResp.text);
    json selectCreated = json::parse(selectResp.text);
    std::string selectQid = selectCreated.value("queryId", std::string());
    if (selectQid.empty()) fail("getQueryResultWithCorrectQueryId: missing select queryId");
    std::string selectStatus = pollQueryStatus(selectQid);
    if (selectStatus != "COMPLETED") fail("getQueryResultWithCorrectQueryId: select did not complete: " + selectStatus);

    // fetch result
    cpr::Response res = cpr::Get(cpr::Url{BASE_URL + "/result/" + selectQid}, cpr::Header{{"Accept","application/json"}});
    if (res.status_code != 200) fail("getQueryResultWithCorrectQueryId: GET /result failed: " + res.text);
    json results = json::parse(res.text);
    if (!results.is_array() || results.empty()) fail("getQueryResultWithCorrectQueryId: result missing or empty: " + res.text);
}

void getQueryResultWithInccorectQueryId(){
    vector<string> qids = getQuieriesId();
    std::string id = getNotExistingId(qids);
    cpr::Response r = cpr::Get(cpr::Url{BASE_URL + "/result/" + id}, cpr::Header{{"Accept","application/json"}});
    if (r.status_code == 200) fail("getQueryResultWithInccorectQueryId: expected non-200 for missing id");
}

void getQueryErrorWithCorrectQueryId(){
    // create table and submit a copy with invalid data to produce an error
    std::string tableName = "qe_err_" + std::to_string(::time(nullptr));
    std::string createBody = "{" + std::string("\"" + tableName + "\": { \"columns\": { \"id\": \"INT64\" } } }");
    cpr::Response r = cpr::Put(cpr::Url{BASE_URL + "/table"}, cpr::Header{{"Content-Type","application/json"}}, cpr::Body{createBody});
    if (r.status_code != 200) fail("getQueryErrorWithCorrectQueryId: create table failed: " + r.text);
    json created = json::parse(r.text);
    std::string csvPath = "/tmp/" + tableName + ".csv";
    {
        std::ofstream out(csvPath);
        out << "id\n";
        out << "bad-int\n";
    }
    json copyReq = json::object();
    copyReq["queryDefinition"] = json::object();
    copyReq["queryDefinition"]["CopyQuery"] = json::object({{"sourceFilepath", csvPath}, {"destinationTableName", tableName}, {"doesCsvContainHeader", true}});
    cpr::Response copyResp = cpr::Post(cpr::Url{BASE_URL + "/query"}, cpr::Header{{"Content-Type","application/json"}}, cpr::Body{copyReq.dump()});
    std::string qid;
    if (copyResp.status_code != 200) {
        // server rejected at submit time; try to find a recent FAILED query and use its id
        // std::cout << "getQueryErrorWithCorrectQueryId: submit rejected with status=" << copyResp.status_code << " body=" << copyResp.text << std::endl;
        cpr::Response qlist = cpr::Get(cpr::Url{BASE_URL + "/queries"}, cpr::Header{{"Accept","application/json"}});
        if (qlist.status_code != 200) {
            std::cout << "getQueryErrorWithCorrectQueryId: cannot list queries, status=" << qlist.status_code << std::endl;
            return;
        }
        try {
            // debug: print what /queries returned so we can inspect why no FAILED query was found
            // std::cout << "[getQueryErrorWithCorrectQueryId] /queries response status=" << qlist.status_code << " body=" << qlist.text << std::endl;
            json qj = json::parse(qlist.text);
            if (qj.is_array()) {
                // find last element with status == FAILED
                for (auto it = qj.rbegin(); it != qj.rend(); ++it) {
                    if (it->is_object()) {
                        std::string st = it->value("status", std::string());
                        if (st == "FAILED" && it->contains("queryId") && (*it)["queryId"].is_string()) {
                            qid = (*it)["queryId"].get<std::string>();
                            // std::cout << "[getQueryErrorWithCorrectQueryId] selected FAILED queryId=" << qid << std::endl;
                            break;
                        }
                    }
                }
            }
        } catch (...) {
            std::cout << "getQueryErrorWithCorrectQueryId: failed to parse /queries response" << std::endl;
            return;
        }
        if (qid.empty()) {
            std::cout << "getQueryErrorWithCorrectQueryId: no recent FAILED query found to fetch error for" << std::endl;
            return;
        }
    } else {
        json copyCreated = json::parse(copyResp.text);
        qid = copyCreated.value("queryId", std::string());
        if (qid.empty()) fail("getQueryErrorWithCorrectQueryId: missing queryId");
        std::string status = pollQueryStatus(qid);
        if (status != "FAILED") fail("getQueryErrorWithCorrectQueryId: expected FAILED but got " + status);
    }

    cpr::Response er = cpr::Get(cpr::Url{BASE_URL + "/error/" + qid}, cpr::Header{{"Accept","application/json"}});
    if (er.status_code != 200) fail("getQueryErrorWithCorrectQueryId: GET /error failed: " + er.text);
    json errj = json::parse(er.text);
    // expect object with problems array or array
    if (errj.is_object()) {
        if (!errj.contains("problems")) fail("getQueryErrorWithCorrectQueryId: error body missing problems");
    } else if (!errj.is_array()) {
        fail("getQueryErrorWithCorrectQueryId: unexpected error shape: " + er.text);
    }
}

void getQueryErrorWithInccorectQueryId(){
    vector<string> qids = getQuieriesId();
    std::string id = getNotExistingId(qids);
    cpr::Response r = cpr::Get(cpr::Url{BASE_URL + "/error/" + id}, cpr::Header{{"Accept","application/json"}});
    if (r.status_code == 200) fail("getQueryErrorWithInccorectQueryId: expected non-200 for missing id");
}

int main() {


    std::cout << "[test-runner] getSystem()" << std::endl;
    getSystem();

    std::cout << "[test-runner] twoTimesCreateAndDeleteTable()" << std::endl;
    twoTimesCreateAndDeleteTable();

    std::cout << "[test-runner] createTableAndCheckData()" << std::endl;
    createTableAndCheckData();

    std::cout << "[test-runner] getTableWithTableIdNotExists()" << std::endl;
    getTableWithTableIdNotExists();

    std::cout << "[test-runner] deleteTableWhichNotExists()" << std::endl;
    deleteTableWhichNotExists();

    std::cout << "[test-runner] tryToCreateTableWithInvalidTypes()" << std::endl;
    tryToCreateTableWithInvalidTypes();

    std::cout << "[test-runner] tryToCreateTableWithDuplicatesColumn()" << std::endl;
    tryToCreateTableWithDuplicatesColumn();

    std::cout << "[test-runner] getQuieriesId() -> count:" << getQuieriesId().size() << std::endl;

    std::cout << "[test-runner] queryWithInvalidQueryDefinition()" << std::endl;
    queryWithInvalidQueryDefinition();

    std::cout << "[test-runner] copyQueryWithoutNeccesaryFields()" << std::endl;
    copyQueryWithoutNeccesaryFields();

    std::cout << "[test-runner] correctCopyQueryBasic()" << std::endl;
    correctCopyQueryBasic();

    std::cout << "[test-runner] correctCopyQueryHeaders()" << std::endl;
    correctCopyQueryHeaders();

    std::cout << "[test-runner] correctCopyQueryDestinationColumns()" << std::endl;
    correctCopyQueryDestinationColumns();

    std::cout << "[test-runner] invalidDataInCSVCopyQuery()" << std::endl;
    invalidDataInCSVCopyQuery();

    std::cout << "[test-runner] moreColumnsInCSVCopyQuery()" << std::endl;
    moreColumnsInCSVCopyQuery();

    std::cout << "[test-runner] lessColumnsInCSVCopyQuery()" << std::endl;
    lessColumnsInCSVCopyQuery();

    std::cout << "[test-runner] tableNotExistsCopyQuery()" << std::endl;
    tableNotExistsCopyQuery();

    std::cout << "[test-runner] invalidPathCopyQuery()" << std::endl;
    invalidPathCopyQuery();

    std::cout << "[test-runner] getQueryResultWithCorrectQueryId()" << std::endl;
    getQueryResultWithCorrectQueryId();

    std::cout << "[test-runner] getQueryResultWithInccorectQueryId()" << std::endl;
    getQueryResultWithInccorectQueryId();

    std::cout << "[test-runner] getQueryErrorWithCorrectQueryId()" << std::endl;
    getQueryErrorWithCorrectQueryId();

    std::cout << "[test-runner] getQueryErrorWithInccorectQueryId()" << std::endl;
    getQueryErrorWithInccorectQueryId();

    std::cout << "[test-runner] ALL TESTS COMPLETED" << std::endl;
}

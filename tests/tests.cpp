#include <cpr/cpr.h>
#include <nlohmann/json.hpp>
#include <iostream>
#include <fstream>
#include <thread>
#include <chrono>
#include <vector>

using namespace std;
using json = nlohmann::ordered_json;

static const std::string BASE_URL = "http://localhost:8086";

void fail(const std::string &msg) {
    std::cerr << "FAIL: " << msg << std::endl;
    std::exit(1);
}

string getNotExistingId(const vector<string> &existing){
    unsigned long long minv = std::numeric_limits<unsigned long long>::max();
    bool found = false;
    for (const auto &s : existing) {
        unsigned long long v = std::stoull(s);
        if (v < minv) minv = v;
    }
    return std::to_string(minv - 1);
}

vector<string> getTablesId(){
    cpr::Response r = cpr::Get(cpr::Url{BASE_URL + "/tables"}, cpr::Header{{"Accept","application/json"}});
    if (r.status_code != 200) fail("getTablesId: GET /tables failed: " + r.text);
    json body = json::parse(r.text);
    vector<string> ids;
    if (!body.is_array()) return ids;
    for (const auto &el : body) {
        if (el.is_object() && el.contains("tableId") && el["tableId"].is_string()) {
            ids.push_back(el["tableId"].get<std::string>());
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
    if (!created.is_string()) fail("twoTimesCreateAndDeleteTable: expected string TableID");
    std::string tableId = created.get<std::string>();

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
    if (!created.is_string()) fail("createTableAndCheckData: expected string TableID");
    std::string tableId = created.get<std::string>();
    if (tableId.empty()) fail("createTableAndCheckData: missing tableId");

    cpr::Response list = cpr::Get(cpr::Url{BASE_URL + "/tables"});
    if (list.status_code != 200) fail("createTableAndCheckData: GET /tables failed: " + list.text);
    json listj = json::parse(list.text);
    if (!listj.is_array()) fail("createTableAndCheckData: /tables did not return an array");

    for (const auto &el : listj) {
        if (!el.is_object() || !el.contains("tableId") || !el["tableId"].is_string() || !el.contains("name") || !el["name"].is_string()) {
            fail(std::string("createTableAndCheckData: /tables element has invalid shape: ") + el.dump());
        }
    }

    bool found = false;
    for (const auto &el : listj) {
        if (el.value("name", std::string()) == name) { found = true; break; }
    }
    if (!found) fail("createTableAndCheckData: table name not in /tables");

    cpr::Response byid = cpr::Get(cpr::Url{BASE_URL + "/table/" + tableId});
    if (byid.status_code != 200) fail("createTableAndCheckData: GET /table/{id} failed: " + byid.text);
    json byidj = json::parse(byid.text);

    if (!byidj.is_object()) fail("createTableAndCheckData: GET /table/{id} did not return object: " + byid.text);

    if (!byidj.contains("name") || !byidj["name"].is_string()) fail("createTableAndCheckData: /table missing name or not a string");
    if (!byidj.contains("columns") || !byidj["columns"].is_array()) fail("createTableAndCheckData: /table missing columns array");
    for (const auto &c : byidj["columns"]) {
        if (!c.is_object() || !c.contains("name") || !c["name"].is_string() || !c.contains("type") || !c["type"].is_string()) {
            fail(std::string("createTableAndCheckData: invalid column element: ") + c.dump());
        }
        std::string t = c["type"].get<std::string>();
        if (t != "INT64" && t != "VARCHAR") fail(std::string("createTableAndCheckData: invalid column type: ") + t);
    }

    if (!tableId.empty()) cpr::Response del = cpr::Delete(cpr::Url{BASE_URL + "/table/" + tableId});
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
    std::string body = "{" + std::string("\"" + name + "\": { \"columns\": { \"id\": \"BOOL\" } } }");
    cpr::Response r = cpr::Put(cpr::Url{BASE_URL + "/table"}, cpr::Header{{"Content-Type","application/json"}}, cpr::Body{body});
    if (r.status_code == 200) fail("tryToCreateTableWithInvalidTypes: expected failure but create succeeded");
}

void tryToCreateTableWithDuplicatesColumn(){
    std::string name = "dupcol_" + std::to_string(::time(nullptr));
    std::string body = "{" + std::string("\"" + name + "\": { \"columns\": [ {\"name\": \"id\", \"type\": \"INT64\"}, {\"name\": \"id\", \"type\": \"VARCHAR\"} ] } }");
    cpr::Response r = cpr::Put(cpr::Url{BASE_URL + "/table"}, cpr::Header{{"Content-Type","application/json"}}, cpr::Body{body});
    if (r.status_code == 200) fail("tryToCreateTableWithDuplicatesColumn: expected failure but create succeeded");
}

void tryToCreateTableMissingColumns(){
    std::string body = "{ \"name\": \"missing_columns_test\" }";
    cpr::Response r = cpr::Put(cpr::Url{BASE_URL + "/table"}, cpr::Header{{"Content-Type","application/json"}}, cpr::Body{body});
    if (r.status_code == 200) fail("tryToCreateTableMissingColumns: expected failure but create succeeded");
    try {
        json jb = json::parse(r.text);
        if (!jb.is_object()) fail(std::string("tryToCreateTableMissingColumns: expected object error response: ") + r.text);
        if (!jb.contains("problems") || !jb["problems"].is_array()) fail(std::string("tryToCreateTableMissingColumns: missing problems array: ") + r.text);
        for (const auto &p : jb["problems"]) {
            if (!p.is_object() || !p.contains("error") || !p["error"].is_string()) fail(std::string("tryToCreateTableMissingColumns: invalid problem element: ") + p.dump());
            if (p.contains("context") && !p["context"].is_string()) fail(std::string("tryToCreateTableMissingColumns: problem.context not string: ") + p.dump());
        }
    } catch (const std::exception &ex) {
        fail(std::string("tryToCreateTableMissingColumns: server returned non-json or invalid error response: ") + r.text);
    }
}

void tryToCreateTableColumnEntryMissingType(){
    std::string body = "{ \"name\": \"col_missing_type_test\", \"columns\": [ { \"name\": \"id\" } ] }";
    cpr::Response r = cpr::Put(cpr::Url{BASE_URL + "/table"}, cpr::Header{{"Content-Type","application/json"}}, cpr::Body{body});
    if (r.status_code == 200) fail("tryToCreateTableColumnEntryMissingType: expected failure but create succeeded");
    try {
        json jb = json::parse(r.text);
        if (!jb.is_object()) fail(std::string("tryToCreateTableColumnEntryMissingType: expected object error response: ") + r.text);
        if (!jb.contains("problems") || !jb["problems"].is_array()) fail(std::string("tryToCreateTableColumnEntryMissingType: missing problems array: ") + r.text);
        for (const auto &p : jb["problems"]) {
            if (!p.is_object() || !p.contains("error") || !p["error"].is_string()) fail(std::string("tryToCreateTableColumnEntryMissingType: invalid problem element: ") + p.dump());
            if (p.contains("context") && !p["context"].is_string()) fail(std::string("tryToCreateTableColumnEntryMissingType: problem.context not string: ") + p.dump());
        }

    } catch (const std::exception &ex) {
        fail(std::string("tryToCreateTableColumnEntryMissingType: server returned non-json or invalid error response: ") + r.text);
    }
}

void getSystem(){
    cpr::Response r = cpr::Get(cpr::Url{BASE_URL + "/system/info"});
    if (r.status_code != 200) fail("getSystem: GET /system/info failed: " + r.text);
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

static std::string pollQueryStatus(const std::string &queryId, int maxChecks = 10, int ms = 100) {
    for (int i = 0; i < maxChecks; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(ms));
        cpr::Response qr = cpr::Get(cpr::Url{BASE_URL + "/query/" + queryId}, cpr::Header{{"Accept","application/json"}});
        if (qr.status_code != 200) continue;

        json qbody = json::parse(qr.text);
        std::string status;
        if (qbody.is_object()) {
            status = qbody.value("status", "");
        } else if (qbody.is_array() && !qbody.empty() && qbody[0].is_object()) {
            status = qbody[0].value("status", "");
        } else {
            continue;
        }
        if (status == "COMPLETED" || status == "FAILED") return status;
    }
    return std::string();
}

void queryWithInvalidQueryDefinition() {
    cpr::Response r = cpr::Post(cpr::Url{BASE_URL + "/query"}, cpr::Header{{"Content-Type","application/json"}}, cpr::Body{"{}"});
    if (r.status_code == 200) fail("queryWithInvalidQueryDefinition: expected server to reject missing queryDefinition");
}

void copyQueryWithoutNeccesaryFields() {
    json body = json::object();
    body["queryDefinition"] = json::object({{"destinationTableName", "no-table"}});

    cpr::Response r = cpr::Post(cpr::Url{BASE_URL + "/query"}, cpr::Header{{"Content-Type","application/json"}}, cpr::Body{body.dump()});
    if (r.status_code != 200) return;
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
    copyReq["queryDefinition"] = json::object({{"sourceFilepath", csvPath}, {"destinationTableName", tableName}, {"doesCsvContainHeader", false}});
    cpr::Response copyResp = cpr::Post(cpr::Url{BASE_URL + "/query"}, cpr::Header{{"Content-Type","application/json"}}, cpr::Body{copyReq.dump()});
    if (copyResp.status_code != 200) fail("correctCopyQueryBasic: submit failed: " + copyResp.text);

    json copyCreated = json::parse(copyResp.text);
    std::string qid = copyCreated.value("queryId", std::string());
    if (qid.empty()) fail("correctCopyQueryBasic: missing queryId");
    std::string status = pollQueryStatus(qid);
    if (status != "COMPLETED") fail("correctCopyQueryBasic: expected COMPLETED but got " + status);

    if (!created.is_string()) fail("expected string TableID");
    std::string tableId = created.get<std::string>();
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
    copyReq["queryDefinition"] = json::object({{"sourceFilepath", csvPath}, {"destinationTableName", tableName}, {"doesCsvContainHeader", true}});
    cpr::Response copyResp = cpr::Post(cpr::Url{BASE_URL + "/query"}, cpr::Header{{"Content-Type","application/json"}}, cpr::Body{copyReq.dump()});
    if (copyResp.status_code != 200) fail("correctCopyQueryHeaders: submit failed: " + copyResp.text);

    json copyCreated = json::parse(copyResp.text);
    std::string qid = copyCreated.value("queryId", std::string());
    if (qid.empty()) fail("correctCopyQueryHeaders: missing queryId");
    std::string status = pollQueryStatus(qid);
    if (status != "COMPLETED") fail("correctCopyQueryHeaders: expected COMPLETED but got " + status);
    if (!created.is_string()) fail("expected string TableID");
    std::string tableId = created.get<std::string>();
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
    copyReq["queryDefinition"] = json::object({{"sourceFilepath", csvPath}, {"destinationTableName", tableName}, {"doesCsvContainHeader", true}, {"destinationColumns", json::array({"note","id"})}});
    cpr::Response copyResp = cpr::Post(cpr::Url{BASE_URL + "/query"}, cpr::Header{{"Content-Type","application/json"}}, cpr::Body{copyReq.dump()});
    if (copyResp.status_code != 200) fail("correctCopyQueryDestinationColumns: submit failed: " + copyResp.text);

    json copyCreated = json::parse(copyResp.text);
    std::string qid = copyCreated.value("queryId", std::string());
    if (qid.empty()) fail("correctCopyQueryDestinationColumns: missing queryId");
    std::string status = pollQueryStatus(qid);
    if (status != "COMPLETED") fail("correctCopyQueryDestinationColumns: expected COMPLETED but got " + status);

    if (!created.is_string()) fail("expected string TableID");
    std::string tableId = created.get<std::string>();
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
    copyReq["queryDefinition"] = json::object({{"sourceFilepath", csvPath}, {"destinationTableName", tableName}, {"doesCsvContainHeader", true}});
    cpr::Response copyResp = cpr::Post(cpr::Url{BASE_URL + "/query"}, cpr::Header{{"Content-Type","application/json"}}, cpr::Body{copyReq.dump()});
    if (copyResp.status_code == 200) {
        fail(std::string("invalidDataInCSVCopyQuery: unexpected 200 status"));
    }
    if (!created.is_string()) fail("expected string TableID");
    std::string tableId = created.get<std::string>();
    if (!tableId.empty()) cpr::Response del = cpr::Delete(cpr::Url{BASE_URL + "/table/" + tableId});
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
    copyReq["queryDefinition"] = json::object({{"sourceFilepath", csvPath}, {"destinationTableName", tableName}, {"doesCsvContainHeader", true}});
    cpr::Response copyResp = cpr::Post(cpr::Url{BASE_URL + "/query"}, cpr::Header{{"Content-Type","application/json"}}, cpr::Body{copyReq.dump()});
    if (copyResp.status_code == 200) fail(std::string("lessColumnsInCSVCopyQuery: unexpected 200 status"));

    if (!created.is_string()) fail("expected string TableID");
    std::string tableId = created.get<std::string>();
    if (!tableId.empty()) cpr::Response del = cpr::Delete(cpr::Url{BASE_URL + "/table/" + tableId});
}

void tableNotExistsCopyQuery(){
    std::string tableName = "not_exists_" + std::to_string(::time(nullptr));
    std::string csvPath = "/tmp/" + tableName + ".csv";
    {
        std::ofstream out(csvPath);
        out << "id\n1\n";
    }
    json copyReq = json::object();
    copyReq["queryDefinition"] = json::object({{"sourceFilepath", csvPath}, {"destinationTableName", tableName}, {"doesCsvContainHeader", true}});
    cpr::Response copyResp = cpr::Post(cpr::Url{BASE_URL + "/query"}, cpr::Header{{"Content-Type","application/json"}}, cpr::Body{copyReq.dump()});
    if (copyResp.status_code == 200) fail("tableNotExistsCopyQuery: expected non-200");

    json copyCreated = json::parse(copyResp.text);
    std::string qid = copyCreated.value("queryId", std::string());
    if (qid.empty()) return;
    std::string status = pollQueryStatus(qid);
    if (status != "FAILED") fail("tableNotExistsCopyQuery: expected FAILED but got " + status);
}

void invalidPathCopyQuery(){
    std::string tableName = "ct_path_" + std::to_string(::time(nullptr));
    std::string createBody = "{" + std::string("\"" + tableName + "\": { \"columns\": { \"id\": \"INT64\" } } }");
    cpr::Response r = cpr::Put(cpr::Url{BASE_URL + "/table"}, cpr::Header{{"Content-Type","application/json"}}, cpr::Body{createBody});

    if (r.status_code != 200) fail("invalidPathCopyQuery: create table failed: " + r.text);
    json created = json::parse(r.text);
    std::string csvPath = "/tmp/does-not-exist-" + std::to_string(::time(nullptr)) + ".csv";

    json copyReq = json::object();
    copyReq["queryDefinition"] = json::object({{"sourceFilepath", csvPath}, {"destinationTableName", tableName}, {"doesCsvContainHeader", true}});
    cpr::Response copyResp = cpr::Post(cpr::Url{BASE_URL + "/query"}, cpr::Header{{"Content-Type","application/json"}}, cpr::Body{copyReq.dump()});
    if (copyResp.status_code == 200) fail("tableNotExistsCopyQuery: expected non-200");

    if (!created.is_string()) fail("expected string TableID");
    std::string tableId = created.get<std::string>();
    if (!tableId.empty()) cpr::Response del = cpr::Delete(cpr::Url{BASE_URL + "/table/" + tableId});
}

void getQueryResultWithCorrectQueryId(){
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
    json copyReq = json::object();
    copyReq["queryDefinition"] = json::object({{"sourceFilepath", csvPath}, {"destinationTableName", tableName}, {"doesCsvContainHeader", true}});
    cpr::Response copyResp = cpr::Post(cpr::Url{BASE_URL + "/query"}, cpr::Header{{"Content-Type","application/json"}}, cpr::Body{copyReq.dump()});
    if (copyResp.status_code != 200) fail("getQueryResultWithCorrectQueryId: copy submit failed: " + copyResp.text);
    json copyCreated = json::parse(copyResp.text);
    std::string copyQid = copyCreated.value("queryId", std::string());
    if (copyQid.empty()) fail("getQueryResultWithCorrectQueryId: missing copy queryId");
    std::string copyStatus = pollQueryStatus(copyQid);
    if (copyStatus != "COMPLETED") fail("getQueryResultWithCorrectQueryId: copy did not complete: " + copyStatus);

    json selectReq = json::object();
    selectReq["queryDefinition"] = json::object({{"tableName", tableName}});
    cpr::Response selectResp = cpr::Post(cpr::Url{BASE_URL + "/query"}, cpr::Header{{"Content-Type","application/json"}}, cpr::Body{selectReq.dump()});
    if (selectResp.status_code != 200) fail("getQueryResultWithCorrectQueryId: select submit failed: " + selectResp.text);
    json selectCreated = json::parse(selectResp.text);
    std::string selectQid = selectCreated.value("queryId", std::string());
    if (selectQid.empty()) fail("getQueryResultWithCorrectQueryId: missing select queryId");
    std::string selectStatus = pollQueryStatus(selectQid);
    if (selectStatus != "COMPLETED") fail("getQueryResultWithCorrectQueryId: select did not complete: " + selectStatus);

    cpr::Response res = cpr::Get(cpr::Url{BASE_URL + "/result/" + selectQid}, cpr::Header{{"Accept","application/json"}});
    if (res.status_code != 200) fail("getQueryResultWithCorrectQueryId: GET /result failed: " + res.text);
    json results = json::parse(res.text);
    if (!results.is_array() || results.empty()) fail("getQueryResultWithCorrectQueryId: result missing or empty: " + res.text);

    for (const auto &elem : results) {
        if (!elem.is_object()) fail(std::string("getQueryResultWithCorrectQueryId: result elem not object: ") + elem.dump());
        if (!elem.contains("rowCount") || !elem["rowCount"].is_number_integer()) fail(std::string("getQueryResultWithCorrectQueryId: result missing rowCount or not int: ") + elem.dump());
        if (!elem.contains("columns") || !elem["columns"].is_array()) fail(std::string("getQueryResultWithCorrectQueryId: result missing columns array: ") + elem.dump());
        int rowCount = elem["rowCount"].get<int>();
        for (const auto &col : elem["columns"]) {
            if (!col.is_array()) fail(std::string("getQueryResultWithCorrectQueryId: column is not array: ") + col.dump());
            if (rowCount > 0 && static_cast<int>(col.size()) != rowCount) {
                fail(std::string("getQueryResultWithCorrectQueryId: column length != rowCount: ") + col.dump());
            }
            for (const auto &cell : col) {
                if (!(cell.is_number_integer() || cell.is_string())) {
                    fail(std::string("getQueryResultWithCorrectQueryId: column cell has invalid type: ") + cell.dump());
                }
            }
        }
    }

    if (!created.is_string()) fail("expected string TableID");
    std::string tableId = created.get<std::string>();
    if (!tableId.empty()) cpr::Response del = cpr::Delete(cpr::Url{BASE_URL + "/table/" + tableId});
}

void getQueryResultWithInccorectQueryId(){
    vector<string> qids = getQuieriesId();
    std::string id = getNotExistingId(qids);
    cpr::Response r = cpr::Get(cpr::Url{BASE_URL + "/result/" + id}, cpr::Header{{"Accept","application/json"}});
    if (r.status_code == 200) fail("getQueryResultWithInccorectQueryId: expected non-200 for missing id");
}

void getQueryErrorWithCorrectQueryId(){
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
    copyReq["queryDefinition"] = json::object({{"sourceFilepath", csvPath}, {"destinationTableName", tableName}, {"doesCsvContainHeader", true}});
    cpr::Response copyResp = cpr::Post(cpr::Url{BASE_URL + "/query"}, cpr::Header{{"Content-Type","application/json"}}, cpr::Body{copyReq.dump()});
    std::string qid;

    if (copyResp.status_code == 200) fail("getQueryErrorWithCorrectQueryId: expected non-200 for bad copy body");
    
    cpr::Response qlist = cpr::Get(cpr::Url{BASE_URL + "/queries"}, cpr::Header{{"Accept","application/json"}});
    if (qlist.status_code != 200) {
        std::cout << "getQueryErrorWithCorrectQueryId: cannot list queries, status=" << qlist.status_code << std::endl;
        return;
    }
    json qj = json::parse(qlist.text);
    if (qj.is_array()) {
        for (auto it = qj.rbegin(); it != qj.rend(); ++it) {
            if (it->is_object()) {
                std::string st = it->value("status", std::string());
                if (st == "FAILED" && it->contains("queryId") && (*it)["queryId"].is_string()) {
                    qid = (*it)["queryId"].get<std::string>();
                    break;
                }
            }
        }
    }

    if (qid.empty()) {
        std::cout << "getQueryErrorWithCorrectQueryId: no recent FAILED query found to fetch error for" << std::endl;
        return;
    }

    cpr::Response er = cpr::Get(cpr::Url{BASE_URL + "/error/" + qid}, cpr::Header{{"Accept","application/json"}});
    if (er.status_code != 200) fail("getQueryErrorWithCorrectQueryId: GET /error failed: " + er.text);
    json errj = json::parse(er.text);
    if (!errj.is_object()) fail(std::string("getQueryErrorWithCorrectQueryId: expected object for /error response: ") + errj.dump());
    if (!errj.contains("problems") || !errj["problems"].is_array()) fail(std::string("getQueryErrorWithCorrectQueryId: /error missing problems array: ") + errj.dump());
    for (const auto &p : errj["problems"]) {
        if (!p.is_object() || !p.contains("error") || !p["error"].is_string()) fail(std::string("getQueryErrorWithCorrectQueryId: problem element invalid: ") + p.dump());
        if (p.contains("context") && !p["context"].is_string()) fail(std::string("getQueryErrorWithCorrectQueryId: problem.context not string: ") + p.dump());
    }
    if (!created.is_string()) fail("expected string TableID");
    std::string tableId = created.get<std::string>();
    if (!tableId.empty()) cpr::Response del = cpr::Delete(cpr::Url{BASE_URL + "/table/" + tableId});
}

void getQueryErrorWithInccorectQueryId(){
    vector<string> qids = getQuieriesId();
    std::string id = getNotExistingId(qids);
    cpr::Response r = cpr::Get(cpr::Url{BASE_URL + "/error/" + id}, cpr::Header{{"Accept","application/json"}});
    if (r.status_code == 200) fail("getQueryErrorWithInccorectQueryId: expected non-200 for missing id");
}

void getQueryByIdInvalidJson(){
    cpr::Response r = cpr::Post(cpr::Url{BASE_URL + "/query"}, cpr::Header{{"Content-Type","application/json"}}, cpr::Body{"{\"queryId\": \"abc\"}"});
    if (r.status_code == 200) fail("getQueryByIdInvalidJson: expected server to reject invalid query JSON");
}

void testInvalidJson(){
    cpr::Response r = cpr::Put(cpr::Url{BASE_URL + "/table"}, cpr::Header{{"Content-Type","application/json"}}, cpr::Body{"this is not json"});
    if (r.status_code == 200) fail("testInvalidJson: expected server to reject malformed JSON for create table");
}

void getQueryByInvalidID(){
    cpr::Response r = cpr::Get(cpr::Url{BASE_URL + "/query/invalid-id"}, cpr::Header{{"Accept","application/json"}});
    if (r.status_code == 200) fail("getQueryByInvalidID: expected non-200 for invalid query id");
}

void testRowLimitAndFlushResult(){
    std::string tableName = "rlfr_" + std::to_string(::time(nullptr));
    std::string createBody = "{" + std::string("\"" + tableName + "\": { \"columns\": { \"id\": \"INT64\" } } }");
    cpr::Response r = cpr::Put(cpr::Url{BASE_URL + "/table"}, cpr::Header{{"Content-Type","application/json"}}, cpr::Body{createBody});
    if (r.status_code != 200) fail("testRowLimitAndFlushResult: create table failed: " + r.text);
    json created = json::parse(r.text);
    if (!created.is_string()) fail("testRowLimitAndFlushResult: expected string TableID");
    std::string tableId = created.get<std::string>();

    std::string csvPath = "/tmp/" + tableName + ".csv";
    {
        std::ofstream out(csvPath);
        out << "id\n";
        out << "10\n";
        out << "20\n";
        out << "30\n";
    }

    json copyReq = json::object();
    copyReq["queryDefinition"] = json::object({{"sourceFilepath", csvPath}, {"destinationTableName", tableName}, {"doesCsvContainHeader", true}});
    cpr::Response copyResp = cpr::Post(cpr::Url{BASE_URL + "/query"}, cpr::Header{{"Content-Type","application/json"}}, cpr::Body{copyReq.dump()});
    if (copyResp.status_code != 200) fail("testRowLimitAndFlushResult: copy submit failed: " + copyResp.text);
    json copyCreated = json::parse(copyResp.text);
    std::string copyQid = copyCreated.value("queryId", std::string());
    if (copyQid.empty()) fail("testRowLimitAndFlushResult: missing copy queryId");
    std::string copyStatus = pollQueryStatus(copyQid);
    if (copyStatus != "COMPLETED") fail("testRowLimitAndFlushResult: copy did not complete: " + copyStatus);

    json selectReq = json::object();
    selectReq["queryDefinition"] = json::object({{"tableName", tableName}});
    cpr::Response selectResp = cpr::Post(cpr::Url{BASE_URL + "/query"}, cpr::Header{{"Content-Type","application/json"}}, cpr::Body{selectReq.dump()});
    if (selectResp.status_code != 200) fail("testRowLimitAndFlushResult: select submit failed: " + selectResp.text);
    json selectCreated = json::parse(selectResp.text);
    std::string selectQid = selectCreated.value("queryId", std::string());
    if (selectQid.empty()) fail("testRowLimitAndFlushResult: missing select queryId");
    std::string selectStatus = pollQueryStatus(selectQid);
    if (selectStatus != "COMPLETED") fail("testRowLimitAndFlushResult: select did not complete: " + selectStatus);


    json getReq = json::object();
    getReq["rowLimit"] = 1;
    getReq["flushResult"] = false;
    cpr::Response res = cpr::Get(cpr::Url{BASE_URL + "/result/" + selectQid}, cpr::Header{{"Content-Type","application/json"}}, cpr::Body{getReq.dump()});
    if (res.status_code != 200) fail("testRowLimitAndFlushResult: GET /result failed: " + res.text);
    json results = json::parse(res.text);
    if (!results.is_array() || results.empty()) fail("testRowLimitAndFlushResult: result missing or empty: " + res.text);
    json elem = results[0];
    if (!elem.contains("columns") || !elem["columns"].is_array()) fail("testRowLimitAndFlushResult: result missing columns array: " + elem.dump());

    if (elem["columns"].size() != 1) fail("testRowLimitAndFlushResult: expected 1 column but got " + std::to_string(elem["columns"].size()));
    if (!elem["columns"][0].is_array()) fail("testRowLimitAndFlushResult: column is not array: " + elem["columns"][0].dump());
    if (static_cast<int>(elem["columns"][0].size()) != 1) fail("testRowLimitAndFlushResult: expected 1 row in column but got " + std::to_string(elem["columns"][0].size()));

    if (!tableId.empty()) cpr::Response del = cpr::Delete(cpr::Url{BASE_URL + "/table/" + tableId});
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

    std::cout << "[test-runner] tryToCreateTableMissingColumns()" << std::endl;
    tryToCreateTableMissingColumns();

    std::cout << "[test-runner] tryToCreateTableColumnEntryMissingType()" << std::endl;
    tryToCreateTableColumnEntryMissingType();

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

    std::cout << "[test-runner] lessColumnsInCSVCopyQuery()" << std::endl;
    lessColumnsInCSVCopyQuery();

    std::cout << "[test-runner] tableNotExistsCopyQuery()" << std::endl;
    tableNotExistsCopyQuery();

    std::cout << "[test-runner] invalidPathCopyQuery()" << std::endl;
    invalidPathCopyQuery();

    std::cout << "[test-runner] getQueryResultWithCorrectQueryId()" << std::endl;
    getQueryResultWithCorrectQueryId();

    std::cout << "[test-runner] testRowLimitAndFlushResult()" << std::endl;
    testRowLimitAndFlushResult();

    std::cout << "[test-runner] getQueryResultWithInccorectQueryId()" << std::endl;
    getQueryResultWithInccorectQueryId();

    std::cout << "[test-runner] getQueryErrorWithCorrectQueryId()" << std::endl;
    getQueryErrorWithCorrectQueryId();

    std::cout << "[test-runner] getQueryErrorWithInccorectQueryId()" << std::endl;
    getQueryErrorWithInccorectQueryId();

    std::cout << "[test-runner] getQueryByIdInvalidJson()" << std::endl;
    getQueryByIdInvalidJson();

    std::cout << "[test-runner] testInvalidJson()" << std::endl;
    testInvalidJson();

    std::cout << "[test-runner] getQueryByInvalidID()" << std::endl;
    getQueryByInvalidID();

    std::cout << "[test-runner] ALL TESTS COMPLETED" << std::endl;
}

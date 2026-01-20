#include <iostream>
#include <filesystem>
#include "../metastore/metastore.h"
#include "../types.h"
#include <iostream>
#include <fstream>
#include <unordered_map>
#include <memory>
#include <string>
#include <vector>
#include <optional>
#include <nlohmann/json.hpp>

using json = nlohmann::ordered_json;

using namespace std;

void log_info(const std::string &msg);

void log_error(const std::string &msg);

json getSystemInfo(); 

string prepareTablesInfo(const map<uint64_t, string>& tables); 

string prepareTableInfo(TableInfo& tableInfo);

string statusToString(QueryStatus s); 

json prepareQueryResponse(const QueryResponse &response); 

json prepareQueryResultResponse(const QueryResult& response);

json prepareQueryErrorResponse( const QueryError& error); 

json createErrorResponse(const string message); 

json errorResponse(const vector<Problem>& messages);

string handleCsvError(const std::string &query_id, CSV_TABLE_ERROR code); 

QueryType recogniseQuery(const json &query);

string generateID();

CopyQuery createCopyQuery(const json& json_message);

string statusToStringFromInt(int s);

json copyQueryToJson(const CopyQuery &q);

json selectQueryToJson(const SelectQuery &q);

CopyQuery jsonToCopyQuery(const json &copy_query);

SelectQuery jsonToSelectQuery(const json &select_query);

json buildQueryDefinition(const QueryToJson &q);

json readLocalFile(const std::filesystem::path &basePath);

void saveFile(const std::filesystem::path &basePath, json results);

void removeFiles(const std::string &Path, const std::vector<std::string> &file_names);

vector<string> findDuplicateColumns(const json &cols);

bool validateCreateTableRequest(const json &parsed, json &out_create, std::vector<Problem> &problems);

bool parseResultRequestBody(const std::string &json_body, int &rowLimit, bool &flushResult, json &errorOrParsed);
#include "../types.h"
#include <string>
#include <nlohmann/json.hpp>
#include <vector>
#include <optional>

void initQuery(std::string id);

void modifyQuery(std::string id, vector<Batch>& batches);

std::optional<QueryResponse> getQueryResponse(const std::string &id);

nlohmann::json getQueries();

void changeStatus(std::string id, QueryStatus status);

void addCopyBody(std::string id, CopyQuery copyQuery);

void addSelectBody(std::string id, SelectQuery selectQuery);

#include "../types.h"
#include <string>
#include <nlohmann/json.hpp>
#include <vector>
#include <optional>

using json = nlohmann::ordered_json;

void initQuery(std::string id);

std::optional<QueryResponse> getQueryResponse(const std::string &id);

json getQueries();

void changeStatus(std::string id, QueryStatus status);

void addQueryDefinition(std::string id, QueryToJson query);


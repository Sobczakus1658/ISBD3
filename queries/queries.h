#include "../types.h"
#include <string>
#include <nlohmann/json.hpp>
#include <vector>

void initQuery(std::string id);

void modifyQuery(std::string id, vector<Batch>& batches);

nlohmann::json getQueryResult(const std::string &id);

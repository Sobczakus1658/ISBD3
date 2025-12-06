#include "../types.h"
#include <optional>

std::optional<QueryResult> getQueryResult(const std::string &id, int rowLimit = 0);

void modifyResult(std::string id, vector<Batch>& batches);

void initResult(std::string id);

void removeResult(const std::string &id);
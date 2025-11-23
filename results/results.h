#include "../types.h"
#include <optional>

std::optional<QueryResult> getQueryResult(std::string id);

void modifyResult(std::string id, vector<Batch>& batches);

void initResult(std::string id);